from fastapi import FastAPI, Form, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from typing import Optional
import typing as t
import uvicorn
import re
import os
import json
import sqlglot
import logging
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow.fs as fs
from pythonjsonlogger import jsonlogger
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one
from apis.utils.helpers import (
    strip_comment,
    unsupported_functionality_identifiers,
    extract_functions_from_query,
    categorize_functions,
    add_comment_to_query,
    replace_struct_in_query,
    ensure_select_from_values,
    extract_udfs,
    load_supported_functions,
    extract_db_and_Table_names,
    extract_joins_from_query,
    extract_cte_n_subquery_list,
    normalize_unicode_spaces,
    transform_table_part,
    transform_catalog_schema_only,
    set_cte_names_case_sensitively,
)

if t.TYPE_CHECKING:
    from sqlglot._typing import E

# Setup structured logging
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
handler.setFormatter(formatter)
logging.getLogger().handlers = []
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

app = FastAPI(
    title="E6 SQL Transpiler API",
    description="SQL transpiler API for E6 dialect with support for multiple source dialects",
    version="1.0.0",
    default_response_class=ORJSONResponse,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus middleware (if metrics enabled)
from apis.config import get_transpiler_config
config = get_transpiler_config()
if config.enable_metrics:
    from apis.middleware.prometheus import PrometheusMiddleware
    app.add_middleware(PrometheusMiddleware)

logger = logging.getLogger(__name__)

# Import and include v1 routers
from apis.routers.v1 import inline, batch, meta

app.include_router(inline.router, prefix="/api/v1/inline", tags=["Inline Mode"])
app.include_router(batch.router, prefix="/api/v1/batch", tags=["Batch Mode"])
app.include_router(meta.router, prefix="/api/v1", tags=["Meta"])


def escape_unicode(s: str) -> str:
    """
    Turn every non-ASCII (including all Unicode spaces) into \\uXXXX,
    so even “invisible” characters become visible in logs.
    """
    return s.encode("unicode_escape").decode("ascii")


@app.post("/convert-query", deprecated=True)
async def convert_query(
    query: str = Form(...),
    query_id: Optional[str] = Form("NO_ID_MENTIONED"),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
    feature_flags: Optional[str] = Form(None),
):
    timestamp = datetime.now().isoformat()
    to_sql = to_sql.lower()

    flags_dict = {}
    if feature_flags:
        try:
            flags_dict = json.loads(feature_flags)
        except json.JSONDecodeError as je:
            return HTTPException(status_code=500, detail=str(je))

    if not query or not query.strip():
        logger.info(
            "%s AT %s FROM %s — Empty query received, returning empty result",
            query_id,
            timestamp,
            from_sql.upper(),
        )
        return {"converted_query": ""}

    # Set table alias qualification flag from feature_flags (similar to PRETTY_PRINT)
    from sqlglot.dialects.e6 import E6

    original_qualification_flag = E6.ENABLE_TABLE_ALIAS_QUALIFICATION
    E6.ENABLE_TABLE_ALIAS_QUALIFICATION = flags_dict.get("ENABLE_TABLE_ALIAS_QUALIFICATION", False)

    try:
        logger.info(
            "%s AT %s FROM %s — Original:\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            escape_unicode(query),
        )

        query = normalize_unicode_spaces(query)
        logger.info(
            "%s AT %s FROM %s — Normalized (escaped):\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            escape_unicode(query),
        )

        item = "condenast"
        query, comment = strip_comment(query, item)

        tree = sqlglot.parse_one(query, read=from_sql, error_level=None)

        if flags_dict.get("USE_TWO_PHASE_QUALIFICATION_SCHEME", False):
            # Check if we should only transform catalog.schema without full transpilation
            if flags_dict.get("SKIP_E6_TRANSPILATION", False):
                transformed_query = transform_catalog_schema_only(query, from_sql)
                transformed_query = add_comment_to_query(transformed_query, comment)
                logger.info(
                    "%s AT %s FROM %s — Catalog.Schema Transformed Query:\n%s",
                    query_id,
                    timestamp,
                    from_sql.upper(),
                    transformed_query,
                )
                return {"converted_query": transformed_query}
            tree = transform_table_part(tree)

        tree2 = quote_identifiers(tree, dialect=to_sql)

        values_ensured_ast = ensure_select_from_values(tree2)

        cte_names_equivalence_checked_ast = set_cte_names_case_sensitively(values_ensured_ast)

        double_quotes_added_query = cte_names_equivalence_checked_ast.sql(
            dialect=to_sql, from_dialect=from_sql, pretty=flags_dict.get("PRETTY_PRINT", True)
        )

        double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

        double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)

        logger.info(
            "%s AT %s FROM %s — Transpiled Query:\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            double_quotes_added_query,
        )
        return {"converted_query": double_quotes_added_query}
    except Exception as e:
        logger.error(
            "%s AT %s FROM %s — Error:\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            str(e),
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Always restore the original flag value
        E6.ENABLE_TABLE_ALIAS_QUALIFICATION = original_qualification_flag


@app.get("/health", deprecated=True)
def health_check():
    return Response(status_code=200)


@app.get("/metrics")
async def metrics():
    """
    Prometheus metrics endpoint.

    Returns metrics in Prometheus text format for scraping.
    """
    from apis.config import get_transpiler_config
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, REGISTRY
    from prometheus_client.multiprocess import MultiProcessCollector
    from fastapi import HTTPException

    config = get_transpiler_config()

    if not config.enable_metrics:
        raise HTTPException(status_code=404, detail="Metrics are disabled")

    # Check if using multiprocess mode
    if config.uvicorn_workers > 1 and os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        # Use multiprocess collector to aggregate metrics from all workers
        from prometheus_client import CollectorRegistry
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        metrics_output = generate_latest(registry)
    else:
        # Single process mode
        metrics_output = generate_latest(REGISTRY)

    return Response(content=metrics_output, media_type=CONTENT_TYPE_LATEST)






@app.post("/statistics", deprecated=True)
async def stats_api(
    query: str = Form(...),
    query_id: Optional[str] = Form("NO_ID_MENTIONED"),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
    feature_flags: Optional[str] = Form(None),
):
    """
    API endpoint to extract supported and unsupported SQL functions from a query.
    """
    timestamp = datetime.now().isoformat()
    to_sql = to_sql.lower()

    logger.info(f"{query_id} AT start time: {timestamp} FROM {from_sql.upper()}")
    flags_dict = {}

    if feature_flags:
        try:
            flags_dict = json.loads(feature_flags)
        except json.JSONDecodeError as je:
            return HTTPException(status_code=500, detail=str(je))

    try:
        supported_functions_in_e6 = load_supported_functions(to_sql)

        # Functions treated as keywords (no parentheses required)
        functions_as_keywords = [
            "LIKE",
            "ILIKE",
            "RLIKE",
            "AT TIME ZONE",
            "||",
            "DISTINCT",
            "QUALIFY",
        ]

        # Exclusion list for words that are followed by '(' but are not functions
        exclusion_list = [
            "AS",
            "AND",
            "THEN",
            "OR",
            "ELSE",
            "WHEN",
            "WHERE",
            "FROM",
            "JOIN",
            "OVER",
            "ON",
            "ALL",
            "NOT",
            "BETWEEN",
            "UNION",
            "SELECT",
            "BY",
            "GROUP",
            "EXCEPT",
            "SETS",
        ]

        # Regex patterns
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = (
            r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
        )

        if not query.strip():
            logger.info("Query is empty or only contains comments!")
            return {
                "supported_functions": [],
                "unsupported_functions": [],
                "udf_list": [],
                "converted-query": "Query is empty or only contains comments.",
                "unsupported_functions_after_transpilation": [],
                "executable": "NO",
                "error": True,
            }

        item = "condenast"
        query, comment = strip_comment(query, item)

        # Extract functions from the query
        all_functions = extract_functions_from_query(
            query, function_pattern, keyword_pattern, exclusion_list
        )
        supported, unsupported = categorize_functions(
            all_functions, supported_functions_in_e6, functions_as_keywords
        )

        from_dialect_function_list = load_supported_functions(from_sql)
        udf_list, unsupported = extract_udfs(unsupported, from_dialect_function_list)

        # Parse and transpile query
        executable = "YES"
        error_flag = False
        try:
            # Parse the original query
            original_ast = parse_one(query, read=from_sql)
            tables_list = extract_db_and_Table_names(original_ast)
            supported, unsupported = unsupported_functionality_identifiers(
                original_ast, unsupported, supported
            )
            values_ensured_ast = ensure_select_from_values(original_ast)
            cte_names_equivalence_ast = set_cte_names_case_sensitively(values_ensured_ast)
            query = cte_names_equivalence_ast.sql(from_sql)

            # Transpile the query
            tree = sqlglot.parse_one(query, read=from_sql, error_level=None)
            tree2 = quote_identifiers(tree, dialect=to_sql)

            double_quotes_added_query = tree2.sql(
                dialect=to_sql, from_dialect=from_sql, pretty=flags_dict.get("PRETTY_PRINT", True)
            )

            double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

            double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)

            logger.info("Got the converted query!!!!")

            all_functions_converted_query = extract_functions_from_query(
                double_quotes_added_query, function_pattern, keyword_pattern, exclusion_list
            )
            supported_functions_in_converted_query, unsupported_functions_in_converted_query = (
                categorize_functions(
                    all_functions_converted_query, supported_functions_in_e6, functions_as_keywords
                )
            )

            double_quote_ast = parse_one(double_quotes_added_query, read=to_sql)
            supported_in_converted, unsupported_in_converted = (
                unsupported_functionality_identifiers(
                    double_quote_ast,
                    unsupported_functions_in_converted_query,
                    supported_functions_in_converted_query,
                )
            )

            joins_list = extract_joins_from_query(original_ast)
            cte_values_subquery_list = extract_cte_n_subquery_list(original_ast)

            if unsupported_in_converted:
                executable = "NO"

            logger.info(
                f"{query_id} — Completed in {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds():.3f}s FROM {from_sql.upper()}"
            )

        except Exception as e:
            logger.error(
                f"{query_id} — Failed in {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds():.3f}s FROM {from_sql.upper()} — Error: {str(e)}",
                exc_info=True
            )
            error_message = f"{str(e)}"
            error_flag = True
            double_quotes_added_query = error_message
            tables_list = []
            joins_list = []
            cte_values_subquery_list = []
            unsupported_in_converted = []
            executable = "NO"

        return {
            "supported_functions": supported,
            "unsupported_functions": set(unsupported),
            "udf_list": set(udf_list),
            "converted-query": double_quotes_added_query,  # Will contain error message if error_flag is True
            "unsupported_functions_after_transpilation": set(unsupported_in_converted),
            "executable": executable,
            "tables_list": set(tables_list),
            "joins_list": joins_list,
            "cte_values_subquery_list": cte_values_subquery_list,
            "error": error_flag,
        }

    except Exception as e:
        logger.error(
            f"{query_id} — Fatal error at {datetime.now().isoformat()} after {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds():.3f}s FROM {from_sql.upper()} — Error: {str(e)}",
            exc_info=True
        )
        return {
            "supported_functions": [],
            "unsupported_functions": [],
            "udf_list": [],
            "converted-query": f"Internal Server Error: {str(e)}",
            "unsupported_functions_after_transpilation": [],
            "executable": "NO",
            "tables_list": [],
            "joins_list": [],
            "cte_values_subquery_list": [],
            "error": True,
        }




if __name__ == "__main__":
    from apis.config import get_transpiler_config
    from sqlglot.dialects.e6 import configure_e6_dialect_from_system_config
    import tempfile
    import shutil

    config = get_transpiler_config()

    logger.info(
        "transpiler_starting",
        extra={"config": config.model_dump()},
    )

    # Configure E6 dialect from system config
    logger.info("e6_dialect_configuring")
    configure_e6_dialect_from_system_config()
    logger.info("e6_dialect_configured")

    # Configure Prometheus multiprocess mode if using multiple workers
    if config.enable_metrics and config.uvicorn_workers > 1:
        # Create temporary directory for multiprocess metrics
        multiproc_dir = tempfile.mkdtemp(prefix="prometheus_multiproc_")
        os.environ["PROMETHEUS_MULTIPROC_DIR"] = multiproc_dir
        logger.info("prometheus_multiprocess_configured", extra={"dir": multiproc_dir})

    # Initialize metrics
    if config.enable_metrics:
        from apis.metrics import initialize_metrics
        initialize_metrics()

    uvicorn.run(
        "converter_api:app",
        host=config.uvicorn_host,
        port=config.uvicorn_port,
        proxy_headers=True,
        workers=config.uvicorn_workers,
        access_log=False,
        log_level=config.log_level.lower()
    )
