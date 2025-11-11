from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional
import typing as t
import uvicorn
import re
import os
import json
import sqlglot
import logging
import time
from datetime import datetime
from log_collector import setup_logger, log_records
import pyarrow.parquet as pq
import pyarrow.fs as fs
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one
from guardrail.main import StorageServiceClient
from guardrail.main import extract_sql_components_per_table_with_alias, get_table_infos
from guardrail.rules_validator import validate_queries
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

# Prometheus imports
from prometheus_client import Counter, Histogram, Gauge, CONTENT_TYPE_LATEST, CollectorRegistry, multiprocess, generate_latest as prom_generate_latest

if t.TYPE_CHECKING:
    from sqlglot._typing import E

setup_logger()

ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "localhost")  # cops-beta1-storage-storage-blue
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", 9005)

storage_service_client = None

app = FastAPI()

logger = logging.getLogger(__name__)

# ==================== Prometheus Metrics Setup ====================
PROMETHEUS_MULTIPROC_DIR = os.getenv('PROMETHEUS_MULTIPROC_DIR', '/tmp/prometheus_multiproc_dir')
if not os.path.exists(PROMETHEUS_MULTIPROC_DIR):
    os.makedirs(PROMETHEUS_MULTIPROC_DIR, exist_ok=True)
os.environ['PROMETHEUS_MULTIPROC_DIR'] = PROMETHEUS_MULTIPROC_DIR

# Create a custom registry for multiprocess mode
registry = CollectorRegistry()

# ==================== Prometheus Metrics ====================
# Request counters
requests_total = Counter(
    'requests_total',
    'Total number of requests',
    ['from_dialect', 'to_dialect', 'status'],
    registry=registry
)

errors_total = Counter(
    'errors_total',
    'Total number of errors',
    ['from_dialect', 'to_dialect', 'error_type'],
    registry=registry
)

# Duration histogram
request_duration_seconds = Histogram(
    'request_duration_seconds',
    'Duration of requests in seconds',
    ['from_dialect', 'to_dialect'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
    registry=registry
)

# Process duration histograms
process_duration_seconds = Histogram(
    'process_duration_seconds',
    'Duration of individual processing steps',
    ['step_name', 'from_dialect', 'to_dialect'],
    buckets=[0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
    registry=registry
)

# Active requests gauge - use 'pid' mode for multiprocess
active_requests = Gauge(
    'active_requests',
    'Number of requests currently being processed',
    ['from_dialect', 'to_dialect'],
    registry=registry,
    multiprocess_mode='livesum'
)

# Query characteristics
query_size_bytes = Histogram(
    'query_size_bytes',
    'Size of input queries in bytes',
    ['from_dialect', 'to_dialect'],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
    registry=registry
)


if ENABLE_GUARDRAIL.lower() == "true":
    logger.info("Storage Engine URL: ", STORAGE_ENGINE_URL)
    logger.info("Storage Engine Port: ", STORAGE_ENGINE_PORT)

    storage_service_client = StorageServiceClient(host=STORAGE_ENGINE_URL, port=STORAGE_ENGINE_PORT)

logger.info("Storage Service Client is created")


def escape_unicode(s: str) -> str:
    """
    Turn every non-ASCII (including all Unicode spaces) into \\uXXXX,
    so even “invisible” characters become visible in logs.
    """
    return s.encode("unicode_escape").decode("ascii")


@app.post("/convert-query")
async def convert_query(
    query: str = Form(...),
    query_id: Optional[str] = Form("NO_ID_MENTIONED"),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
    feature_flags: Optional[str] = Form(None),
):
    # Start overall timing
    start_time_total = time.perf_counter()
    timestamp = datetime.now().isoformat()
    logger.info("%s — Query received at: %s", query_id, timestamp)
    to_sql = to_sql.lower()
    from_sql_upper = from_sql.upper()
    to_sql_lower = to_sql.lower()

    # Increment active requests gauge
    active_requests.labels(from_dialect=from_sql_upper, to_dialect=to_sql_lower).inc()

    # Record query size
    query_size = len(query.encode('utf-8'))
    query_size_bytes.labels(from_dialect=from_sql_upper, to_dialect=to_sql_lower).observe(query_size)

    # Feature flags parsing
    logger.info("%s — Started: Feature flags parsing", query_id)
    step_start = time.perf_counter()
    flags_dict = {}
    if feature_flags:
        try:
            flags_dict = json.loads(feature_flags)
        except json.JSONDecodeError as je:
            active_requests.labels(from_dialect=from_sql_upper, to_dialect=to_sql_lower).dec()
            errors_total.labels(
                from_dialect=from_sql_upper,
                to_dialect=to_sql_lower,
                error_type="json_decode_error"
            ).inc()
            return HTTPException(status_code=500, detail=str(je))
    process_duration_seconds.labels(
        step_name="feature_flags_parsing",
        from_dialect=from_sql_upper,
        to_dialect=to_sql_lower
    ).observe(time.perf_counter() - step_start)
    logger.info("%s — Completed: Feature flags parsing", query_id)

    if not query or not query.strip():
        logger.info(
            "%s AT %s FROM %s — Empty query received, returning empty result",
            query_id,
            timestamp,
            from_sql.upper(),
        )
        active_requests.labels(from_dialect=from_sql_upper, to_dialect=to_sql_lower).dec()
        requests_total.labels(
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower,
            status="empty_query"
        ).inc()
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

        # Unicode normalization
        logger.info("%s — Started: Unicode normalization", query_id)
        step_start = time.perf_counter()
        query = normalize_unicode_spaces(query)
        process_duration_seconds.labels(
            step_name="unicode_normalization",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Unicode normalization", query_id)

        logger.info(
            "%s AT %s FROM %s — Normalized (escaped):\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            escape_unicode(query),
        )

        # Comment stripping
        logger.info("%s — Started: Comment stripping", query_id)
        step_start = time.perf_counter()
        item = "condenast"
        query, comment = strip_comment(query, item)
        process_duration_seconds.labels(
            step_name="comment_stripping",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Comment stripping", query_id)

        # Initial parsing
        logger.info("%s — Started: SQL parsing", query_id)
        step_start = time.perf_counter()
        tree = sqlglot.parse_one(query, read=from_sql, error_level=None)
        process_duration_seconds.labels(
            step_name="parsing",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Initial SQL parsing", query_id)

        # Two-phase qualification (if enabled)
        if flags_dict.get("USE_TWO_PHASE_QUALIFICATION_SCHEME", False):
            logger.info("%s — Started: Two-phase qualification", query_id)
            step_start = time.perf_counter()
            # Check if we should only transform catalog.schema without full transpilation
            if flags_dict.get("SKIP_E6_TRANSPILATION", False):
                transformed_query = transform_catalog_schema_only(query, from_sql)
                transformed_query = add_comment_to_query(transformed_query, comment)
                process_duration_seconds.labels(
                    step_name="two_phase_catalog_schema",
                    from_dialect=from_sql_upper,
                    to_dialect=to_sql_lower
                ).observe(time.perf_counter() - step_start)
                logger.info("%s — Completed: Two-phase catalog.schema transform", query_id)
                logger.info(
                    "%s AT %s FROM %s — Catalog.Schema Transformed Query:\n%s",
                    query_id,
                    timestamp,
                    from_sql.upper(),
                    transformed_query,
                )
                total_time = time.perf_counter() - start_time_total
                request_duration_seconds.labels(
                    from_dialect=from_sql_upper,
                    to_dialect=to_sql_lower
                ).observe(total_time)
                requests_total.labels(
                    from_dialect=from_sql_upper,
                    to_dialect=to_sql_lower,
                    status="success"
                ).inc()
                active_requests.labels(from_dialect=from_sql_upper, to_dialect=to_sql_lower).dec()
                logger.info("%s — TOTAL /convert-query execution took %.4f ms", query_id, total_time * 1000)
                return {"converted_query": transformed_query}
            tree = transform_table_part(tree)
            process_duration_seconds.labels(
                step_name="two_phase_table_transform",
                from_dialect=from_sql_upper,
                to_dialect=to_sql_lower
            ).observe(time.perf_counter() - step_start)
            logger.info("%s — Completed: Two-phase table transform", query_id)

        # Quote identifiers
        logger.info("%s — Started: Quote identifiers", query_id)
        step_start = time.perf_counter()
        tree2 = quote_identifiers(tree, dialect=to_sql)
        process_duration_seconds.labels(
            step_name="quote_identifiers",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Quote identifiers", query_id)

        # Ensure select from values
        logger.info("%s — Started: Ensure select from values", query_id)
        step_start = time.perf_counter()
        values_ensured_ast = ensure_select_from_values(tree2)
        process_duration_seconds.labels(
            step_name="ensure_select_from_values",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Ensure select from values", query_id)

        # CTE names case sensitivity
        logger.info("%s — Started: Set CTE names case sensitively", query_id)
        step_start = time.perf_counter()
        cte_names_equivalence_checked_ast = set_cte_names_case_sensitively(values_ensured_ast)
        process_duration_seconds.labels(
            step_name="set_cte_names_case_sensitively",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Set CTE names case sensitively", query_id)

        # SQL generation
        logger.info("%s — Started: SQL generation", query_id)
        step_start = time.perf_counter()
        double_quotes_added_query = cte_names_equivalence_checked_ast.sql(
            dialect=to_sql, from_dialect=from_sql, pretty=flags_dict.get("PRETTY_PRINT", True)
        )
        process_duration_seconds.labels(
            step_name="sql_generation",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: SQL generation", query_id)

        # Struct replacement
        logger.info("%s — Started: Struct replacement", query_id)
        step_start = time.perf_counter()
        double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)
        process_duration_seconds.labels(
            step_name="struct_replacement",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Struct replacement", query_id)

        # Add comment
        logger.info("%s — Started: Add comment", query_id)
        step_start = time.perf_counter()
        double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)
        process_duration_seconds.labels(
            step_name="add_comment",
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(time.perf_counter() - step_start)
        logger.info("%s — Completed: Add comment", query_id)

        # Total timing
        total_time = time.perf_counter() - start_time_total
        end_timestamp = datetime.now().isoformat()

        # Record total duration
        request_duration_seconds.labels(
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower
        ).observe(total_time)

        # Increment success counter
        requests_total.labels(
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower,
            status="success"
        ).inc()

        logger.info("%s — TOTAL /convert-query execution took %.4f ms", query_id, total_time * 1000)
        logger.info("%s — Query left at: %s", query_id, end_timestamp)

        logger.info(
            "%s AT %s FROM %s — Transpiled Query:\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            double_quotes_added_query,
        )

        # Add processing time to response headers
        from fastapi.responses import JSONResponse
        response = JSONResponse(content={"converted_query": double_quotes_added_query})
        response.headers["X-Processing-Time-Ms"] = f"{total_time * 1000:.4f}"
        response.headers["X-Query-Id"] = query_id
        return response
    except Exception as e:
        total_time = time.perf_counter() - start_time_total

        # Determine error type
        error_type = type(e).__name__

        # Record error metrics
        errors_total.labels(
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower,
            error_type=error_type
        ).inc()

        requests_total.labels(
            from_dialect=from_sql_upper,
            to_dialect=to_sql_lower,
            status="error"
        ).inc()

        logger.error(
            "%s AT %s FROM %s — Error (after %.4f ms):\n%s",
            query_id,
            timestamp,
            from_sql.upper(),
            total_time * 1000,
            str(e),
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Decrement active requests gauge
        active_requests.labels(from_dialect=from_sql_upper, to_dialect=to_sql_lower).dec()

        # Always restore the original flag value
        E6.ENABLE_TABLE_ALIAS_QUALIFICATION = original_qualification_flag


@app.get("/health")
def health_check():
    return Response(status_code=200)


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint for multiprocess mode"""
    # Use multiprocess collector to aggregate metrics from all worker processes
    from prometheus_client import generate_latest
    registry_for_metrics = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry_for_metrics)
    return Response(content=generate_latest(registry_for_metrics), media_type=CONTENT_TYPE_LATEST)

@app.post("/guardrail")
async def gaurd(
    query: str = Form(...),
    schema: str = Form(...),
    catalog: str = Form(...),
):
    try:
        if storage_service_client is not None:
            parsed = sqlglot.parse(query, error_level=None)

            queries, tables = extract_sql_components_per_table_with_alias(parsed)

            # tables = client.get_table_names(catalog_name="hive", db_name="tpcds_1000")
            table_map = get_table_infos(tables, storage_service_client, catalog, schema)
            logger.info("table info is ", table_map)

            violations_found = validate_queries(queries, table_map)

            if violations_found:
                return {"action": "deny", "violations": violations_found}
            else:
                return {"action": "allow", "violations": []}
        else:
            detail = (
                "Storage Service Not Initialized. Guardrail service status: " + ENABLE_GUARDRAIL
            )
            logger.error(detail)
            raise HTTPException(status_code=500, detail=detail)

    except Exception as e:
        logger.error(f"Error in guardrail API: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/transpile-guardrail")
async def Transgaurd(
    query: str = Form(...),
    schema: str = Form(...),
    catalog: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
):
    to_sql = to_sql.lower()
    try:
        if storage_service_client is not None:
            # This is the main method will which help in transpiling to our e6data SQL dialects from other dialects
            converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[
                0
            ]

            # This is additional steps to replace the STRUCT(STRUCT()) --> {{}}
            converted_query = replace_struct_in_query(converted_query)

            converted_query_ast = parse_one(converted_query, read=to_sql)

            double_quotes_added_query = quote_identifiers(converted_query_ast, dialect=to_sql).sql(
                dialect=to_sql
            )

            # ------------------------#
            # GuardRail Application
            parsed = sqlglot.parse(double_quotes_added_query, error_level=None)

            # now lets validate the query
            queries, tables = extract_sql_components_per_table_with_alias(parsed)

            table_map = get_table_infos(tables, storage_service_client, catalog, schema)

            violations_found = validate_queries(queries, table_map)

            if violations_found:
                return {"action": "deny", "violations": violations_found}
            else:
                return {"action": "allow", "violations": []}
        else:
            detail = (
                "Storage Service Not Initialized. Guardrail service status: " + ENABLE_GUARDRAIL
            )
            raise HTTPException(status_code=500, detail=detail)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/statistics")
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
    # Start overall timing
    start_time_total = time.perf_counter()
    timestamp = datetime.now().isoformat()
    to_sql = to_sql.lower()

    logger.info(f"{query_id} AT start time: {timestamp} FROM {from_sql.upper()}")

    # Timing: Feature flags parsing
    t0 = time.perf_counter()
    flags_dict = {}
    if feature_flags:
        try:
            flags_dict = json.loads(feature_flags)
        except json.JSONDecodeError as je:
            return HTTPException(status_code=500, detail=str(je))
    t_flags = time.perf_counter() - t0
    logger.info("%s — Timing: Feature flags parsing took %.4f ms", query_id, t_flags * 1000)

    try:
        # Timing: Load supported functions
        t0 = time.perf_counter()
        supported_functions_in_e6 = load_supported_functions(to_sql)
        t_load = time.perf_counter() - t0
        logger.info("%s — Timing: Load supported functions (e6) took %.4f ms", query_id, t_load * 1000)

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
            total_time = time.perf_counter() - start_time_total
            logger.info("Query is empty or only contains comments! (after %.4f ms)", total_time * 1000)
            return {
                "supported_functions": [],
                "unsupported_functions": [],
                "udf_list": [],
                "converted-query": "Query is empty or only contains comments.",
                "unsupported_functions_after_transpilation": [],
                "executable": "NO",
                "error": True,
                "log_records": log_records,
            }

        # Timing: Comment stripping
        t0 = time.perf_counter()
        item = "condenast"
        query, comment = strip_comment(query, item)
        t_strip = time.perf_counter() - t0
        logger.info("%s — Timing: Comment stripping took %.4f ms", query_id, t_strip * 1000)

        # Timing: Extract functions from original query
        t0 = time.perf_counter()
        all_functions = extract_functions_from_query(
            query, function_pattern, keyword_pattern, exclusion_list
        )
        supported, unsupported = categorize_functions(
            all_functions, supported_functions_in_e6, functions_as_keywords
        )
        t_extract_funcs = time.perf_counter() - t0
        logger.info("%s — Timing: Extract/categorize functions (original) took %.4f ms", query_id, t_extract_funcs * 1000)

        # Timing: Load from-dialect functions and extract UDFs
        t0 = time.perf_counter()
        from_dialect_function_list = load_supported_functions(from_sql)
        udf_list, unsupported = extract_udfs(unsupported, from_dialect_function_list)
        t_udfs = time.perf_counter() - t0
        logger.info("%s — Timing: Load from-dialect functions & extract UDFs took %.4f ms", query_id, t_udfs * 1000)

        # --------------------------
        # HANDLING PARSING ERRORS
        # --------------------------
        executable = "YES"
        error_flag = False
        try:
            # ------------------------------
            # Step 1: Parse the Original Query
            # ------------------------------
            # Timing: Parse original query
            t0 = time.perf_counter()
            original_ast = parse_one(query, read=from_sql)
            t_parse_orig = time.perf_counter() - t0
            logger.info("%s — Timing: Parse original query took %.4f ms", query_id, t_parse_orig * 1000)

            # Timing: Extract tables/databases
            t0 = time.perf_counter()
            tables_list = extract_db_and_Table_names(original_ast)
            t_tables = time.perf_counter() - t0
            logger.info("%s — Timing: Extract tables/databases took %.4f ms", query_id, t_tables * 1000)

            # Timing: Identify unsupported functionality
            t0 = time.perf_counter()
            supported, unsupported = unsupported_functionality_identifiers(
                original_ast, unsupported, supported
            )
            t_unsup_ident = time.perf_counter() - t0
            logger.info("%s — Timing: Identify unsupported functionality took %.4f ms", query_id, t_unsup_ident * 1000)

            # Timing: Ensure values and CTE names
            t0 = time.perf_counter()
            values_ensured_ast = ensure_select_from_values(original_ast)
            cte_names_equivalence_ast = set_cte_names_case_sensitively(values_ensured_ast)
            query = cte_names_equivalence_ast.sql(from_sql)
            t_ensure = time.perf_counter() - t0
            logger.info("%s — Timing: Ensure values & CTE names took %.4f ms", query_id, t_ensure * 1000)

            # ------------------------------
            # Step 2: Transpile the Query
            # ------------------------------
            # Timing: Parse and transpile
            t0 = time.perf_counter()
            tree = sqlglot.parse_one(query, read=from_sql, error_level=None)
            tree2 = quote_identifiers(tree, dialect=to_sql)

            double_quotes_added_query = tree2.sql(
                dialect=to_sql, from_dialect=from_sql, pretty=flags_dict.get("PRETTY_PRINT", True)
            )
            t_transpile = time.perf_counter() - t0
            logger.info("%s — Timing: Parse & transpile query took %.4f ms", query_id, t_transpile * 1000)

            # Timing: Post-processing (struct replacement & comment)
            t0 = time.perf_counter()
            double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)
            double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)
            t_postproc = time.perf_counter() - t0
            logger.info("%s — Timing: Post-processing (struct/comment) took %.4f ms", query_id, t_postproc * 1000)

            logger.info("Got the converted query!!!!")

            # Timing: Extract functions from transpiled query
            t0 = time.perf_counter()
            all_functions_converted_query = extract_functions_from_query(
                double_quotes_added_query, function_pattern, keyword_pattern, exclusion_list
            )
            supported_functions_in_converted_query, unsupported_functions_in_converted_query = (
                categorize_functions(
                    all_functions_converted_query, supported_functions_in_e6, functions_as_keywords
                )
            )
            t_extract_conv = time.perf_counter() - t0
            logger.info("%s — Timing: Extract/categorize functions (converted) took %.4f ms", query_id, t_extract_conv * 1000)

            # Timing: Identify unsupported in converted query
            t0 = time.perf_counter()
            double_quote_ast = parse_one(double_quotes_added_query, read=to_sql)
            supported_in_converted, unsupported_in_converted = (
                unsupported_functionality_identifiers(
                    double_quote_ast,
                    unsupported_functions_in_converted_query,
                    supported_functions_in_converted_query,
                )
            )
            t_unsup_conv = time.perf_counter() - t0
            logger.info("%s — Timing: Identify unsupported (converted) took %.4f ms", query_id, t_unsup_conv * 1000)

            # Timing: Extract joins and CTEs
            t0 = time.perf_counter()
            joins_list = extract_joins_from_query(original_ast)
            cte_values_subquery_list = extract_cte_n_subquery_list(original_ast)
            t_joins_ctes = time.perf_counter() - t0
            logger.info("%s — Timing: Extract joins & CTEs took %.4f ms", query_id, t_joins_ctes * 1000)

            if unsupported_in_converted:
                executable = "NO"

            # Total timing for successful execution
            total_time = time.perf_counter() - start_time_total
            logger.info("%s — Timing: TOTAL /statistics execution took %.4f ms", query_id, total_time * 1000)

            logger.info(
                f"{query_id} executed in {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds()} seconds FROM {from_sql.upper()}\n"
                "-----------------------\n"
                "--- Original query ---\n"
                "-----------------------\n"
                f"{query}"
                "-----------------------\n"
                "--- Transpiled query ---\n"
                "-----------------------\n"
                f"{double_quotes_added_query}"
            )

        except Exception as e:
            # Total timing for error case
            total_time = time.perf_counter() - start_time_total
            logger.info(
                f"{query_id} executed in {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds()} seconds FROM {from_sql.upper()} (after %.4f ms error)\n"
                "-----------------------\n"
                "--- Original query ---\n"
                "-----------------------\n"
                f"{query}"
                "-----------------------\n"
                "-------- Error --------\n"
                "-----------------------\n"
                f"{str(e)}" % (total_time * 1000,)
            )
            error_message = f"{str(e)}"
            error_flag = True
            double_quotes_added_query = error_message
            tables_list = []
            joins_list = []
            cte_values_subquery_list = []
            unsupported_in_converted = []
            executable = "NO"

        # Final return - add total timing
        total_time_final = time.perf_counter() - start_time_total
        logger.info("%s — Timing: FINAL /statistics return after %.4f ms", query_id, total_time_final * 1000)

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
            "log_records": log_records,
        }

    except Exception as e:
        # Total timing for outer exception
        total_time = time.perf_counter() - start_time_total
        logger.error(
            f"{query_id} occurred at time {datetime.now().isoformat()} with processing time {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds()} FROM {from_sql.upper()} (after %.4f ms)\n"
            "-----------------------\n"
            "--- Original query ---\n"
            "-----------------------\n"
            f"{query}"
            "-----------------------\n"
            "-------- Error --------\n"
            "-----------------------\n"
            f"{str(e)}" % (total_time * 1000,)
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
            "log_records": log_records,
        }


@app.post("/guardstats")
async def guardstats(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
    schema: str = Form(...),
    catalog: str = Form(...),
):
    to_sql = to_sql.lower()
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
        ]

        # Regex patterns
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = (
            r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
        )

        item = "condenast"
        query, comment = strip_comment(query, item)

        # Extract functions from the query
        all_functions = extract_functions_from_query(
            query, function_pattern, keyword_pattern, exclusion_list
        )
        supported, unsupported = categorize_functions(
            all_functions, supported_functions_in_e6, functions_as_keywords
        )
        logger.info(f"supported: {supported}\n\nunsupported: {unsupported}")

        original_ast = parse_one(query, read=from_sql)
        tables_list = extract_db_and_Table_names(original_ast)
        supported, unsupported = unsupported_functionality_identifiers(
            original_ast, unsupported, supported
        )
        values_ensured_ast = ensure_select_from_values(original_ast)
        query = values_ensured_ast.sql(dialect=from_sql)

        tree = sqlglot.parse_one(query, read=from_sql, error_level=None)

        tree2 = quote_identifiers(tree, dialect=to_sql)

        double_quotes_added_query = tree2.sql(dialect=to_sql, from_dialect=from_sql)

        double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

        double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)

        all_functions_converted_query = extract_functions_from_query(
            double_quotes_added_query, function_pattern, keyword_pattern, exclusion_list
        )
        supported_functions_in_converted_query, unsupported_functions_in_converted_query = (
            categorize_functions(
                all_functions_converted_query, supported_functions_in_e6, functions_as_keywords
            )
        )

        double_quote_ast = parse_one(double_quotes_added_query, read=to_sql)
        supported_in_converted, unsupported_in_converted = unsupported_functionality_identifiers(
            double_quote_ast,
            unsupported_functions_in_converted_query,
            supported_functions_in_converted_query,
        )

        from_dialect_func_list = load_supported_functions(from_sql)

        udf_list, unsupported = extract_udfs(unsupported, from_dialect_func_list)

        executable = "NO" if unsupported_in_converted else "YES"

        if storage_service_client is not None:
            parsed = sqlglot.parse(double_quotes_added_query, error_level=None)

            queries, tables = extract_sql_components_per_table_with_alias(parsed)

            # tables = client.get_table_names(catalog_name="hive", db_name="tpcds_1000")
            table_map = get_table_infos(tables, storage_service_client, catalog, schema)
            logger.info("table info is ", table_map)

            violations_found = validate_queries(queries, table_map)

            joins_list = extract_joins_from_query(original_ast)

            cte_values_subquery_list = extract_cte_n_subquery_list(original_ast)

            if violations_found:
                return {
                    "supported_functions": supported,
                    "unsupported_functions": unsupported,
                    "udf_list": udf_list,
                    "converted-query": double_quotes_added_query,
                    "unsupported_functions_after_transpilation": unsupported_in_converted,
                    "executable": executable,
                    "tables_list": tables_list,
                    "joins_list": joins_list,
                    "cte_values_subquery_list": cte_values_subquery_list,
                    "action": "deny",
                    "violations": violations_found,
                    "log_records": log_records,
                }
            else:
                return {
                    "supported_functions": supported,
                    "unsupported_functions": unsupported,
                    "converted-query": double_quotes_added_query,
                    "unsupported_functions_after_transpilation": unsupported_in_converted,
                    "udf_list": udf_list,
                    "executable": executable,
                    "tables_list": tables_list,
                    "joins_list": joins_list,
                    "cte_values_subquery_list": cte_values_subquery_list,
                    "action": "allow",
                    "violations": [],
                    "log_records": log_records,
                }
        else:
            detail = (
                "Storage Service Not Initialized. Guardrail service status: " + ENABLE_GUARDRAIL
            )
            raise HTTPException(status_code=500, detail=detail)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import multiprocessing

    # Calculate optimal workers based on CPU cores
    cpu_cores = multiprocessing.cpu_count()
    # Formula: (2 × CPU_cores) + 1, with min 2 and max 20
    optimal_workers = min(max((2 * cpu_cores) + 1, 2), 20)

    # Allow override via environment variable
    workers = int(os.getenv("UVICORN_WORKERS", optimal_workers))

    logger.info(f"Detected {cpu_cores} CPU cores, using {workers} workers")

    uvicorn.run("converter_api:app", host="0.0.0.0", port=8100, proxy_headers=True, workers=workers)
