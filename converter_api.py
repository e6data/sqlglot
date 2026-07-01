from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional
import typing as t
import uvicorn
import re
import os
import json
import sqlglot
import logging
from datetime import datetime
from log_collector import setup_logger, log_records
import pyarrow.parquet as pq
import pyarrow.fs as fs
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one
from sqlglot.dialects.snowflake_backticks import SnowflakeBackticks
from apis.utils.multidialect import pg_outer_to_inner, split_pg_outer, _splice
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
    fix_quote_escapes,
    restore_quote_escapes,
    extract_large_in_clauses,
    restore_large_in_clauses,
)
from formatting_utils import preserve_formatting

if t.TYPE_CHECKING:
    from sqlglot._typing import E

setup_logger()

ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "localhost")  # cops-beta1-storage-storage-blue
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", 9005)
SKIP_COMMENT = os.getenv("SKIP_COMMENT", "True")  # Always strip multi-line comments
FIX_QUOTE_ESCAPES = os.getenv("FIX_QUOTE_ESCAPES", "False")  # Fix '' inside single-quoted strings
E6_EXECUTOR_TYPE = os.getenv(
    "E6_EXECUTOR_TYPE", "java"
)  # "java" divides TO_UNIX_TIMESTAMP by 1000; "native" does not

storage_service_client = None

app = FastAPI()

logger = logging.getLogger(__name__)


if ENABLE_GUARDRAIL.lower() == "true":
    logger.info("Storage Engine URL: ", STORAGE_ENGINE_URL)
    logger.info("Storage Engine Port: ", STORAGE_ENGINE_PORT)

    storage_service_client = StorageServiceClient(host=STORAGE_ENGINE_URL, port=STORAGE_ENGINE_PORT)

logger.info("Storage Service Client is created")
logger.info(
    "Environment flags — ENABLE_GUARDRAIL: %s, SKIP_COMMENT: %s, FIX_QUOTE_ESCAPES: %s, "
    "E6_EXECUTOR_TYPE: %s, STORAGE_ENGINE_URL: %s, STORAGE_ENGINE_PORT: %s",
    ENABLE_GUARDRAIL,
    SKIP_COMMENT,
    FIX_QUOTE_ESCAPES,
    E6_EXECUTOR_TYPE,
    STORAGE_ENGINE_URL,
    STORAGE_ENGINE_PORT,
)


def escape_unicode(s: str) -> str:
    """
    Turn every non-ASCII (including all Unicode spaces) into \\uXXXX,
    so even “invisible” characters become visible in logs.
    """
    return s.encode("unicode_escape").decode("ascii")


def _region_to_e6(region_sql: str, from_sql: str, pretty: bool) -> str:
    """Transpile ONE region of a multi-dialect BI-tool query to e6.

    This deliberately runs the SAME steps as the main /convert-query pipeline
    (normalize -> strip comments -> parse -> quote identifiers -> ensure SELECT FROM
    VALUES -> case-sensitive CTE names -> .sql -> replace STRUCT), so a region
    produces output identical to any other query going through the converter.

    The one thing that varies is ``from_sql`` -- the dialect is "rewired" per region:
      - "databricks" for the primary path (the merged pg->dbr intermediary) and for each
        inner subquery, and
      - "postgres" for the fallback OUTER, so e6 applies its Postgres-specific rules
        (e.g. dropping a 1-arg numeric ``TRUNC`` that Databricks would mis-read as a
        date truncation).
    """
    # Same input cleanup the main path does before parsing.
    region_sql = normalize_unicode_spaces(region_sql)
    if SKIP_COMMENT.lower() == "true":
        region_sql, _ = strip_comment(region_sql)
    # Large IN-clause optimization: pull out oversized literal lists before
    # parsing so sqlglot doesn't build/traverse thousands of AST nodes.
    region_sql, in_replacements = extract_large_in_clauses(region_sql)
    # Parse with the region's own source dialect, then run the standard e6 steps.
    tree = sqlglot.parse_one(region_sql, read=from_sql, error_level=None)
    tree = quote_identifiers(tree, dialect="e6")
    tree = ensure_select_from_values(tree)
    tree = set_cte_names_case_sensitively(tree)
    # from_dialect=from_sql is what lets e6 honor the source dialect's semantics.
    out = tree.sql(dialect="e6", from_dialect=from_sql, pretty=pretty)
    out = replace_struct_in_query(out)
    # Restore original IN-clause values after transpilation.
    return restore_large_in_clauses(out, in_replacements)


@app.post("/convert-query")
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

    if flags_dict.get("MULTIDIALECT", False):
        # Multi-dialect BI-tool queries (Power BI / Tableau / ThoughtSpot): a Postgres
        # outer wrapper ("..." = identifier) wrapping inner subqueries written in another
        # dialect. The inner dialect is read from the feature flags via INNER_DIALECT
        # (default "databricks"; e.g. "snowflake"). This branch does its own e6 generation
        # (via _region_to_e6) and returns directly -- it does NOT fall through to the
        # shared pipeline below.
        inner_dialect = flags_dict.get("INNER_DIALECT", "databricks").lower()
        pretty = flags_dict.get("PRETTY_PRINT", True)
        logger.info(
            "%s AT %s — MULTIDIALECT flag set: Postgres outer + %s inner "
            "(INNER_DIALECT=%s, from_sql=%s ignored)",
            query_id,
            timestamp,
            inner_dialect,
            inner_dialect,
            from_sql,
        )
        try:
            # PRIMARY: outer pg -> <inner_dialect> (inner subqueries kept verbatim), then
            # one <inner_dialect> -> e6 pass over the merged query. For inner_dialect
            # "snowflake" this is pg -> snowflake -> e6; for "databricks", pg -> dbr -> e6.
            intermediary = pg_outer_to_inner(query, inner_dialect)
            logger.info(
                "%s AT %s — MULTIDIALECT primary intermediary (pg -> %s):\n%s",
                query_id,
                timestamp,
                inner_dialect,
                intermediary,
            )
            converted_query = _region_to_e6(intermediary, inner_dialect, pretty)
            logger.info(
                "%s AT %s — MULTIDIALECT PRIMARY pass taken (pg -> %s -> e6)",
                query_id,
                timestamp,
                inner_dialect,
            )
        except Exception as e:
            # FALLBACK: the <inner_dialect> -> e6 step failed -- e.g. a Postgres construct
            # the inner dialect can't re-read (numeric TRUNC, which Databricks/e6 treat as
            # a date truncation needing a unit). Split the query and run each region
            # through the SAME e6 pipeline with the dialect rewired: the OUTER as
            # "postgres" (so e6 applies Postgres rules, e.g. dropping the 1-arg TRUNC) and
            # each inner subquery as <inner_dialect>, then splice the e6 fragments.
            logger.warning(
                "%s AT %s — MULTIDIALECT primary pg -> %s -> e6 failed (%s); "
                "FALLBACK pass taken (outer pg -> e6, inner %s -> e6)",
                query_id,
                timestamp,
                inner_dialect,
                e,
                inner_dialect,
            )
            outer, inner_subqueries = split_pg_outer(query)
            logger.info(
                "%s AT %s — MULTIDIALECT fallback intermediary outer "
                "(pg, %d inner subqueries held out):\n%s",
                query_id,
                timestamp,
                len(inner_subqueries),
                outer,
            )
            converted_query = _region_to_e6(outer, "postgres", pretty)
            for marker, subquery in inner_subqueries.items():
                converted_query = _splice(
                    converted_query,
                    marker,
                    _region_to_e6(subquery, inner_dialect, pretty),
                )
        logger.info(
            "%s AT %s — MULTIDIALECT Transpiled Query:\n%s",
            query_id,
            timestamp,
            converted_query,
        )
        return {"converted_query": converted_query}
    elif flags_dict.get("POWERBI_SF_TO_DBR", False):
        # Intermediary vanilla Snowflake -> Databricks transpile, run
        # unconditionally whenever the flag is set -- the caller's `from_sql`
        # is intentionally ignored so the planner can opt a query into the
        # SF -> DBR step without first having to assert its dialect.
        #   - on success: `query` is now Databricks-shaped (SF identifiers
        #     turned into backticks, function renames, etc.).
        #   - on failure: keep the original query unchanged; assume it was
        #     already Databricks-shaped.
        # In either case the query going into the downstream pipeline is
        # Databricks-shaped, so override `from_sql` to "databricks" so the
        # rest of the handler parses it with the right dialect.
        logger.info(
            "%s AT %s — POWERBI_SF_TO_DBR: intermediary Snowflake -> Databricks transpile (from_sql=%s ignored)",
            query_id,
            timestamp,
            from_sql,
        )
        try:
            # Parse as Snowflake but tolerate Databricks backtick identifiers:
            # Power BI queries mix ANSI double-quoted identifiers (the outer
            # wrapper) with backtick identifiers (inner CTEs). Plain Snowflake
            # chokes on the backticks; SnowflakeBackticks treats both " and `
            # as identifiers, so every identifier is correctly carried over to
            # Databricks backticks while single-quoted strings stay literals.
            query = sqlglot.transpile(
                query,
                read=SnowflakeBackticks,
                write="databricks",
                identify=False,
            )[0]
            logger.info(
                "%s AT %s — Intermediary (SF -> DBR) result:\n%s",
                query_id,
                timestamp,
                query,
            )
        except Exception as e:
            logger.warning(
                "%s AT %s — Intermediary SF -> DBR failed (%s); forwarding original query as Databricks",
                query_id,
                timestamp,
                e,
            )
        from_sql = "databricks"

    if not query or not query.strip():
        logger.info(
            "%s AT %s FROM %s — Empty query received, returning empty result",
            query_id,
            timestamp,
            from_sql.upper(),
        )
        return {"converted_query": ""}

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

        # Always strip comment from query, but only re-add if SKIP_COMMENT is false

        if SKIP_COMMENT.lower() == "true":
            query, comment = strip_comment(query)
            logger.info("%s — SKIP_COMMENT: stripped comments", query_id)

        # Large IN-clause optimization: extract oversized literal-only value
        # lists before parsing so sqlglot doesn't build/traverse thousands of
        # AST nodes for values that need no dialect transformation.
        query, in_replacements = extract_large_in_clauses(query)
        if in_replacements:
            logger.info(
                "%s — Large IN-clause optimization: extracted %d clause(s)",
                query_id,
                len(in_replacements),
            )

        tree = sqlglot.parse_one(query, read=from_sql, error_level=None)

        if flags_dict.get("USE_TWO_PHASE_QUALIFICATION_SCHEME", False):
            logger.info("%s — USE_TWO_PHASE_QUALIFICATION_SCHEME: enabled", query_id)
            # Check if we should only transform catalog.schema without full transpilation
            if flags_dict.get("SKIP_E6_TRANSPILATION", False):
                logger.info("%s — SKIP_E6_TRANSPILATION: enabled", query_id)
                transformed_query = transform_catalog_schema_only(query, from_sql)
                # transformed_query = add_comment_to_query(transformed_query, comment)
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
            dialect=to_sql,
            from_dialect=from_sql,
            pretty=flags_dict.get("PRETTY_PRINT", True),
        )

        double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

        # Restore original IN-clause values that were extracted before parsing.
        double_quotes_added_query = restore_large_in_clauses(
            double_quotes_added_query, in_replacements
        )

        # Preserve original formatting if enabled via feature flag
        if flags_dict.get("PRESERVE_FORMATTING", False):
            logger.info("%s — PRESERVE_FORMATTING: enabled", query_id)
            double_quotes_added_query = preserve_formatting(
                query, double_quotes_added_query, from_sql, to_sql
            )

        # double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)

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


@app.get("/health")
def health_check():
    return Response(status_code=200)


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
                "log_records": log_records,
            }

        query, comment = strip_comment(query)

        # Extract functions from the query
        all_functions = extract_functions_from_query(
            query, function_pattern, keyword_pattern, exclusion_list
        )
        supported, unsupported = categorize_functions(
            all_functions, supported_functions_in_e6, functions_as_keywords
        )

        from_dialect_function_list = load_supported_functions(from_sql)
        udf_list, unsupported = extract_udfs(unsupported, from_dialect_function_list)

        # --------------------------
        # HANDLING PARSING ERRORS
        # --------------------------
        executable = "YES"
        error_flag = False
        try:
            # ------------------------------
            # Step 1: Parse the Original Query
            # ------------------------------
            original_ast = parse_one(query, read=from_sql)
            tables_list = extract_db_and_Table_names(original_ast)
            supported, unsupported = unsupported_functionality_identifiers(
                original_ast, unsupported, supported
            )
            values_ensured_ast = ensure_select_from_values(original_ast)
            cte_names_equivalence_ast = set_cte_names_case_sensitively(values_ensured_ast)
            query = cte_names_equivalence_ast.sql(from_sql)

            # ------------------------------
            # Step 2: Transpile the Query
            # ------------------------------
            tree = sqlglot.parse_one(query, read=from_sql, error_level=None)
            tree2 = quote_identifiers(tree, dialect=to_sql)

            double_quotes_added_query = tree2.sql(
                dialect=to_sql,
                from_dialect=from_sql,
                pretty=flags_dict.get("PRETTY_PRINT", True),
            )

            double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

            # Preserve original formatting if enabled via feature flag
            if flags_dict.get("PRESERVE_FORMATTING", False):
                double_quotes_added_query = preserve_formatting(
                    query, double_quotes_added_query, from_sql, to_sql
                )

            double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)

            logger.info("Got the converted query!!!!")

            all_functions_converted_query = extract_functions_from_query(
                double_quotes_added_query,
                function_pattern,
                keyword_pattern,
                exclusion_list,
            )
            (
                supported_functions_in_converted_query,
                unsupported_functions_in_converted_query,
            ) = categorize_functions(
                all_functions_converted_query,
                supported_functions_in_e6,
                functions_as_keywords,
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
            logger.info(
                f"{query_id} executed in {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds()} seconds FROM {from_sql.upper()}\n"
                "-----------------------\n"
                "--- Original query ---\n"
                "-----------------------\n"
                f"{query}"
                "-----------------------\n"
                "-------- Error --------\n"
                "-----------------------\n"
                f"{str(e)}"
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
            "log_records": log_records,
        }

    except Exception as e:
        logger.error(
            f"{query_id} occurred at time {datetime.now().isoformat()} with processing time {(datetime.now() - datetime.fromisoformat(timestamp)).total_seconds()} FROM {from_sql.upper()}\n"
            "-----------------------\n"
            "--- Original query ---\n"
            "-----------------------\n"
            f"{query}"
            "-----------------------\n"
            "-------- Error --------\n"
            "-----------------------\n"
            f"{str(e)}"
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

        query, comment = strip_comment(query)

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

        # Note: PRESERVE_FORMATTING not available here as no flags_dict
        # Can be added if needed by adding feature_flags parameter to this endpoint

        double_quotes_added_query = add_comment_to_query(double_quotes_added_query, comment)

        all_functions_converted_query = extract_functions_from_query(
            double_quotes_added_query, function_pattern, keyword_pattern, exclusion_list
        )
        (
            supported_functions_in_converted_query,
            unsupported_functions_in_converted_query,
        ) = categorize_functions(
            all_functions_converted_query,
            supported_functions_in_e6,
            functions_as_keywords,
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

    uvicorn.run(
        "converter_api:app",
        host="0.0.0.0",
        port=8100,
        proxy_headers=True,
        workers=workers,
    )
