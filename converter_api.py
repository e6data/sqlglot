from fastapi import FastAPI, Form, HTTPException, Response, BackgroundTasks
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict, Any
import typing as t
import uvicorn
import re
import os
import json
import sqlglot
import logging
from datetime import datetime
from log_collector import setup_logger, log_records
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.compute as pc
import pandas as pd
from io import BytesIO
import time
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

if t.TYPE_CHECKING:
    from sqlglot._typing import E

setup_logger()

ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "localhost")  # cops-beta1-storage-storage-blue
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", 9005)

storage_service_client = None

app = FastAPI()

logger = logging.getLogger(__name__)

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
            "supported_functions": set(supported),
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


# Import the parquet reader functions
import sys
sys.path.append('/Users/tanaykulkarni/Documents/bucket-reader')
from efficient_large_parquet_reader import read_large_parquet_efficiently, stream_large_parquet_efficiently


@app.post("/batch-statistics-s3")
async def batch_statistics_s3(
    s3_path: str = Form(...),
    query_column: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
    feature_flags: Optional[str] = Form(None),
    memory_threshold_mb: Optional[int] = Form(500),
    batch_size: Optional[int] = Form(50000),
):
    """
    Process SQL queries from S3 parquet file with full statistics like the /statistics endpoint.
    Automatically chooses streaming vs direct read based on file size.
    
    Args:
        s3_path: S3 path like 's3://bucket/path/to/file.parquet'
        query_column: Column name containing SQL queries
        from_sql: Source SQL dialect
        to_sql: Target SQL dialect
        feature_flags: JSON string of feature flags
        memory_threshold_mb: File size threshold (default 500MB) - files larger than this will be streamed
        batch_size: Rows per batch when streaming (default 50000)
    """
    timestamp = datetime.now().isoformat()
    to_sql = to_sql.lower()
    
    logger.info(f"Batch statistics from S3 AT {timestamp} FROM {from_sql.upper()}")
    
    # Parse feature flags
    flags_dict = {}
    if feature_flags and feature_flags.strip():
        try:
            flags_dict = json.loads(feature_flags)
        except json.JSONDecodeError as je:
            raise HTTPException(status_code=400, detail=f"Invalid feature_flags JSON: {str(je)}")
    
    # Load supported functions
    supported_functions_in_e6 = load_supported_functions(to_sql)
    from_dialect_function_list = load_supported_functions(from_sql)
    
    functions_as_keywords = [
        "LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT", "QUALIFY"
    ]
    
    exclusion_list = [
        "AS", "AND", "THEN", "OR", "ELSE", "WHEN", "WHERE", "FROM", 
        "JOIN", "OVER", "ON", "ALL", "NOT", "BETWEEN", "UNION", 
        "SELECT", "BY", "GROUP", "EXCEPT", "SETS"
    ]
    
    function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
    keyword_pattern = r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
    
    # Results aggregation
    batch_results = []
    total_processed = 0
    total_successful = 0
    total_failed = 0
    
    # Aggregate statistics
    all_supported_functions = set()
    all_unsupported_functions = set()
    all_udf_list = set()
    all_tables = set()
    all_joins = []
    all_unsupported_after_transpilation = set()
    
    # Initialize PyArrow table for efficient storage and eventual Iceberg/Delta integration
    results_schema = pa.schema([
        ('query_id', pa.int64()),
        ('status', pa.string()),
        ('executable', pa.string()),
        ('original_query', pa.string()),
        ('converted_query', pa.string()),
        ('supported_functions', pa.list_(pa.string())),
        ('unsupported_functions', pa.list_(pa.string())),
        ('udf_list', pa.list_(pa.string())),
        ('tables_list', pa.list_(pa.string())),
        ('error_message', pa.string())
    ])
    
    # Initialize empty PyArrow table with proper empty arrays for each column
    empty_arrays = [
        pa.array([], type=pa.int64()),           # query_id
        pa.array([], type=pa.string()),          # status
        pa.array([], type=pa.string()),          # executable
        pa.array([], type=pa.string()),          # original_query
        pa.array([], type=pa.string()),          # converted_query
        pa.array([], type=pa.list_(pa.string())),  # supported_functions
        pa.array([], type=pa.list_(pa.string())),  # unsupported_functions
        pa.array([], type=pa.list_(pa.string())),  # udf_list
        pa.array([], type=pa.list_(pa.string())),  # tables_list
        pa.array([], type=pa.string())           # error_message
    ]
    results_table = pa.table(empty_arrays, schema=results_schema)
    
    try:
        # Parse S3 path
        bucket = s3_path.split('/')[2]
        key = '/'.join(s3_path.split('/')[3:])
        
        # Create S3 filesystem to check file size
        s3fs = fs.S3FileSystem(
            access_key=os.environ.get('AWS_ACCESS_KEY_ID'),
            secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            session_token=os.environ.get('AWS_SESSION_TOKEN'),
            region='us-east-1',
            connect_timeout=30,
            request_timeout=60
        )
        
        # Get file size
        file_info = s3fs.get_file_info(f"{bucket}/{key}")
        file_size_mb = file_info.size / (1024**2)
        
        logger.info(f"File size: {file_size_mb:.1f} MB, threshold: {memory_threshold_mb} MB")
        
        # Choose processing method based on file size
        use_streaming = file_size_mb > memory_threshold_mb
        
        def process_query(query_str, query_id):
            """Process a single query and return statistics"""
            if not query_str or not query_str.strip():
                return None
            
            try:
                query_str = normalize_unicode_spaces(query_str)
                item = "condenast"
                query_str, comment = strip_comment(query_str, item)
                
                # Extract functions
                all_functions = extract_functions_from_query(
                    query_str, function_pattern, keyword_pattern, exclusion_list
                )
                supported, unsupported = categorize_functions(
                    all_functions, supported_functions_in_e6, functions_as_keywords
                )
                
                udf_list, unsupported = extract_udfs(unsupported, from_dialect_function_list)
                
                # Parse and analyze
                original_ast = parse_one(query_str, read=from_sql)
                tables_list = extract_db_and_Table_names(original_ast)
                supported, unsupported = unsupported_functionality_identifiers(
                    original_ast, unsupported, supported
                )
                values_ensured_ast = ensure_select_from_values(original_ast)
                cte_names_equivalence_ast = set_cte_names_case_sensitively(values_ensured_ast)
                query_str = cte_names_equivalence_ast.sql(from_sql)
                
                # Transpile
                tree = sqlglot.parse_one(query_str, read=from_sql, error_level=None)
                
                if flags_dict.get("USE_TWO_PHASE_QUALIFICATION_SCHEME", False):
                    if flags_dict.get("SKIP_E6_TRANSPILATION", False):
                        converted = transform_catalog_schema_only(query_str, from_sql)
                        converted = add_comment_to_query(converted, comment)
                    else:
                        tree = transform_table_part(tree)
                        tree2 = quote_identifiers(tree, dialect=to_sql)
                        values_ensured = ensure_select_from_values(tree2)
                        cte_checked = set_cte_names_case_sensitively(values_ensured)
                        converted = cte_checked.sql(
                            dialect=to_sql, from_dialect=from_sql, 
                            pretty=flags_dict.get("PRETTY_PRINT", True)
                        )
                        converted = replace_struct_in_query(converted)
                        converted = add_comment_to_query(converted, comment)
                else:
                    tree2 = quote_identifiers(tree, dialect=to_sql)
                    values_ensured = ensure_select_from_values(tree2)
                    cte_checked = set_cte_names_case_sensitively(values_ensured)
                    converted = cte_checked.sql(
                        dialect=to_sql, from_dialect=from_sql,
                        pretty=flags_dict.get("PRETTY_PRINT", True)
                    )
                    converted = replace_struct_in_query(converted)
                    converted = add_comment_to_query(converted, comment)
                
                # Analyze converted query
                all_functions_converted = extract_functions_from_query(
                    converted, function_pattern, keyword_pattern, exclusion_list
                )
                supported_in_converted, unsupported_in_converted = categorize_functions(
                    all_functions_converted, supported_functions_in_e6, functions_as_keywords
                )
                
                converted_ast = parse_one(converted, read=to_sql)
                supported_in_converted, unsupported_in_converted = unsupported_functionality_identifiers(
                    converted_ast, unsupported_in_converted, supported_in_converted
                )
                
                joins_list = extract_joins_from_query(original_ast)
                cte_values_subquery_list = extract_cte_n_subquery_list(original_ast)
                
                executable = "NO" if unsupported_in_converted else "YES"
                
                return {
                    "supported_functions": set(supported),
                    "unsupported_functions": set(unsupported),
                    "udf_list": set(udf_list),
                    "converted_query": converted,
                    "unsupported_after_transpilation": set(unsupported_in_converted),
                    "executable": executable,
                    "tables_list": set(tables_list),
                    "joins_list": joins_list,
                    "cte_values_subquery_list": cte_values_subquery_list,
                    "error": False
                }
                
            except Exception as e:
                logger.debug(f"Error processing query {query_id}: {str(e)}")
                return {
                    "error": True,
                    "error_message": str(e),
                    "supported_functions": set(),
                    "unsupported_functions": set(),
                    "udf_list": set(),
                    "unsupported_after_transpilation": set(),
                    "tables_list": set(),
                    "joins_list": [],
                    "cte_values_subquery_list": []
                }
        
        # Process queries based on file size
        if use_streaming:
            logger.info(f"Using STREAMING for large file ({file_size_mb:.1f} MB)")
            
            batch_num = 0
            for chunk_df in stream_large_parquet_efficiently(
                s3_path,
                batch_size=batch_size,
                columns=[query_column]
            ):
                batch_num += 1
                batch_start = time.time()
                batch_success = 0
                batch_fail = 0
                
                for idx, query in enumerate(chunk_df[query_column].dropna()):
                    query_id = f"batch_{batch_num}_row_{idx}"
                    result = process_query(str(query), query_id)
                    
                    if result:
                        if not result.get("error"):
                            batch_success += 1
                            all_supported_functions.update(result["supported_functions"])
                            all_unsupported_functions.update(result["unsupported_functions"])
                            all_udf_list.update(result["udf_list"])
                            all_tables.update(result["tables_list"])
                            all_joins.extend(result["joins_list"])
                            all_unsupported_after_transpilation.update(result["unsupported_after_transpilation"])
                        else:
                            batch_fail += 1
                    
                    # Log progress every 100 queries
                    if (batch_success + batch_fail) % 100 == 0:
                        logger.info(f"Progress: {batch_success + batch_fail} queries processed in batch {batch_num} ({batch_success} successful, {batch_fail} failed)")
                
                batch_time = time.time() - batch_start
                batch_results.append({
                    "batch_number": batch_num,
                    "rows_processed": len(chunk_df),
                    "successful": batch_success,
                    "failed": batch_fail,
                    "processing_time": batch_time
                })
                
                total_processed += len(chunk_df)
                total_successful += batch_success
                total_failed += batch_fail
                
                logger.info(f"Batch {batch_num}: {batch_success} successful, {batch_fail} failed in {batch_time:.2f}s")
        
        else:
            logger.info(f"Using DIRECT READ for small file ({file_size_mb:.1f} MB)")
            
            df = read_large_parquet_efficiently(
                s3_path,
                columns=[query_column]
            )
            
            batch_start = time.time()
            for idx, query in enumerate(df[query_column].dropna()):
                query_id = f"row_{idx}"
                result = process_query(str(query), query_id)
                
                if result:
                    # Add individual query result to results array (NO TRUNCATION)
                    individual_result = {
                        "query_id": idx + 1,
                        "original_query": str(query),  # FULL QUERY - NO TRUNCATION
                        "status": "success" if not result.get("error") else "failed",
                        "converted_query": result.get("converted_query", ""),
                        "executable": result.get("executable", "NO"),
                        "supported_functions": list(result.get("supported_functions", [])),
                        "unsupported_functions": list(result.get("unsupported_functions", [])),
                        "udf_list": list(result.get("udf_list", [])),
                        "tables_list": list(result.get("tables_list", [])),
                        "joins_list": result.get("joins_list", []),
                        "error_message": result.get("error_message", "") if result.get("error") else ""
                    }
                    
                    # Add to PyArrow table for efficient storage
                    new_row_data = [
                        [individual_result["query_id"]],
                        [individual_result["status"]], 
                        [individual_result["executable"]],
                        [individual_result["original_query"]],
                        [individual_result["converted_query"]],
                        [individual_result["supported_functions"]],
                        [individual_result["unsupported_functions"]],
                        [individual_result["udf_list"]],
                        [individual_result["tables_list"]],
                        [individual_result["error_message"]]
                    ]
                    
                    new_row_table = pa.table(new_row_data, schema=results_schema)
                    results_table = pa.concat_tables([results_table, new_row_table])
                    
                    # Display all 100 individual queries every 100 queries processed
                    if (idx + 1) % 100 == 0:
                        print(f"\n{'='*80}")
                        print(f"ALL QUERIES - {idx + 1} queries processed:")
                        print(f"{'='*80}")
                        
                        # Show all queries in the table with all columns
                        for i in range(results_table.num_rows):
                            query_id = results_table['query_id'][i].as_py()
                            status = results_table['status'][i].as_py()
                            executable = results_table['executable'][i].as_py()
                            original = results_table['original_query'][i].as_py()
                            converted = results_table['converted_query'][i].as_py()
                            supported_funcs = results_table['supported_functions'][i].as_py()
                            unsupported_funcs = results_table['unsupported_functions'][i].as_py()
                            udf_list = results_table['udf_list'][i].as_py()
                            tables_list = results_table['tables_list'][i].as_py()
                            error_msg = results_table['error_message'][i].as_py()
                            
                            print(f"\n--- QUERY #{query_id} ---")
                            print(f"Status: {status} | Executable: {executable}")
                            
                            print(f"\nORIGINAL:")
                            print(f"{original}")
                            
                            print(f"\nCONVERTED:")
                            print(f"{converted}")
                            
                            # Show function analysis
                            if supported_funcs:
                                print(f"\nSUPPORTED FUNCTIONS: {supported_funcs}")
                            if unsupported_funcs:
                                print(f"UNSUPPORTED FUNCTIONS: {unsupported_funcs}")
                            if udf_list:
                                print(f"UDF LIST: {udf_list}")
                            
                            # Show table analysis
                            if tables_list:
                                print(f"TABLES: {tables_list}")
                            
                            # Show errors
                            if error_msg:
                                print(f"\nERROR: {error_msg}")
                            
                            print(f"{'-'*50}")
                        
                        print(f"\n{'='*80}\n", flush=True)
                    
                    if not result.get("error"):
                        total_successful += 1
                        all_supported_functions.update(result["supported_functions"])
                        all_unsupported_functions.update(result["unsupported_functions"])
                        all_udf_list.update(result["udf_list"])
                        all_tables.update(result["tables_list"])
                        all_joins.extend(result["joins_list"])
                        all_unsupported_after_transpilation.update(result["unsupported_after_transpilation"])
                    else:
                        total_failed += 1
                
            
            total_processed = len(df)
            batch_time = time.time() - batch_start
            
            batch_results.append({
                "batch_number": 1,
                "rows_processed": total_processed,
                "successful": total_successful,
                "failed": total_failed,
                "processing_time": batch_time
            })
        
        processing_time = (datetime.now() - datetime.fromisoformat(timestamp)).total_seconds()
        
        logger.info(
            f"Batch processing complete in {processing_time:.2f}s: "
            f"{total_processed} rows, {total_successful} successful, {total_failed} failed"
        )
        
        return {
            "s3_path": s3_path,
            "file_size_mb": file_size_mb,
            "processing_method": "streaming" if use_streaming else "direct",
            "total_rows_processed": total_processed,
            "total_successful": total_successful,
            "total_failed": total_failed,
            "success_rate": f"{(total_successful / total_processed * 100):.2f}%" if total_processed > 0 else "0%",
            "processing_time_seconds": processing_time,
            "batch_results": batch_results,
            "aggregate_statistics": {
                "supported_functions": list(all_supported_functions),
                "unsupported_functions": list(all_unsupported_functions),
                "udf_list": list(all_udf_list),
                "unsupported_after_transpilation": list(all_unsupported_after_transpilation),
                "tables_list": list(all_tables),
                "unique_joins_count": len(set(map(str, all_joins))),
                "executable_count": total_successful - len([x for x in all_unsupported_after_transpilation if x])
            },
            "log_records": log_records
        }
        
    except Exception as e:
        logger.error(f"Error in batch statistics: {str(e)}", exc_info=True)
        return {
            "error": True,
            "error_message": str(e),
            "log_records": log_records
        }


@app.post("/validate-s3-bucket")
async def validate_s3_bucket(
    s3_path: str = Form(...),
):
    """
    Validate S3 bucket connection and identify query columns in parquet files.
    Handles both individual files and directories.
    
    Args:
        s3_path: S3 path like 's3://bucket/path/to/directory/' or 's3://bucket/path/to/file.parquet'
    
    Returns:
        - authenticated: Whether connection succeeded
        - format: File format (parquet, delta, iceberg)
        - files_found: Number of parquet files found
        - columns: Common columns across all files
        - query_column: Identified query column (if found)
        - response: "Yes" if query column found, "No" otherwise
    """
    
    try:
        # Parse S3 path
        if not s3_path.startswith('s3://'):
            return {
                "authenticated": False,
                "error": "Invalid S3 path format. Must start with 's3://'"
            }
        
        bucket = s3_path.split('/')[2]
        key_prefix = '/'.join(s3_path.split('/')[3:])
        
        # Ensure key_prefix ends with / for directory scanning
        if key_prefix and not key_prefix.endswith('/'):
            key_prefix = key_prefix + '/'
        
        # Create S3 filesystem
        try:
            s3fs = fs.S3FileSystem(
                access_key=os.environ.get('AWS_ACCESS_KEY_ID'),
                secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                session_token=os.environ.get('AWS_SESSION_TOKEN'),
                region='us-east-1',
                connect_timeout=30,
                request_timeout=60
            )
        except Exception as e:
            return {
                "authenticated": False,
                "error": f"S3 authentication failed: {str(e)}",
                "response": "No"
            }
        
        # List all parquet files in the directory
        try:
            # Use get_file_info to check if it's a file or directory
            path = f"{bucket}/{key_prefix}" if key_prefix else bucket
            file_info = s3fs.get_file_info(path)
            
            parquet_files = []
            
            if file_info.type == fs.FileType.File:
                # Single file provided
                if path.endswith('.parquet'):
                    parquet_files = [path]
            else:
                # Directory provided - list all parquet files
                from pyarrow.fs import FileSelector
                selector = FileSelector(path, recursive=True)
                file_infos = s3fs.get_file_info(selector)
                
                for finfo in file_infos:
                    if finfo.type == fs.FileType.File and finfo.path.endswith('.parquet'):
                        parquet_files.append(finfo.path)
            
            if not parquet_files:
                return {
                    "authenticated": True,
                    "error": f"No parquet files found in: {s3_path}",
                    "files_found": 0,
                    "response": "No"
                }
            
            logger.info(f"Found {len(parquet_files)} parquet files")
            
            # Simple analysis - just get columns from first file and look for query columns
            all_columns = []
            query_column = None
            total_size_mb = 0
            
            # Common query column names to look for (prioritized order)
            query_patterns = ['hashed_query', 'query', 'sql', 'statement_text', 'sql_query', 'command']
            
            # Just read the first parquet file to get schema
            try:
                first_file = parquet_files[0]
                parquet_file = pq.ParquetFile(first_file, filesystem=s3fs)
                schema = parquet_file.schema
                
                # Get all column names
                all_columns = [field.name for field in schema]
                
                # Look for query column by name (prioritize exact matches)
                # First check for exact matches
                for pattern in query_patterns:
                    for col in all_columns:
                        if col.lower() == pattern.lower():
                            query_column = col
                            break
                    if query_column:
                        break
                
                # If no exact match, look for partial matches
                if not query_column:
                    for pattern in query_patterns:
                        for col in all_columns:
                            if pattern.lower() in col.lower():
                                query_column = col
                                break
                        if query_column:
                            break
                
                # Get total size of all files
                for file_path in parquet_files[:10]:  # Check first 10 files for size
                    try:
                        finfo = s3fs.get_file_info(file_path)
                        total_size_mb += finfo.size / (1024**2)
                    except:
                        pass
                
                files_analyzed = len(parquet_files)
                
            except Exception as e:
                logger.error(f"Error reading parquet file: {e}")
                return {
                    "authenticated": True,
                    "error": f"Could not read parquet files: {str(e)}",
                    "files_found": len(parquet_files),
                    "response": "No"
                }
            
            if files_analyzed == 0:
                return {
                    "authenticated": True,
                    "error": "Could not read schema from any parquet files",
                    "files_found": len(parquet_files),
                    "response": "No"
                }
            
            response = "Yes" if query_column else "No"
            
            return {
                "authenticated": True,
                "format": "parquet",
                "files_found": len(parquet_files),
                "files_analyzed": files_analyzed,
                "total_size_mb": round(total_size_mb, 2),
                "common_columns": sorted(list(all_columns)),
                "query_column": query_column,
                "response": response,
                "message": f"Found query column '{query_column}' across {len(parquet_files)} files" if query_column else f"No query column identified in {len(parquet_files)} files. Please specify manually.",
                "sample_files": parquet_files[:5]  # Show first 5 files as sample
            }
            
        except Exception as e:
            return {
                "authenticated": True,
                "error": f"Error scanning directory: {str(e)}",
                "response": "No"
            }
        
    except Exception as e:
        logger.error(f"Error in validate_s3_bucket: {str(e)}", exc_info=True)
        return {
            "authenticated": False,
            "error": f"Validation failed: {str(e)}",
            "response": "No"
        }


if __name__ == "__main__":
    uvicorn.run("converter_api:app", host="localhost", port=8080, proxy_headers=True, workers=5)
