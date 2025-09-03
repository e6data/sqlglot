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

# Set AWS credentials
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_ACCESS_KEY")
os.environ["AWS_SESSION_TOKEN"] = os.getenv("AWS_SESSION_TOKEN", "YOUR_SESSION_TOKEN")
os.environ["AWS_REGION"] = os.getenv("AWS_REGION", "us-east-1")
from datetime import datetime
from log_collector import setup_logger, log_records
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.compute as pc
from pathlib import Path
import pandas as pd
from io import BytesIO
import time
import uuid
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


# Import distributed processing modules
import sys
sys.path.append('./final_distributed_processing')
sys.path.append('./automated_processing')
from automated_processing.orchestrator import orchestrate_processing, get_processing_status, get_task_result


@app.post("/process-parquet-directory-automated")
async def process_parquet_directory_automated(
    directory_path: str = Form(..., description="Path to directory containing parquet files OR path to a single parquet file"),
    company_name: str = Form(..., description="Company identifier for Iceberg partitioning"),
    from_dialect: str = Form(..., description="Source SQL dialect (e.g., snowflake, bigquery)"),
    to_dialect: str = Form("e6", description="Target SQL dialect"),
    query_column: str = Form(..., description="Column name containing SQL queries"),
    batch_size: int = Form(10000, description="Number of queries per batch"),
    filters: Optional[str] = Form(None, description="JSON string of column filters e.g. '{\"statement_type\": \"SELECT\", \"client_application\": \"PowerBI\"}'"),
    session_name: Optional[str] = Form(None, description="Custom session name for identification")
):
    """
    Batch processing endpoint for parquet files containing SQL queries.
    
    Accepts either:
    - Path to a directory containing parquet files (e.g., "/path/to/parquet_files/")
    - Path to a single parquet file (e.g., "/path/to/file.parquet")
    - S3 directory path (e.g., "s3://bucket/path/to/directory/")
    - S3 single file path (e.g., "s3://bucket/path/to/file.parquet")
    
    Processes queries through SQLGlot transpilation using Celery distributed workers.
    Results are stored in Iceberg table with partitioning by company_name and event_date.
    """
    
    logger.info(f"🚀 Starting FULLY AUTONOMOUS processing: {directory_path} ({from_dialect} -> {to_dialect})")
    
    try:
        # Validate inputs first
        if not directory_path or not directory_path.strip():
            raise HTTPException(status_code=400, detail="directory_path is required")
        
        if not query_column or not query_column.strip():
            raise HTTPException(status_code=400, detail="query_column is required")
        
        if batch_size <= 0:
            raise HTTPException(status_code=400, detail="batch_size must be positive")
        
        # Parse filters if provided
        filter_dict = {}
        if filters and filters.strip():
            try:
                filter_dict = json.loads(filters)
                logger.info(f"Parsed filters: {filter_dict}")
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON format for filters parameter")
        
        # Use the new simplified orchestrator
        logger.info("🔧 Starting processing with Celery orchestrator...")
        
        # Call the orchestrator which returns immediately
        result = orchestrate_processing(
            directory_path=directory_path.strip(),
            company_name=company_name.strip(),
            from_dialect=from_dialect.lower().strip(),
            to_dialect=to_dialect.lower().strip(),
            query_column=query_column.strip(),
            batch_size=batch_size,
            filters=filter_dict,
            name=session_name.strip() if session_name else None
        )
        
        # Check if there was an error
        if 'error' in result:
            logger.error(f"❌ Orchestration failed: {result['error']}")
            raise HTTPException(status_code=500, detail=result['error'])
        
        logger.info(f"✅ Processing started with session {result['session_id']}")
        
        # Fixed Iceberg storage structure
        event_date = datetime.now().strftime('%Y-%m-%d')
        iceberg_structure = f"company_name={company_name}/event_date={event_date}/"
        
        logger.info(f"📂 Iceberg structure: {iceberg_structure}")
        
        return {
            "session_id": result['session_id'],
            "total_files": result.get('total_files', 0),
            "total_batches": result.get('total_batches', 0),
            "task_ids": result.get('task_ids', []),
            "status": result.get('status', 'processing'),
            "created_at": result.get('created_at'),
            "status_url": f"/processing-status/{result['session_id']}",
            "configuration": {
                "directory_path": directory_path,
                "company_name": company_name,
                "query_column": query_column,
                "batch_size": batch_size,
                "dialect_conversion": f"{from_dialect} -> {to_dialect}"
            },
            "iceberg_storage": {
                "table": "default.batch_statistics",
                "partition_structure": iceberg_structure,
                "storage_pattern": f"{iceberg_structure}session_{result['session_id']}_batch_{{batch_id}}.parquet"
            },
            "message": "Processing initiated! Monitor progress at the status_url."
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"❌ Error in autonomous processing: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start autonomous processing: {str(e)}")


@app.get("/processing-status/{session_id}")
async def get_processing_session_status(session_id: str):
    """Get status of automated processing session"""
    try:
        # Let the orchestrator handle task discovery from Redis
        result = get_processing_status(session_id, [])
        return result
    except Exception as e:
        logger.error(f"❌ Error getting session status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get session status: {str(e)}")


@app.get("/task-result/{task_id}")
async def get_individual_task_result(task_id: str):
    """Get result of a specific task"""
    try:
        result = get_task_result(task_id)
        return result
    except Exception as e:
        logger.error(f"❌ Error getting task result: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task result: {str(e)}")

@app.post("/validate-s3-bucket")
async def validate_s3_bucket(
    s3_path: str = Form(...),
):
    try:
        if not s3_path.startswith('s3://'):
            return {"authenticated": False, "error": "Invalid S3 path"}
        
        bucket = s3_path.split('/')[2]
        key_prefix = '/'.join(s3_path.split('/')[3:])
        
        try:
            s3fs = fs.S3FileSystem(
                access_key=os.getenv("AWS_ACCESS_KEY_ID", "YOUR_ACCESS_KEY_ID"),
                secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET_ACCESS_KEY"),
                session_token=os.getenv("AWS_SESSION_TOKEN", "YOUR_SESSION_TOKEN"),
                region=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            )
        except Exception as e:
            return {"authenticated": False, "error": f"S3 auth failed: {str(e)}"}
        
        path = f"{bucket}/{key_prefix}" if key_prefix else bucket
        file_info = s3fs.get_file_info(path)
        
        parquet_files = []
        if file_info.type == fs.FileType.File and path.endswith('.parquet'):
            parquet_files = [path]
        else:
            from pyarrow.fs import FileSelector
            selector = FileSelector(path, recursive=True)
            file_infos = s3fs.get_file_info(selector)
            parquet_files = [f.path for f in file_infos if f.type == fs.FileType.File and f.path.endswith('.parquet')]
        
        if not parquet_files:
            return {"authenticated": True, "error": "No parquet files found"}
        
        parquet_file = pq.ParquetFile(parquet_files[0], filesystem=s3fs)
        all_columns = [field.name for field in parquet_file.schema]
        
        return {
            "authenticated": True,
            "columns": all_columns
        }
        
    except Exception as e:
        return {"authenticated": False, "error": f"Validation failed: {str(e)}"}


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
