from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional
import typing as t
import uvicorn
import re
import os
import sqlglot
import logging
from datetime import datetime
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
)

if t.TYPE_CHECKING:
    from sqlglot._typing import E

logger = logging.getLogger("uvicorn.error")

ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "localhost")  # cops-beta1-storage-storage-blue
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", 9005)

storage_service_client = None

if ENABLE_GUARDRAIL.lower() == "true":
    print("Storage Engine URL: ", STORAGE_ENGINE_URL)
    print("Storage Engine Port: ", STORAGE_ENGINE_PORT)

    storage_service_client = StorageServiceClient(host=STORAGE_ENGINE_URL, port=STORAGE_ENGINE_PORT)

print("Storage Service Client is created")
app = FastAPI()


@app.post("/convert-query")
async def convert_query(
    query: str = Form(...),
    query_id: Optional[str] = Form("NO_ID_MENTIONED"),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("e6"),
):
    timestamp = datetime.now().isoformat()
    to_sql = to_sql.lower()
    try:
        tree = sqlglot.parse_one(query, read=from_sql, error_level=None)

        tree2 = quote_identifiers(tree, dialect=to_sql)

        double_quotes_added_query = tree2.sql(dialect=to_sql, from_dialect=from_sql)

        double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

        logger.info(
            f"{query_id} AT {timestamp} FROM {from_sql.upper()}\n"
            "-----------------------\n"
            "--- Original query ---\n"
            "-----------------------\n"
            f"{query}"
            "-----------------------\n"
            "--- Transpiled query ---\n"
            "-----------------------\n"
            f"{double_quotes_added_query}"
        )
        return {"converted_query": double_quotes_added_query}
    except Exception as e:
        logger.info(
            f"{query_id} AT {timestamp} FROM {from_sql.upper()}\n"
            "-----------------------\n"
            "--- Original query ---\n"
            "-----------------------\n"
            f"{query}"
            "-----------------------\n"
            "-------- Error --------\n"
            "-----------------------\n"
            f"{str(e)}"
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
            print("table info is ", table_map)

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
):
    """
    API endpoint to extract supported and unsupported SQL functions from a query.
    """
    timestamp = datetime.now().isoformat()
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
            "SETS",
        ]

        # Regex patterns
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = (
            r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
        )

        if not query.strip():
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
            query = values_ensured_ast.sql(from_sql)

            # ------------------------------
            # Step 2: Transpile the Query
            # ------------------------------
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
                f"{query_id} AT {timestamp} FROM {from_sql.upper()}\n"
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
                f"{query_id} AT {timestamp} FROM {from_sql.upper()}\n"
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
            print(error_message)
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
        }

    except Exception as e:
        logger.info(
            f"{query_id} AT {timestamp} FROM {from_sql.upper()}\n"
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
        print(f"supported: {supported}\n\nunsupported: {unsupported}")

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
            print("table info is ", table_map)

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
                }
        else:
            detail = (
                "Storage Service Not Initialized. Guardrail service status: " + ENABLE_GUARDRAIL
            )
            raise HTTPException(status_code=500, detail=detail)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("converter_api:app", host="localhost", port=8100, proxy_headers=True, workers=5)
