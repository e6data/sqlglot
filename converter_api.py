from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional
import typing as t
import uvicorn
import re
import os
import sqlglot
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
)

if t.TYPE_CHECKING:
    from sqlglot._typing import E

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
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    try:
        # This is the main method will which help in transpiling to our e6data SQL dialects from other dialects
        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]

        # This is additional steps to replace the STRUCT(STRUCT()) --> {{}}
        converted_query = replace_struct_in_query(converted_query)

        converted_query_ast = parse_one(converted_query, read=to_sql)
        double_quotes_added_query = quote_identifiers(converted_query_ast, dialect=to_sql).sql(
            dialect=to_sql
        )

        return {"converted_query": double_quotes_added_query}
    except Exception as e:
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
    to_sql: Optional[str] = Form("E6"),
):
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
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    """
    API endpoint to extract supported and unsupported SQL functions from a query.
    """
    try:
        supported_functions_in_e6 = load_supported_functions(to_sql)

        # Functions treated as keywords (no parentheses required)
        functions_as_keywords = ["LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT"]

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
        supported, unsupported = unsupported_functionality_identifiers(
            original_ast, unsupported, supported
        )
        values_ensured_ast = ensure_select_from_values(original_ast)
        query = values_ensured_ast.sql(from_sql)

        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]
        converted_query = replace_struct_in_query(converted_query)

        converted_query_ast = parse_one(converted_query, read=to_sql)
        double_quotes_added_query = quote_identifiers(converted_query_ast, dialect=to_sql).sql(
            dialect=to_sql
        )
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

        from_dialect_function_list = load_supported_functions(from_sql)

        udf_list, unsupported = extract_udfs(unsupported, from_dialect_function_list)

        executable = "NO" if unsupported_in_converted else "YES"

        return {
            "supported_functions": supported,
            "unsupported_functions": unsupported,
            "udf_list": udf_list,
            "converted-query": double_quotes_added_query,
            "unsupported_functions_after_transpilation": unsupported_in_converted,
            "executable": executable,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/guardstats")
async def guardstats(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
    schema: str = Form(...),
    catalog: str = Form(...),
):
    try:
        supported_functions_in_e6 = load_supported_functions(to_sql)

        # Functions treated as keywords (no parentheses required)
        functions_as_keywords = ["LIKE", "ILIKE", "RLIKE", "AT TIME ZONE", "||", "DISTINCT"]

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
        supported, unsupported = unsupported_functionality_identifiers(
            original_ast, unsupported, supported
        )
        values_ensured_ast = ensure_select_from_values(original_ast)
        query = values_ensured_ast.sql(dialect=from_sql)

        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]
        converted_query = replace_struct_in_query(converted_query)

        converted_query_ast = parse_one(converted_query, read=to_sql)
        double_quotes_added_query = quote_identifiers(converted_query_ast, dialect=to_sql).sql(
            dialect=to_sql
        )
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

            if violations_found:
                return {
                    "supported_functions": supported,
                    "unsupported_functions": unsupported,
                    "udf_list": udf_list,
                    "converted-query": double_quotes_added_query,
                    "unsupported_functions_after_transpilation": unsupported_in_converted,
                    "executable": executable,
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
