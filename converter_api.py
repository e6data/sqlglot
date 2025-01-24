from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional
import uvicorn
import re
import os
import sqlglot
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one
from guardrail.main import StorageServiceClient
from guardrail.main import extract_sql_components_per_table_with_alias, get_table_infos
from guardrail.rules_validator import validate_queries


ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv(
    "STORAGE_ENGINE_URL", "cops-beta1-storage-storage-blue"
)  # cops-beta1-storage-storage-blue
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", "9006")

storage_service_client = None

if ENABLE_GUARDRAIL.lower() == "true":
    print("Storage Engine URL: ", STORAGE_ENGINE_URL)
    print("Storage Engine Port: ", STORAGE_ENGINE_PORT)

    storage_service_client = StorageServiceClient(host=STORAGE_ENGINE_URL, port=STORAGE_ENGINE_PORT)

print("Storage Service Client is created")
app = FastAPI()


def replace_struct_in_query(query):
    """

    Replace struct in query with struct in query.
    # TODO:: Document this functions.
    #       STRUCT(STRUCT()) --> {{}}

    """

    # Define the regex pattern to match Struct(Struct(anything))
    pattern = re.compile(r"Struct\s*\(\s*Struct\s*\(\s*([^\(\)]+)\s*\)\s*\)", re.IGNORECASE)

    # Function to perform the replacement
    def replace_match(match):
        return f"{{{{{match.group(1)}}}}}"

    # Process the query
    if query is not None:
        modified_query = pattern.sub(replace_match, query)
        return modified_query
    return query


@app.post("/convert-query")
async def convert_query(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    try:
        # This is the main method will which help in transpiling to our e6data SQL dialects from other dialects
        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]

        # SELECT "COL1", sum("COL2"), "ABS()" from table1 group by col2.

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


@app.post("/extract-functions")
async def extract_functions_api(query: str = Form(...)):
    """
    API endpoint to extract supported and unsupported SQL functions from a query.
    """
    try:
        # List of SQL functions (requiring parentheses) that you support
        supported_functions_in_e6 = [
            "AVG",
            "COUNT",
            "DISTINCT",
            "MAX",
            "MIN",
            "SUM",
            "ARBITRARY",
            "ANY_VALUE",
            "COALESCE",
            "CONCAT",
            "LISTAGG",
            "STRING_AGG",
            "CEIL",
            "FLOOR",
            "ROUND",
            "ABS",
            "SIGN",
            "MOD",
            "POWER",
            "NULLIF",
            "FACTORIAL",
            "CBRT",
            "SQRT",
            "EXP",
            "SIN",
            "SINH",
            "COS",
            "COSH",
            "ACOSH",
            "TANH",
            "COT",
            "DEGREES",
            "RADIANS",
            "PI",
            "LN",
            "CHARACTER_LENGTH",
            "CHAR_LENGTH",
            "LEN",
            "LENGTH",
            "REPLACE",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "LOWER",
            "UPPER",
            "SUBSTRING",
            "SUBSTR",
            "INITCAP",
            "CHARINDEX",
            "POSITION",
            "RIGHT",
            "LEFT",
            "LOCATE",
            "CONTAINS_SUBSTR",
            "INSTR",
            "SOUNDEX",
            "SPLIT",
            "SPLIT_PART",
            "ASCII",
            "REPEAT",
            "ENDSWITH",
            "ENDB_WITH",
            "STARTSWITH",
            "STARTS_WITH",
            "STRPOS",
            "LPAD",
            "RPAD",
            "REVERSE",
            "TO_CHAR",
            "TO_VARCHAR",
            "TRY_CAST",
            "CAST",
            "CURRENT_DATE",
            "CURRENT_TIMESTAMP",
            "NOW",
            "DATE",
            "TIMESTAMP",
            "TO_DATE",
            "TO_TIMESTAMP",
            "FROM_UNIXTIME_WITHUNIT",
            "TO_UNIX_TIMESTAMP",
            "PARSE_DATE",
            "PARSE_DATETIME",
            "PARSE_TIMESTAMP",
            "DATE_TRUNC",
            "DATE_ADD",
            "DATEADD",
            "DATE_DIFF",
            "DATEDIFF",
            "TIMESTAMP_ADD",
            "TIMESTAMP_DIFF",
            "EXTRACT",
            "DATEPART",
            "WEEK",
            "YEAR",
            "MONTH",
            "DAYS",
            "LAST_DAY",
            "DAYNAME",
            "HOUR",
            "MINUTE",
            "SECOND",
            "DAYOFWEEKISO",
            "WEEKOFYEAR",
            "WEEKISO",
            "FORMAT_DATE",
            "FORMAT_TIMESTAMP",
            "DATE_FORMAT",
            "DATETIME",
            "CONVERT_TIMEZONE",
            "RANK",
            "DENSE_RANK",
            "ROW_NUMBER",
            "NTILE",
            "FIRST_VALUE",
            "LAST_VALUE",
            "LEAD",
            "LAG",
            "COLLECT_LIST",
            "STDDEV",
            "STDDEV_POP",
            "IN",
            "PERCENTILE_CONT",
            "APPROX_QUANTILES",
            "BITWISE_OR",
            "BITWISE_XOR",
            "BITWISE_NOT",
            "SHIFTRIGHT",
            "APPROX_COUNT_DISTINCT",
            "SHIFTLEFT",
            "SUBSCRIPT OPERATOR",
            "ELEMENT_AT",
            "ARRAY_POSITION",
            "SIZE",
            "ARRAY_TO_STRING",
            "ARRAY_AGG",
            "ARRAY_APPEND",
            "ARRAY_PREPEND",
            "ARRAY_CONCAT",
            "ARRAY_CONTAINS",
            "ARRAY_JOIN",
            "ARRAY_SLICE",
            "FILTER_ARRAY",
            "UNNEST",
            "REGEXP_CONTAINS",
            "REGEXP_REPLACE",
            "REGEXP_EXTRACT",
            "REGEXP_EXTRACT_ALL",
            "REGEXP_COUNT",
            "REGEXP_LIKE",
            "JSON_VALUE",
            "JSON_EXTRACT",
            "TO_JSON",
            "MD5",
            "NAMED_STRUCT",
        ]

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
        ]

        # Regex patterns
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = (
            r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
        )

        def find_double_pipe(query):
            """Find '||' used as a string concatenation operator."""
            return re.findall(r"\|\|", query)

        def extract_functions(query):
            """Extract all function names from a query."""
            all_functions = set()

            # Step 1: Find all occurrences of '||'
            pipe_matches = find_double_pipe(query)
            if pipe_matches:
                for match in pipe_matches:
                    all_functions.add("||")

            # Step 2: Match functions requiring parentheses
            matches = re.findall(function_pattern, query.upper())
            for match in matches:
                if match not in exclusion_list:
                    all_functions.add(match)

            # Step 3: Match keywords treated as functions
            keyword_matches = re.findall(keyword_pattern, query.upper())
            for match in keyword_matches:
                all_functions.add(match)

            return all_functions

        def categorize_functions(extracted_functions):
            """Categorize functions into supported and unsupported."""
            supported_functions = set()
            unsupported_functions = set()

            for func in extracted_functions:
                if func in supported_functions_in_e6 or func in functions_as_keywords:
                    supported_functions.add(func)
                else:
                    unsupported_functions.add(func)

            return list(supported_functions), list(unsupported_functions)

        # Extract functions
        all_functions = extract_functions(query)
        supported, unsupported = categorize_functions(all_functions)

        return {
            "supported_functions": supported,
            "unsupported_functions": unsupported,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("converter_api:app", host="localhost", port=8100, proxy_headers=True, workers=5)
