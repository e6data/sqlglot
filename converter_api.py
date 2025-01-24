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
from sqlglot.expressions import EQ, Column, Table, Identifier, Join

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


@app.post("/parse-query")
async def parse_query(
    queries: str = Form(...),  # Accept multiple queries as a single string
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    """
    Accepts multiple SQL queries as input, processes each one, and returns their components and transpiled versions.
    Queries should be separated by a semicolon (;).
    """
    try:
        # Split the input into individual queries by semicolon
        query_list = [q.strip() for q in queries.split(";") if q.strip()]
        if not query_list:
            raise HTTPException(status_code=400, detail="No valid queries provided.")

        results = []

        for query in query_list:
            # Transpile the query to the target SQL dialect
            converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]
            converted_query = replace_struct_in_query(converted_query)

            # Parse the converted query
            parsed = sqlglot.parse(converted_query, error_level=None)

            # Extract details (tables, columns, joins, etc.)
            extracted_components, tables = extract_sql_components_per_table_with_alias(parsed)

            # Build the alias mapping for joins
            alias_mapping = {}
            for expression in parsed:
                for table in expression.find_all(Table):
                    if table.alias:
                        alias_mapping[table.alias] = table.name

            # Handle joins explicitly
            for expression in parsed:
                for join in expression.find_all(Join):
                    on_expression = join.args.get("on")
                    if on_expression:
                        if isinstance(on_expression, EQ):
                            left = on_expression.this
                            right = on_expression.expression

                            left_table = ""
                            right_table = ""

                            if isinstance(left, Column):
                                left_table_alias = left.table
                                left_column = left.name
                                left_table = alias_mapping.get(left_table_alias, left_table_alias)
                            else:
                                left_column = None

                            # Extract right column
                            if isinstance(right, Column):
                                right_table_alias = right.table
                                right_column = right.name
                                right_table = alias_mapping.get(right_table_alias, right_table_alias)
                            else:
                                right_column = None

                            # Append join details to results
                            results.append(
                                {
                                    "alias": alias_mapping,
                                    "left_name": left_table_alias,
                                    "left_table": left_table,
                                    "right_name": right_table_alias,
                                    "right_table": right_table,
                                    "left_column": left_column,
                                    "right_column": right_column,
                                }
                            )

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("converter_api:app", host="localhost", port=8100, proxy_headers=True, workers=5)

