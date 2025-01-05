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

STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "localhost")
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", "9006")

print("Storage Engine URL: ", STORAGE_ENGINE_URL)
print("Storage Engine Port: ", STORAGE_ENGINE_PORT)

storage_service_client = StorageServiceClient(
    host=STORAGE_ENGINE_URL,
    port=STORAGE_ENGINE_PORT
)

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
        parsed = sqlglot.parse(query, error_level=None)
        # print("\nParsed is\n",parsed)
        queries , tables = extract_sql_components_per_table_with_alias(parsed) 
        # tables = client.get_table_names(catalog_name="hive", db_name="tpcds_1000")
        table_map = get_table_infos(tables)    
        # print("\nInfo is\n",info)
        print("\nGot info from Storage Service for tables -> ",tables,"\n")    
        violations_found = validate_queries(queries, table_map)
        
        if violations_found:
            return {"action": "deny", "violations": violations_found}
        else:
            return {"action": "allow","violations":[]}

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
        # This is the main method will which help in transpiling to our e6data SQL dialects from other dialects
        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]

        # This is additional steps to replace the STRUCT(STRUCT()) --> {{}}
        converted_query = replace_struct_in_query(converted_query)

        converted_query_ast = parse_one(converted_query, read=to_sql)
        double_quotes_added_query = quote_identifiers(converted_query_ast, dialect=to_sql).sql(
            dialect=to_sql
        )

        parsed = sqlglot.parse(double_quotes_added_query, error_level=None)

        # now lets validate the query
        queries , tables = extract_sql_components_per_table_with_alias(parsed) 

        # tables = client.get_table_names(catalog_name="hive", db_name="tpcds_1000")
        table_map = get_table_infos(tables,catalog=catalog,schema=schema)

        print("\nGot info from Storage Service for tables -> ",tables,"\n")    
        violations_found = validate_queries(queries, table_map)
        
        if violations_found:
            return {"action": "deny", "violations": violations_found}
        else:
            return {"action": "allow","violations":[]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("converter_api:app", host="localhost", port=8100, proxy_headers=True, workers=5)