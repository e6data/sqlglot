from fastapi import FastAPI, Form, HTTPException, Response
from typing import Optional
import uvicorn
import re

import sqlglot
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one

app = FastAPI()


def replace_struct_in_query(query):
    # Define the regex pattern to match Struct(Struct(anything))
    pattern = re.compile(r'Struct\s*\(\s*Struct\s*\(\s*([^\(\)]+)\s*\)\s*\)', re.IGNORECASE)

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
        to_sql: Optional[str] = Form("E6")
):
    try:
        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]
        converted_query = replace_struct_in_query(converted_query)
        converted_query_ast = parse_one(converted_query, read=to_sql)
        double_quotes_added_query = quote_identifiers(converted_query_ast, dialect=to_sql).sql(dialect=to_sql)
        return {"converted_query": double_quotes_added_query}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    return Response(status_code=200)


if __name__ == "__main__":
    uvicorn.run("converter_api:app", host="0.0.0.0", port=8100, proxy_headers=True, workers=5)
