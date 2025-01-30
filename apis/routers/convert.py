from fastapi import APIRouter, Form, HTTPException
from typing import Optional
import sqlglot
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import parse_one
from apis.utils.helpers import transpile_query

router = APIRouter()


@router.post("/convert-query")
async def convert_query(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    try:
        double_quotes_added_query = transpile_query(query, from_sql, to_sql)
        return {"converted_query": double_quotes_added_query}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
