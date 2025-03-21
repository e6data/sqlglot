from fastapi import APIRouter, Form, HTTPException
from typing import Optional
import os
from guardrail.main import StorageServiceClient

import re
import sqlglot
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlglot import parse_one
from apis.utils.helpers import (
    strip_comment,
    unsupported_functionality_identifiers,
    extract_functions_from_query,
    categorize_functions,
    add_comment_to_query,
    replace_struct_in_query,
    process_guardrail,
    transpile_query,
    extract_udfs,
    load_supported_functions,
)

router = APIRouter()

# Environment variables for Guardrail service
ENABLE_GUARDRAIL = os.getenv("ENABLE_GUARDRAIL", "False")
STORAGE_ENGINE_URL = os.getenv("STORAGE_ENGINE_URL", "localhost")
STORAGE_ENGINE_PORT = os.getenv("STORAGE_ENGINE_PORT", 9005)

# Initialize the storage service client if guardrail is enabled
storage_service_client = None
if ENABLE_GUARDRAIL.lower() == "true":
    print("Storage Engine URL: ", STORAGE_ENGINE_URL)
    print("Storage Engine Port: ", STORAGE_ENGINE_PORT)

    storage_service_client = StorageServiceClient(host=STORAGE_ENGINE_URL, port=STORAGE_ENGINE_PORT)
print("Storage Service Client is created")


@router.post("/guard")
async def guard(
    query: str = Form(...),
    schema: str = Form(...),
    catalog: str = Form(...),
):
    """Validate SQL queries against guardrails."""
    try:
        if storage_service_client is None:
            raise HTTPException(status_code=500, detail="Storage Service Not Initialized.")

        violations = process_guardrail(query, schema, catalog, storage_service_client)
        return {"action": "deny" if violations else "allow", "violations": violations}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transguard")
async def transguard(
    query: str = Form(...),
    schema: str = Form(...),
    catalog: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    """
    Transpile SQL queries from one dialect to another, then validate them against guardrails.
    """
    try:
        if storage_service_client is None:
            raise HTTPException(status_code=500, detail="Storage Service Not Initialized.")

        # Transpile the query from one SQL dialect to another
        transpiled_query = transpile_query(query, from_sql, to_sql)

        # Validate the transpiled query against guardrails
        violations = process_guardrail(transpiled_query, schema, catalog, storage_service_client)
        return {
            "action": "deny" if violations else "allow",
            "violations": violations,
            "transpiled_query": transpiled_query,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/guardstats")
async def guardstats(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
    schema: str = Form(...),
    catalog: str = Form(...),
):
    try:
        supported_functions_in_e6 = load_supported_functions("E6")

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

        if storage_service_client is None:
            raise HTTPException(status_code=500, detail="Storage Service Not Initialized.")

        violations = process_guardrail(query, schema, catalog, storage_service_client)
        return {
            "supported_functions": supported,
            "unsupported_functions": unsupported,
            "udf_list": udf_list,
            "converted-query": double_quotes_added_query,
            "unsupported_functions_after_transpilation": unsupported_in_converted,
            "executable": executable,
            "action": "deny" if violations else "allow",
            "violations": violations,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
