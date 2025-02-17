from fastapi import APIRouter, Form, HTTPException
from typing import Optional
from apis.utils.helpers import (
    extract_functions_from_query,
    categorize_functions,
    unsupported_functionality_identifiers,
    transpile_query,
    strip_comment,
    add_comment_to_query,
    extract_udfs,
    load_supported_functions,
)
from sqlglot import parse_one
import re

router = APIRouter()


@router.post("/stats")
async def stats_api(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    """
    API endpoint to extract supported and unsupported SQL functions from a query.
    """
    try:
        supported_functions_in_e6 = load_supported_functions("E6")

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

        # Transpile the query and analyze unsupported functions post-transpilation
        original_ast = parse_one(query, read=from_sql)
        supported, unsupported = unsupported_functionality_identifiers(
            original_ast, unsupported, supported
        )

        # Transpile the query to target SQL dialect
        converted_query = transpile_query(query, from_sql, to_sql)
        converted_query = add_comment_to_query(converted_query, comment)
        all_functions_converted_query = extract_functions_from_query(
            converted_query, function_pattern, keyword_pattern, exclusion_list
        )
        supported_in_converted, unsupported_in_converted = categorize_functions(
            all_functions_converted_query, supported_functions_in_e6, functions_as_keywords
        )

        converted_query_ast = parse_one(converted_query, read=to_sql)
        supported_in_converted, unsupported_in_converted = unsupported_functionality_identifiers(
            converted_query_ast, unsupported_in_converted, supported_in_converted
        )

        from_dialect_func_list = load_supported_functions(from_sql)

        udf_list, unsupported = extract_udfs(
            unsupported, from_dialect_func_list
        )

        executable = "NO" if unsupported_in_converted else "YES"

        return {
            "supported_functions": supported,
            "unsupported_functions": unsupported,
            "udf_list": udf_list,
            "converted-query": converted_query,
            "unsupported_functions_after_transpilation": unsupported_in_converted,
            "executable": executable,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
