from fastapi import APIRouter, Form, HTTPException
from typing import Optional
from apis.utils.helpers import (
    extract_functions_from_query,
    categorize_functions,
    unsupported_functionality_identifiers,
    transpile_query,
)
from sqlglot import parse_one
import re

router = APIRouter()


@router.post("/")
async def stats_api(
    query: str = Form(...),
    from_sql: str = Form(...),
    to_sql: Optional[str] = Form("E6"),
):
    """
    API endpoint to extract supported and unsupported SQL functions from a query.
    """
    try:
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
            "HAVING",
            "APPROX_PERCENTILE",
            "USING",
            "EXISTS",
            "CARDINALITY",
            "FILTER",
            "IF",
            "IFNULL",
            "ISNULL",
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
            "SELECT",
            "BY",
        ]

        # Regex patterns
        function_pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("
        keyword_pattern = (
            r"\b(?:" + "|".join([re.escape(func) for func in functions_as_keywords]) + r")\b"
        )

        # Extract functions from the query
        all_functions = extract_functions_from_query(
            query, function_pattern, keyword_pattern, exclusion_list
        )
        supported, unsupported = categorize_functions(
            all_functions, supported_functions_in_e6, functions_as_keywords
        )

        # Transpile the query and analyze unsupported functions post-transpilation
        original_ast = parse_one(query, read=from_sql)
        unsupported = unsupported_functionality_identifiers(original_ast, unsupported)

        # Transpile the query to target SQL dialect
        converted_query = transpile_query(query, from_sql, to_sql)
        all_functions_converted_query = extract_functions_from_query(
            converted_query, function_pattern, keyword_pattern, exclusion_list
        )
        supported_in_converted, unsupported_in_converted = categorize_functions(
            all_functions_converted_query, supported_functions_in_e6, functions_as_keywords
        )

        executable = "NO" if unsupported_in_converted else "YES"

        return {
            "supported_functions": supported,
            "unsupported_functions": unsupported,
            "converted-query": converted_query,
            "unsupported_functions_after_transpilation": unsupported_in_converted,
            "executable": executable,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
