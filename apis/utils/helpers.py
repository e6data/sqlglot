import re

import sqlglot
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import exp, parse_one
import typing as t


def transpile_query(query: str, from_sql: str, to_sql: str) -> str:
    """
    Transpile a SQL query from one dialect to another.
    """
    try:
        transpiled_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=False)[0]

        # Parse and reformat the query to add proper quoting
        transpiled_query_ast = parse_one(transpiled_query, read=to_sql)
        transpiled_query_with_quotes = quote_identifiers(transpiled_query_ast, dialect=to_sql).sql(
            dialect=to_sql
        )
        transpiled_query_with_quotes = replace_struct_in_query(transpiled_query_with_quotes)

        return transpiled_query_with_quotes
    except Exception as e:
        raise ValueError(f"Error transpiling query: {e}")


def replace_struct_in_query(query: str) -> str:
    """
    Replace STRUCT(STRUCT()) pattern in SQL queries.
    Example: STRUCT(STRUCT(some_value)) → {{some_value}}
    """
    pattern = re.compile(r"Struct\s*\(\s*Struct\s*\(\s*([^\(\)]+)\s*\)\s*\)", re.IGNORECASE)

    def replace_match(match):
        return f"{{{{{match.group(1)}}}}}"

    return pattern.sub(replace_match, query) if query else query


def process_guardrail(query, schema, catalog, storage_service_client):
    """
    Validate a SQL query against guardrails.
    """
    from sqlglot import parse
    from guardrail.main import extract_sql_components_per_table_with_alias, get_table_infos
    from guardrail.rules_validator import validate_queries

    parsed = parse(query, error_level=None)
    queries, tables = extract_sql_components_per_table_with_alias(parsed)
    table_map = get_table_infos(tables, storage_service_client, catalog, schema)
    return validate_queries(queries, table_map)


def find_double_pipe(query: str) -> list:
    """
    Find '||' used as a string concatenation operator.
    """
    return [(match.start(), match.end()) for match in re.finditer(r"\|\|", query)]


def process_query(query: str) -> str:
    """
    Process the query to handle string literals (' or ") while correctly handling escaped quotes.
    """
    sanitized_query = []
    inside_single_quote = False
    inside_double_quote = False
    i = 0

    while i < len(query):
        char = query[i]

        # Check for escaped single quote (\')
        if char == "'" and not inside_double_quote:
            # Check if it is escaped (i.e., preceded by an odd number of backslashes)
            backslash_count = 0
            j = i - 1
            while j >= 0 and query[j] == "\\":
                backslash_count += 1
                j -= 1

            if backslash_count % 2 == 0:  # Even backslashes mean it's not escaped
                inside_single_quote = not inside_single_quote
                sanitized_query.append(" ")  # Replace with space
            else:
                sanitized_query.append(char)  # Keep escaped quote as is

        # Check for escaped double quote (\")
        elif char == '"' and not inside_single_quote:
            backslash_count = 0
            j = i - 1
            while j >= 0 and query[j] == "\\":
                backslash_count += 1
                j -= 1

            if backslash_count % 2 == 0:  # Even backslashes mean it's not escaped
                inside_double_quote = not inside_double_quote
                sanitized_query.append(" ")  # Replace with space
            else:
                sanitized_query.append(char)  # Keep escaped quote as is

        # Replace characters inside string literals with spaces
        elif inside_single_quote or inside_double_quote:
            sanitized_query.append(" ")  # Replace content inside literals with spaces

        else:
            sanitized_query.append(char)  # Keep other characters

        i += 1  # Move to the next character

    return "".join(sanitized_query)


def extract_functions_from_query(
    query: str, function_pattern: str, keyword_pattern: str, exclusion_list: list
) -> set:
    """
    Extract function names from the sanitized query.
    """
    sanitized_query = processing_comments(query)
    sanitized_query = process_query(sanitized_query)
    print(f"sanitized query:\n{sanitized_query}\n")

    all_functions = set()

    # Match functions requiring parentheses
    matches = re.findall(function_pattern, sanitized_query.upper())
    for match in matches:
        if match not in exclusion_list:  # Exclude unwanted tokens
            all_functions.add(match)

    # Match keywords treated as functions
    keyword_matches = re.findall(keyword_pattern, sanitized_query.upper())
    for match in keyword_matches:
        all_functions.add(match)

    # Handle '||' as a function-like operator
    pipe_matches = find_double_pipe(query)
    if pipe_matches:
        all_functions.add("||")

    print(f"all functions: {all_functions}")

    return all_functions


def unsupported_functionality_identifiers(
    expression, unsupported_list: t.List, supported_list: t.List
):
    for sub in expression.find_all(exp.Sub):
        if (
            isinstance(sub.args.get("this"), (exp.CurrentDate, exp.CurrentTimestamp))
            and sub.expression.is_int
        ):
            unsupported_list.append(sub.sql())

    for cte in expression.find_all(exp.CTE, exp.Subquery):
        cte_name = cte.alias.upper()
        if cte_name in unsupported_list:
            unsupported_list.remove(cte_name)

    for filter_expr in expression.find_all(exp.Filter, exp.ArrayFilter):
        if isinstance(filter_expr, exp.Filter) and unsupported_list.count("FILTER") > 0:
            unsupported_list.remove("FILTER")
            supported_list.append("FILTER as projection")

        elif isinstance(filter_expr, exp.ArrayFilter) and unsupported_list.count("FILTER") > 0:
            unsupported_list.remove("FILTER")
            unsupported_list.append("FILTER as filter_array")

    return supported_list, unsupported_list


def processing_comments(query: str) -> str:
    """
    Process a SQL query to remove single-line comments starting with '--'.

    Args:
        query (str): The input SQL query.

    Returns:
        str: The SQL query with single-line comments removed.
    """
    # Remove block comments (multi-line `/* ... */`)
    query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)
    processed_lines = []

    for line in query.splitlines():
        # print(f"{line}\n\n")
        if "--" in line:  # Check if the line contains a comment
            # Split the line at the first occurrence of '--' and take the part before it
            non_comment_part = line.split("--", 1)[0].rstrip()
            if non_comment_part:  # If the non-comment part is not empty, add it
                processed_lines.append(non_comment_part)
        else:
            # If no comment, keep the entire line
            processed_lines.append(line)

    # Join all processed lines back into a single string
    return "\n".join(processed_lines)


def categorize_functions(extracted_functions, supported_functions_in_e6, functions_as_keywords):
    """
    Categorize functions into supported and unsupported.
    """
    supported_functions = set()
    unsupported_functions = set()

    for func in extracted_functions:
        if func in supported_functions_in_e6 or func in functions_as_keywords:
            supported_functions.add(func)
        else:
            unsupported_functions.add(func)

    return list(supported_functions), list(unsupported_functions)
