import re
from typing import Optional, Set, Type
import json
import logging
import os
import unicodedata

import sqlglot
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot import exp, parse_one
import typing as t
from sqlglot.dialects.e6 import E6
from curses.ascii import isascii

FUNCTIONS_FILE = "./apis/utils/supported_functions_in_all_dialects.json"
logger = logging.getLogger(__name__)

# Functions treated as keywords (no parentheses required)
FUNCTIONS_AS_KEYWORDS = [
    "LIKE",
    "ILIKE",
    "RLIKE",
    "AT TIME ZONE",
    "||",
    "DISTINCT",
    "QUALIFY",
]

# Exclusion list for words that are followed by '(' but are not functions
EXCLUSION_LIST = [
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
    "EXCEPT",
    "SETS",
]

# Regex pattern to match function calls
FUNCTION_PATTERN = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\("


def get_keyword_pattern() -> str:
    """Generate regex pattern for keyword functions."""
    return r"\b(?:" + "|".join([re.escape(func) for func in FUNCTIONS_AS_KEYWORDS]) + r")\b"


def transpile_query(query: str, from_sql: str, to_sql: str) -> str:
    """
    Transpile a SQL query from one dialect to another.
    """
    try:
        # original_ast = parse_one(query, read=from_sql)
        # values_ensured_ast = ensure_select_from_values(original_ast)
        # query = values_ensured_ast.sql(dialect=from_sql)
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
    try:
        pattern = re.compile(r"Struct\s*\(\s*Struct\s*\(\s*([^\(\)]+)\s*\)\s*\)", re.IGNORECASE)

        def replace_match(match):
            return f"{{{{{match.group(1)}}}}}"

        return pattern.sub(replace_match, query) if query else query
    except re.error as e:
        logging.error(f"Regex Error replacing struct in query: {e}")
        return query


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
    # logger.info("Extracting functions from query")
    sanitized_query = processing_comments(query)

    try:
        sanitized_query = process_query(sanitized_query)
    except Exception as e:
        logger.warning(f"Error while processing the query to handle string literals: {e}")

    all_functions = set()

    # Match functions requiring parentheses
    try:
        matches = re.findall(function_pattern, sanitized_query.upper())
        for match in matches:
            if not re.search(r"\bAS\s+" + re.escape(match), sanitized_query.upper()):
                if match not in exclusion_list:  # Exclude unwanted tokens
                    all_functions.add(match)
    except re.error as e:
        logging.warning(f"Regex Error matching functions requiring parenthesis: {e}")

    # Match keywords treated as functions
    try:
        keyword_matches = re.findall(keyword_pattern, sanitized_query.upper())
        for match in keyword_matches:
            all_functions.add(match)
    except re.error as e:
        logging.warning(f"Regex error matching keywords: {e}")

    # Handle '||' as a function-like operator
    pipe_matches = find_double_pipe(query)
    if pipe_matches:
        all_functions.add("||")

    return all_functions


def unsupported_functionality_identifiers(
    expression, unsupported_list: t.List, supported_list: t.List
):
    # logger.info("Identifying unsupported functionality.....")
    try:
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

        for parametrised in expression.find_all(exp.Placeholder):
            unsupported_list.append(f":{parametrised.this}")

        for casting in expression.find_all(exp.Cast):
            cast_to = casting.args.get("to").this.name
            if cast_to not in E6.Parser.SUPPORTED_CAST_TYPES:
                unsupported_list.append(f"UNSUPPORTED_CAST_TYPE:{cast_to}")

        if expression.find(exp.GroupingSets):
            supported_list.append(f"GROUPING SETS")
    except Exception as e:
        logger.warning(f"Unexpected error in unsupported_functionality_identifiers: {e}")

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
    try:
        query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)
    except TypeError as e:
        logging.error(f"[processing_comments] Invalid input type: {e}")
    except re.error as e:
        logging.error(f"[processing_comments] Regex error: {e}")
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
    # logger.info("Categorizing extracted functions into supported and unsupported.....")
    supported_functions = set()
    unsupported_functions = set()

    for func in extracted_functions:
        if func in supported_functions_in_e6 or func in functions_as_keywords:
            supported_functions.add(func)
        else:
            unsupported_functions.add(func)

    return list(supported_functions), list(unsupported_functions)


def add_comment_to_query(query: str, comment: str) -> str:
    """
    Add a comment to the first SELECT statement in the query.

    Args:
        query (str): The SQL query to process.
        comment (str): The comment to add.

    Returns:
        str: The modified query with the comment added.
    """
    if comment:
        # Regex to find the first SELECT
        select_pattern = r"\bSELECT\b"
        try:
            match = re.search(select_pattern, query, re.IGNORECASE)
            if match:
                # Insert the comment immediately after the first SELECT
                insert_position = match.end()  # Get the position after "SELECT"
                modified_query = query[:insert_position] + f" {comment} " + query[insert_position:]
                return modified_query
        except re.error as e:
            logging.warning(
                f"Regex Error searching first select while adding comments to query: {e}"
            )

        return query
    else:
        return query


def strip_comment(query: str, item: str) -> tuple:
    """
    Strip a comment pattern like `/* item::UUID */` from the query.

    Args:
        query (str): The SQL query to process.
        item (str): The dynamic keyword to search for (e.g., "condanest").

    Returns:
        tuple: (stripped_query, extracted_comment)
    """
    # Use a regex pattern to find comments like /* item::UUID */
    # logger.info("Stripping Comments!")
    try:
        comment_pattern = rf"/\*\s*{item}::[a-zA-Z0-9]+\s*\*/"

        # Search for the comment in the query
        match = re.search(comment_pattern, query)
        if match:
            extracted_comment = match.group(0)  # Capture the entire comment
            stripped_query = query.replace(
                extracted_comment, ""
            ).strip()  # Remove it from the query
            return stripped_query, extracted_comment
        return query, None

    except re.error as regex_err:
        logger.error(f"Invalid regex pattern with item='{item}': {regex_err}")
        return query, None
    except Exception as e:
        logger.error(f"Unexpected error during comment extraction: {e}")
        return query, None


def ensure_select_from_values(expression: exp.Expression) -> exp.Expression:
    """
    Ensures that any CTE using VALUES directly is modified to SELECT * FROM VALUES(...).
    """
    # logger.info("Ensuring select from values.....")
    for cte in expression.find_all(exp.CTE):
        cte_query = cte.this
        # Check if the CTE contains only a VALUES clause
        if isinstance(cte_query, exp.Values):
            # Transform VALUES() into SELECT * FROM VALUES()
            if cte_query.alias == "":
                cte_query.set("alias", '"values_subq"')

            new_query = exp.Select(expressions=[exp.Star()])
            new_query.set("from", exp.From(this=cte_query))

            cte.set("this", new_query)
    return expression


def extract_udfs(unsupported_list, from_dialect_func_list):
    # logger.info("Extracting UDFs from unsupported functions list.....")
    udf_list = set()
    remaining_unsupported = []
    for unsupported_function in unsupported_list:
        if unsupported_function not in from_dialect_func_list:
            udf_list.add(unsupported_function)
        else:
            remaining_unsupported.append(unsupported_function)
    return list(udf_list), remaining_unsupported


def load_supported_functions(dialect: str):
    """
    Load the supported SQL functions from a JSON file for a given dialect.
    The output will be a list or set of function names for that dialect.

    Args:
        dialect (str): The name of the SQL dialect (e.g., 'snowflake', 'databricks').

    Returns:
        set or list: A set or list of supported functions for the given dialect.
                      Returns an empty set/list if the dialect is not found.
    """
    if not os.path.exists(FUNCTIONS_FILE):
        logger.warning(f"Warning: {FUNCTIONS_FILE} not found. Returning an empty list/set.")
        return set()  # Return an empty set for non-existent file.

    try:
        with open(FUNCTIONS_FILE, "r") as file:
            json_data = json.load(file)

        # Check if the dialect exists in the data and return the corresponding functions
        if dialect in json_data:
            # If the dialect is present, return a set of functions for O(1) lookup
            return set(json_data[dialect])  # Convert the list to set if required.
        else:
            logger.warning(f"Warning: Dialect '{dialect}' not found in the function mapping.")
            return set()  # Return an empty set if dialect is not found.

    except json.JSONDecodeError:
        logger.error(
            f"Error in loading supported functions: {FUNCTIONS_FILE} contains invalid JSON."
        )
        return set()

    except Exception as e:
        logger.error(f"Unexpected error while loading functions: {e}")
        return set()


def extract_db_and_Table_names(sql_query_ast):
    # logger.info("Extracting database and table names....")
    tables_list = []
    if sql_query_ast:
        for table in sql_query_ast.find_all(exp.Table):
            if table.db:
                tables_list.append(f"{table.db}.{table.name}")
            else:
                tables_list.append(table.name)
        tables_list = list(set(tables_list))
        for alias in sql_query_ast.find_all(exp.TableAlias):
            if isinstance(alias.parent, exp.CTE) and alias.name in tables_list:
                tables_list.remove(alias.name)
    return tables_list


def extract_joins_from_query(sql_query_ast):
    """
    Extracts all join information from a SQL query AST.

    Args:
        sql_query_ast (exp.Expression): The parsed SQL AST.

    Returns:
        List[List]: A list of join structures in the format:
            [
                ["Base Table", ["Table1", "Join Type", "Side"], ["Table2", "Join Type", "Side"]],
                ...
            ]
    """
    # logger.info("Extracting joins from query.....")

    join_info_list = []
    joins_list = []

    try:
        for select in sql_query_ast.find_all(exp.Select):
            if not select.args.get("from"):
                continue

            from_statement = select.args.get("from")

            if isinstance(from_statement.this, (exp.Subquery, exp.CTE, exp.Values)):
                alias_columns = ", ".join(from_statement.this.alias_column_names)
                base_table = (
                    f"{from_statement.this.alias}({alias_columns})"
                    if alias_columns
                    else f"{from_statement.this.alias}"
                )

            else:
                base_table = from_statement.this
                base_table = (
                    f"{base_table.db}.{base_table.name}" if base_table.db else base_table.name
                )

            if select.args.get("joins"):
                joins_list.append([base_table])
                for join in select.args.get("joins"):
                    if isinstance(join.this, (exp.Subquery, exp.CTE, exp.Values, exp.Lateral)):
                        alias_columns = ", ".join(join.this.alias_column_names)
                        join_table = (
                            f"{join.this.alias}({alias_columns})"
                            if alias_columns
                            else f"{join.this.alias}"
                        )

                    else:
                        join_table = join.this
                        if isinstance(join_table, exp.Table):
                            join_table = (
                                f"{join_table.db}.{join_table.name}"
                                if join_table.db
                                else join_table.name
                            )
                    # join_table = f"{join.this.db}.{join.this.name}" if join.this.db else join.this.name
                    join_side = join.text("side").upper() or ""
                    join_type = join.text("kind").upper()

                    if not join_type:
                        join_type = "OUTER" if join_side else "CROSS"

                    if not join_side:
                        joins_list.append([join_table, join_type])
                    else:
                        joins_list.append([join_table, join_type, join_side])
                join_info_list.append(joins_list)
                joins_list = []

        join_info_list = list(map(list, {tuple(map(tuple, sublist)) for sublist in join_info_list}))
    except Exception as e:
        logger.error(f"Error in extracting joins from query {e}")

    return join_info_list


def set_cte_names_case_sensitively(sql_query_ast):
    [cte_list, values_list, subquery_list] = extract_cte_n_subquery_list(sql_query_ast)
    total_list = cte_list + values_list + subquery_list

    def compare_names_from_one_to_list(join_name: str, total_name_list: list):
        for cte in total_name_list:
            if cte.lower() == join_name.lower():
                return cte

    for table in sql_query_ast.find_all(exp.Table):
        cte_name = compare_names_from_one_to_list(table.name, total_list)
        if not table.db and cte_name is not None:
            table.this.set("this", cte_name)

    return sql_query_ast


def extract_cte_n_subquery_list(sql_query_ast):
    # logger.info("Extracting cte, subqueries and values....")
    cte_list = []
    subquery_list = []
    values_list = []
    try:
        for node in sql_query_ast.find_all(exp.CTE, exp.Subquery, exp.Values):
            if isinstance(node, exp.Values):
                columns_list = node.alias_column_names
                columns_alises_list = ", ".join(columns_list)
                if node.alias_or_name:
                    if len(columns_list) > 0:
                        values_list.append(f"{node.alias_or_name}({columns_alises_list})")
                    else:
                        values_list.append(f"{node.alias_or_name}")
            elif node.alias:
                if isinstance(node, exp.Subquery):
                    subquery_list.append(node.alias)
                elif isinstance(node, exp.CTE):
                    cte_list.append(node.alias)
    except Exception as e:
        logger.error(f"Error while Extracting cte, subqueries and values: {e}")

    cte_list = list(set(cte_list))
    subquery_list = list(set(subquery_list))
    values_list = list(set(values_list))
    return [cte_list, values_list, subquery_list]


def normalize_unicode_spaces(sql: str) -> str:
    """
    Normalize all Unicode whitespace/separator characters (and U+FFFD) to plain ASCII spaces,
    but do NOT touch anything inside single (') or double (") quoted literals.
    """
    out_chars = []
    in_quote = None  # None, or "'" or '"'
    i = 0
    length = len(sql)

    while i < length:
        ch = sql[i]

        # Are we entering or exiting a quoted literal?
        if in_quote:
            out_chars.append(ch)
            # For SQL, single quotes are escaped by doubling (''),
            # so only end the quote if it's a lone quote character.
            if ch == in_quote:
                if in_quote == "'" and i + 1 < length and sql[i + 1] == "'":
                    # It's an escaped '' inside a single-quoted literal: consume both
                    out_chars.append(sql[i + 1])
                    i += 1
                else:
                    # Closing the literal
                    in_quote = None
            # Otherwise, we stay inside the literal
        else:
            # Not currently in a quote: check for opening
            if ch in ("'", '"'):
                in_quote = ch
                out_chars.append(ch)
            else:
                # Normalize replacement-char
                if not isascii(ch):
                    out_chars.append(" ")
                else:
                    cat = unicodedata.category(ch)
                    if (cat in ("Zs", "Zl", "Zp")) or (ch.isspace() and ch not in "\r\n"):
                        out_chars.append(" ")
                    else:
                        out_chars.append(ch)
        i += 1

    return "".join(out_chars)


def transform_table_part(expression: exp.Expression) -> exp.Expression:
    for column_or_table in expression.find_all(exp.Column, exp.Table):
        db = column_or_table.args.get("db")
        catalog = column_or_table.args.get("catalog")
        if db and catalog:
            db_name = db.this
            catalog_name = catalog.this
            combined_catalog_db = f"{catalog_name}_{db_name}"

            column_or_table.set("db", exp.to_identifier(combined_catalog_db))
            column_or_table.set("catalog", None)

    return expression


def transform_catalog_schema_only(query: str, from_sql: str) -> str:
    """
    Transform only the catalog.schema part to catalog_schema in the query
    without performing full transpilation.

    Args:
        query (str): The SQL query to transform.
        from_sql (str): The source SQL dialect.

    Returns:
        str: The query with only catalog.schema transformed.
    """
    try:
        import re

        tree = sqlglot.parse_one(query, read=from_sql, error_level=None)

        replacements = []

        for table in tree.find_all(exp.Table):
            db = table.args.get("db")
            catalog = table.args.get("catalog")
            if db and catalog:
                db_name = db.this
                catalog_name = catalog.this
                table_name = table.name
                # Create regex pattern that matches the exact pattern with word boundaries
                pattern = (
                    rf"\b{re.escape(catalog_name)}\.{re.escape(db_name)}\.{re.escape(table_name)}\b"
                )
                replacement = f"{catalog_name}_{db_name}.{table_name}"
                replacements.append((pattern, replacement))

        # Find column references with catalog.schema
        for column in tree.find_all(exp.Column):
            db = column.args.get("db")
            catalog = column.args.get("catalog")
            if db and catalog:
                db_name = db.this
                catalog_name = catalog.this
                table_name = column.table
                column_name = column.name
                if table_name:
                    # Create regex pattern for full column reference
                    pattern = rf"\b{re.escape(catalog_name)}\.{re.escape(db_name)}\.{re.escape(table_name)}\.{re.escape(column_name)}\b"
                    replacement = f"{catalog_name}_{db_name}.{table_name}.{column_name}"
                    replacements.append((pattern, replacement))

        # Apply replacements to the original query string
        result = query
        for pattern, replacement in replacements:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        return result
    except Exception as e:
        logger.error(f"Error in transform_catalog_schema_only: {e}")
        raise


def parse_feature_flags_safe(feature_flags: Optional[str]) -> dict:
    """
    Parse JSON feature flags, return empty dict on error.

    Args:
        feature_flags: JSON string containing feature flags

    Returns:
        dict: Parsed feature flags or empty dict if None or parse error
    """
    if not feature_flags:
        return {}
    try:
        return json.loads(feature_flags)
    except json.JSONDecodeError:
        logger.warning("Failed to parse feature flags, using empty dict")
        return {}


def extract_and_categorize_functions(
    query: str,
    from_sql: str,
    to_sql: str
) -> tuple:
    """
    Extract and categorize functions from query.

    This is a convenience function that combines the common pattern of:
    - Loading supported functions for target dialect
    - Extracting functions from query using regex
    - Categorizing functions as supported/unsupported
    - Extracting UDFs by comparing with source dialect functions

    Args:
        query: SQL query string
        from_sql: Source SQL dialect
        to_sql: Target SQL dialect

    Returns:
        tuple: (supported, unsupported, udf_list, supported_functions_in_to_dialect)
            - supported: list of supported function names
            - unsupported: list of unsupported function names
            - udf_list: list of user-defined functions
            - supported_functions_in_to_dialect: dict of supported functions in target dialect
    """

    # Load supported functions for target dialect
    supported_functions_in_to_dialect = load_supported_functions(to_sql)
    keyword_pattern = get_keyword_pattern()

    # Extract all functions from query
    all_functions = extract_functions_from_query(
        query, FUNCTION_PATTERN, keyword_pattern, EXCLUSION_LIST
    )

    # Categorize as supported/unsupported
    supported, unsupported = categorize_functions(
        all_functions, supported_functions_in_to_dialect, FUNCTIONS_AS_KEYWORDS
    )

    # Extract UDFs by comparing with source dialect
    from_dialect_function_list = load_supported_functions(from_sql)
    udf_list, unsupported = extract_udfs(unsupported, from_dialect_function_list)

    return supported, unsupported, udf_list, supported_functions_in_to_dialect


def preprocess_query(query: str, item: str = "condenast") -> tuple:
    """
    Preprocess query: normalize unicode spaces and strip comments.

    Args:
        query: SQL query string
        item: Item identifier for comment stripping

    Returns:
        tuple: (processed_query, comment)
    """
    query = normalize_unicode_spaces(query)
    query, comment = strip_comment(query, item)
    return query, comment


def transpile_with_transforms(
    query: str,
    from_sql: str,
    to_sql: str,
    flags_dict: dict
) -> str:
    """
    Complete transpilation pipeline with all transformations.

    This function encapsulates the core transpilation logic:
    - Parse query
    - Apply two-phase qualification (if enabled)
    - Quote identifiers
    - Ensure SELECT FROM VALUES
    - Set CTE names case-sensitively
    - Generate SQL

    Args:
        query: SQL query string
        from_sql: Source SQL dialect
        to_sql: Target SQL dialect
        flags_dict: Feature flags dictionary
        apply_two_phase: Whether to apply two-phase qualification

    Returns:
        str: Transpiled SQL string
    """
    tree = sqlglot.parse_one(query, read=from_sql, error_level=None)

    tree2 = quote_identifiers(tree, dialect=to_sql)
    values_ensured_ast = ensure_select_from_values(tree2)
    cte_names_equivalence_checked_ast = set_cte_names_case_sensitively(values_ensured_ast)

    return cte_names_equivalence_checked_ast.sql(
        dialect=to_sql,
        from_dialect=from_sql,
        pretty=flags_dict.get("PRETTY_PRINT", True)
    )


def postprocess_query(query: str, comment: str = "") -> str:
    """
    Post-process transpiled query.

    Applies final transformations:
    - Replace STRUCT patterns
    - Add comment back

    Args:
        query: Transpiled SQL query string
        comment: Comment to add back to query

    Returns:
        str: Post-processed query
    """
    query = replace_struct_in_query(query)
    query = add_comment_to_query(query, comment)
    return query
