from typing import List, Dict, Any, Optional
import sqlglot
from sqlglot.expressions import (
    Expression,
    Select,
    Column,
    Table,
    Alias,
    With,
    CTE,
    Join,
    Literal,
    Star,
)
from sqlglot import parse_one
from collections import defaultdict
import logging

# Configure logging for debugging purposes
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def build_alias_mapping(expressions: List[Expression]) -> Dict[str, str]:
    """
    First pass: Gathers all table aliases from all SELECT ... FROM ... clauses and JOINs.

    Returns:
        alias_mapping: a dict of alias -> real_table_name
    """
    alias_mapping = {}

    for expr in expressions:
        # Walk entire tree
        for node in expr.walk():
            if isinstance(node, Select):
                # FROM
                from_clause = node.args.get("from")
                if from_clause:
                    for source in from_clause.find_all(Table):
                        table_name = source.name
                        if not table_name:
                            continue

                        if isinstance(source.parent, Alias):
                            alias = source.parent.alias
                        else:
                            alias = source.alias
                        if alias:
                            alias_mapping[alias] = table_name

                # JOIN
                for join in node.find_all(Join):
                    joined_table = join.this
                    if isinstance(joined_table, Table):
                        jtable = joined_table.name
                        if jtable:
                            if isinstance(joined_table.parent, Alias):
                                jalias = joined_table.parent.alias
                            else:
                                jalias = joined_table.alias
                            if jalias:
                                alias_mapping[jalias] = jtable

    return alias_mapping


def extract_sql_components_per_table_with_alias(
    expressions: List[Expression],
) -> List[Dict[str, Any]]:
    """
    Extracts SQL components (tables, columns, where_columns, limits, joins) from parsed SQL expressions,
    associating LIMIT clauses and JOIN conditions with the specific tables involved in their respective SELECT statements.

    Args:
        expressions (List[Expression]): Parsed SQL expressions from sqlglot.parse().

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a table with its associated columns,
                              where_columns, limits, and joins.
    """
    components = []
    alias_mapping = {}  # Maps aliases to actual table names

    # Helper function to find or create a table entry
    def get_or_create_table_entry(table_name: str) -> Dict[str, Any]:
        table_entry = next((item for item in components if item["table"] == table_name), None)
        if not table_entry:
            table_entry = {
                "table": table_name,
                "columns": [],
                "where_columns": [],
                "limits": [],
                "joins": []  # Add joins to track join details
            }
            components.append(table_entry)
        return table_entry

    cte_names = set()

    for expr in expressions:
        with_clause = expr.args.get("with")
        if with_clause:
            for cte in with_clause.find_all(CTE):
                cte_name = cte.alias_or_name
                if cte_name:
                    cte_names.add(cte_name.lower())  # Use lowercase for consistent comparison

    # Traverse the AST using the walk method provided by Expression class
    for expression in expressions:
        for node in expression.walk():
            if isinstance(node, Select):
                current_select_tables = set()

                # Extract FROM tables and their aliases
                from_clause = node.args.get("from")
                if from_clause:
                    for source in from_clause.find_all(Table):
                        table_name = source.name
                        if not table_name:
                            continue

                        # Check if the table has an alias
                        if isinstance(source.parent, Alias):
                            alias = source.parent.alias
                        else:
                            alias = source.alias

                        if alias:
                            alias_mapping[alias] = table_name

                        current_select_tables.add(table_name)
                        get_or_create_table_entry(table_name)

                # Extract JOIN details
                for join in node.find_all(Join):
                    joined_table = join.this
                    if isinstance(joined_table, Table):
                        table_name = joined_table.name
                        alias = joined_table.alias if joined_table.alias else table_name

                        if alias:
                            alias_mapping[alias] = table_name

                        # Ensure the table is tracked
                        table_entry = get_or_create_table_entry(table_name)

                        # Capture join condition
                        on_clause = join.args.get("on")
                        if on_clause:
                            join_condition = on_clause.sql()
                            # Add join information to the table entry
                            table_entry["joins"].append({
                                "joined_table": table_name,
                                "condition": join_condition
                            })

                # Extract columns from SELECT expressions
                for expr in node.expressions:
                    if isinstance(expr, Column):
                        column_name = expr.name
                        table_alias = expr.table
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = next(
                                (item for item in components if item["table"] == actual_table), None
                            )
                            if table_entry and column_name:
                                table_entry["columns"].append(column_name)
                            else:
                                logger.warning(
                                    f"Column '{column_name}' has an alias '{table_alias}' which does not match any table."
                                )
                        else:
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = get_or_create_table_entry(table)
                                    if column_name:
                                        table_entry["columns"].append(column_name)

                # Extract WHERE columns
                where_clause = node.args.get("where")
                if where_clause:
                    for condition in where_clause.find_all(Column):
                        column_name = condition.name
                        table_alias = condition.table
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = get_or_create_table_entry(actual_table)
                            if column_name:
                                table_entry["where_columns"].append(column_name)

                # Extract LIMIT
                limit_clause = node.args.get("limit")
                if limit_clause:
                    for limit_value in limit_clause.find_all(Literal):
                        limit_num = limit_value.this
                        if current_select_tables:
                            for table in current_select_tables:
                                table_entry = get_or_create_table_entry(table)
                                table_entry["limits"].append(limit_num)

    # Post-process to remove duplicates within each table entry
    for entry in components:
        entry["columns"] = list(set(entry["columns"]))
        entry["where_columns"] = list(set(entry["where_columns"]))
        entry["limits"] = list(set(entry["limits"]))
        entry["joins"] = list({frozenset(d.items()): d for d in entry["joins"]}.values())

    return components, [entry["table"] for entry in components]
