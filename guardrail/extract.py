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
        for node in expr.walk():
            if isinstance(node, Select):
                from_clause = node.args.get("from")
                if from_clause:
                    for source in from_clause.find_all(Table):
                        table_name = source.name
                        if not table_name:
                            continue

                        alias = source.alias if source.alias else (
                            source.parent.alias if isinstance(source.parent, Alias) else None
                        )
                        if alias:
                            alias_mapping[alias] = table_name

                for join in node.find_all(Join):
                    joined_table = join.this
                    if isinstance(joined_table, Table):
                        jtable = joined_table.name
                        if jtable:
                            alias = joined_table.alias if joined_table.alias else (
                                joined_table.parent.alias if isinstance(joined_table.parent, Alias) else None
                            )
                            if alias:
                                alias_mapping[alias] = jtable

    return alias_mapping


def extract_sql_components_per_table_with_alias(
    expressions: List[Expression],
) -> List[Dict[str, Any]]:
    """
    Extracts SQL components (tables, columns, where_columns, limits, joins) from parsed SQL expressions,
    including handling for wildcards (*) and aliased columns.

    Args:
        expressions (List[Expression]): Parsed SQL expressions from sqlglot.parse().

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a table with its associated columns,
                              where_columns, limits, and joins.
    """
    components = []
    alias_mapping = {}

    def get_or_create_table_entry(table_name: str) -> Dict[str, Any]:
        table_entry = next((item for item in components if item["table"] == table_name), None)
        if not table_entry:
            table_entry = {
                "table": table_name,
                "columns": [],
                "where_columns": [],
                "limits": [],
                "joins": []
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
                    cte_names.add(cte_name.lower())

    for expression in expressions:
        for node in expression.walk():
            if isinstance(node, Select):
                current_select_tables = set()

                from_clause = node.args.get("from")
                if from_clause:
                    for source in from_clause.find_all(Table):
                        table_name = source.name
                        if not table_name:
                            continue

                        alias = source.alias if source.alias else (
                            source.parent.alias if isinstance(source.parent, Alias) else None
                        )
                        if alias:
                            alias_mapping[alias] = table_name

                        current_select_tables.add(table_name)
                        get_or_create_table_entry(table_name)

                for join in node.find_all(Join):
                    joined_table = join.this
                    if isinstance(joined_table, Table):
                        table_name = joined_table.name
                        alias = joined_table.alias if joined_table.alias else (
                            joined_table.parent.alias if isinstance(joined_table.parent, Alias) else None
                        )
                        if alias:
                            alias_mapping[alias] = table_name

                        table_entry = get_or_create_table_entry(table_name)

                        on_clause = join.args.get("on")
                        if on_clause:
                            join_condition = on_clause.sql()
                            table_entry["joins"].append({
                                "joined_table": table_name,
                                "condition": join_condition
                            })

                for expr in node.expressions:
                    if isinstance(expr, Column):
                        column_name = expr.name
                        table_alias = expr.table
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = get_or_create_table_entry(actual_table)
                            if column_name:
                                table_entry["columns"].append(column_name)
                        else:
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = get_or_create_table_entry(table)
                                    if column_name:
                                        table_entry["columns"].append(column_name)

                    elif isinstance(expr, Star):
                        table_alias = expr.parent.alias_or_name if isinstance(expr.parent, Alias) else None
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = get_or_create_table_entry(actual_table)
                            if "*" not in table_entry["columns"]:
                                table_entry["columns"].append("*")
                        else:
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = get_or_create_table_entry(table)
                                    if "*" not in table_entry["columns"]:
                                        table_entry["columns"].append("*")

                    elif isinstance(expr, Alias):
                        if isinstance(expr.this, Column):
                            column_name = expr.this.name
                            table_alias = expr.this.table
                            if table_alias:
                                actual_table = alias_mapping.get(table_alias, table_alias)
                                table_entry = get_or_create_table_entry(actual_table)
                                if column_name:
                                    table_entry["columns"].append(column_name)
                            else:
                                if current_select_tables:
                                    for table in current_select_tables:
                                        table_entry = get_or_create_table_entry(table)
                                        if column_name:
                                            table_entry["columns"].append(column_name)

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
                        else:
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = get_or_create_table_entry(table)
                                    if column_name:
                                        table_entry["where_columns"].append(column_name)

                limit_clause = node.args.get("limit")
                if limit_clause:
                    for limit_value in limit_clause.find_all(Literal):
                        limit_num = limit_value.this
                        if current_select_tables:
                            for table in current_select_tables:
                                table_entry = get_or_create_table_entry(table)
                                table_entry["limits"].append(limit_num)

    for entry in components:
        entry["columns"] = list(set(entry["columns"]))
        entry["where_columns"] = list(set(entry["where_columns"]))
        entry["limits"] = list(set(entry["limits"]))
        entry["joins"] = list({frozenset(d.items()): d for d in entry["joins"]}.values())

    return components, [entry["table"] for entry in components]
