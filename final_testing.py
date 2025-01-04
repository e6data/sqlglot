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
    Limit,
    Literal,
    Star,
)
from collections import defaultdict
import logging

# Configure logging for debugging purposes
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

def extract_sql_components_per_table_with_alias(expressions: List[Expression]) -> List[Dict[str, Any]]:
    """
    Extracts SQL components (tables, columns, where_columns, limits) from parsed SQL expressions,
    excluding derived tables and CTEs, while associating LIMIT clauses with the specific tables
    involved in their respective SELECT statements.

    Args:
        expressions (List[Expression]): Parsed SQL expressions from sqlglot.parse().

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a table with its associated columns,
                              where_columns, and limits.
    """
    components = []
    alias_mapping = {}
    cte_names = set()

    # Helper function to find or create a table entry
    def get_or_create_table_entry(table_name: str) -> Dict[str, Any]:
        table_entry = next((item for item in components if item['table'].lower() == table_name.lower()), None)
        if not table_entry:
            table_entry = {
                'table': table_name,
                'columns': [],
                'where_columns': [],
                'limits': []
            }
            components.append(table_entry)
        return table_entry

    # First Pass: Collect all CTE names
    for expr in expressions:
        with_clause = expr.args.get('with')
        if with_clause:
            for cte in with_clause.find_all(CTE):
                cte_name = cte.alias_or_name
                if cte_name:
                    cte_names.add(cte_name.lower())  # Use lowercase for consistent comparison

    # Second Pass: Traverse and extract tables, excluding CTEs and Derived Tables
    for expr in expressions:
        # Iterate over all Select nodes, including those in CTEs
        for select_node in expr.find_all(Select):
            # Determine if this SELECT is part of a CTE
            is_cte = False
            parent = select_node.parent
            while parent:
                if isinstance(parent, CTE):
                    is_cte = True
                    break
                parent = parent.parent

            # Collect current SELECT's tables and aliases
            current_select_tables = set()

            # Extract tables from FROM clause
            from_clause = select_node.args.get('from')
            if from_clause:
                for table in from_clause.find_all(Table):
                    table_name = table.name
                    if not table_name:
                        continue

                    # Skip if the table is a CTE
                    if table_name.lower() in cte_names:
                        continue

                    alias = None
                    if isinstance(table.parent, Alias):
                        alias = table.parent.alias
                        alias_mapping[alias] = table_name
                    elif table.alias:
                        alias = table.alias
                        alias_mapping[alias] = table_name

                    if table_name:
                        current_select_tables.add(table_name)
                        get_or_create_table_entry(table_name)

            # Extract tables from JOIN clauses
            for join in select_node.find_all(Join):
                joined_table = join.this
                if isinstance(joined_table, Table):
                    table_name = joined_table.name
                    if not table_name:
                        continue

                    # Skip if the table is a CTE
                    if table_name.lower() in cte_names:
                        continue

                    alias = None
                    if isinstance(joined_table.parent, Alias):
                        alias = joined_table.parent.alias
                        alias_mapping[alias] = table_name
                    elif joined_table.alias:
                        alias = joined_table.alias
                        alias_mapping[alias] = table_name

                    if table_name:
                        current_select_tables.add(table_name)
                        get_or_create_table_entry(table_name)

            # Extract columns from SELECT expressions
            for expr_col in select_node.expressions:
                # Perform a deep walk on each SELECT expression to find Columns and Stars
                for node in expr_col.walk():
                    if isinstance(node, Column):
                        column_name = node.name
                        table_alias = node.table
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = next((item for item in components if item['table'].lower() == actual_table.lower()), None)
                            if table_entry and column_name:
                                if column_name not in table_entry['columns']:
                                    table_entry['columns'].append(column_name)
                        else:
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = next((item for item in components if item['table'].lower() == table.lower()), None)
                                    if table_entry and column_name:
                                        if column_name not in table_entry['columns']:
                                            table_entry['columns'].append(column_name)
                            else:
                                logger.warning(f"Column '{column_name}' has no table alias and no tables found in SELECT.")

                    elif isinstance(node, Star):
                        # Handle wildcard '*'
                        # Check if the Star has a table alias (e.g., 'e.*')
                        table_alias = node.parent.alias_or_name if isinstance(node.parent, Alias) else None
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = next((item for item in components if item['table'].lower() == actual_table.lower()), None)
                            if table_entry:
                                if '*' not in table_entry['columns']:
                                    table_entry['columns'].append('*')
                        else:
                            # Unqualified '*', associate with all current SELECT tables
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = next((item for item in components if item['table'].lower() == table.lower()), None)
                                    if table_entry:
                                        if '*' not in table_entry['columns']:
                                            table_entry['columns'].append('*')
                            else:
                                logger.warning("Unqualified '*' found but no tables are associated with the current SELECT.")

            # Extract WHERE columns
            where_clause = select_node.args.get('where')
            if where_clause:
                for condition in where_clause.find_all(Column):
                    column_name = condition.name
                    table_alias = condition.table
                    if table_alias:
                        actual_table = alias_mapping.get(table_alias, table_alias)
                        table_entry = next((item for item in components if item['table'].lower() == actual_table.lower()), None)
                        if table_entry and column_name:
                            if column_name not in table_entry['where_columns']:
                                table_entry['where_columns'].append(column_name)
                        else:
                            logger.warning(f"WHERE condition column '{column_name}' has alias '{table_alias}' which does not match any table.")
                    else:
                        if current_select_tables:
                            for table in current_select_tables:
                                table_entry = next((item for item in components if item['table'].lower() == table.lower()), None)
                                if table_entry and column_name:
                                    if column_name not in table_entry['where_columns']:
                                        table_entry['where_columns'].append(column_name)
                        else:
                            logger.warning(f"WHERE condition column '{column_name}' has no table alias and no tables found in SELECT.")

            # Extract LIMIT clauses, associating with current SELECT's tables
            limit_node = select_node.args.get('limit')
            if limit_node:
                limit_value = limit_node.this
                if isinstance(limit_value, Literal):
                    try:
                        limit_num = int(limit_value.this)
                    except ValueError:
                        limit_num = limit_value.this  # Keep as is if not an integer
                    if current_select_tables:
                        for table in current_select_tables:
                            table_entry = next((item for item in components if item['table'].lower() == table.lower()), None)
                            if table_entry:
                                if limit_num not in table_entry['limits']:
                                    table_entry['limits'].append(limit_num)
                    else:
                        logger.warning(f"LIMIT '{limit_num}' found but no tables are associated with the current SELECT.")
                else:
                    logger.warning(f"LIMIT value is not a Literal: {limit_value}")

    # Post-process to remove duplicates within each table entry
    for entry in components:
        entry['columns'] = sorted(list(set(entry['columns'])))
        entry['where_columns'] = sorted(list(set(entry['where_columns'])))
        entry['limits'] = sorted(list(set(entry['limits'])))

    return components



if __name__ == "__main__":
    sql = """
    WITH RECURSIVE cte_final AS (
        SELECT
            e.id,
            e.random_value,
            2 as some_column
        FROM
            employees e
    )
    SELECT
        e.id,
        e.random_value,
        *,
        OBJECT_CONSTRUCT(*),
        JSON_OBJECT(*)
    FROM
        cte_final e
    WHERE
        e.random_value > 20
    LIMIT 10
    """

    # Parse the SQL query
    parsed = sqlglot.parse(sql, read='snowflake', error_level=None)

    # Extract components per table with alias handling
    components = extract_sql_components_per_table_with_alias(parsed)

    # Display the result
    from pprint import pprint
    for c in components:
        pprint(c)
        print("\n")
