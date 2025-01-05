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
    Order,
    Ordered,
    And,
    Or,
    EQ,
    GT,
    LTE,
    GTE,
    Paren,
    Subquery,
    Exists,
    Window,
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
    cte_names = set()

    # Helper function to find or create a table entry
    def get_or_create_table_entry(table_name: str) -> Dict[str, Any]:
        # Use case-insensitive comparison for table names
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

    # Recursive function to process SELECT nodes
    def process_select(select_node: Select, parent_alias_mapping: Dict[str, str]):
        """
        Processes a SELECT node, extracts tables, columns, where_columns, and limits.

        Args:
            select_node (Select): The SELECT node to process.
            parent_alias_mapping (Dict[str, str]): The alias mapping from the parent scope.
        """
        # Create a new alias mapping for the current scope, starting with parent mappings
        alias_mapping = parent_alias_mapping.copy()

        # Collect tables from FROM clause
        current_select_tables = set()
        from_clause = select_node.args.get('from')
        if from_clause:
            for table in from_clause.find_all(Table):
                # Extract table name correctly
                table_name = table.this.name if table.this and hasattr(table.this, 'name') else None
                if not table_name:
                    continue

                # Skip if the table is a CTE
                if table_name.lower() in cte_names:
                    continue

                # Extract alias if present
                alias = None
                if isinstance(table.parent, Alias):
                    alias = table.parent.alias
                elif table.alias:
                    alias = table.alias

                if alias:
                    alias_mapping[alias] = table_name

                if table_name:
                    current_select_tables.add(table_name)
                    get_or_create_table_entry(table_name)

        # Collect tables from JOIN clauses
        for join in select_node.find_all(Join):
            joined_table = join.this
            if isinstance(joined_table, Table):
                # Extract table name correctly
                table_name = joined_table.this.name if joined_table.this and hasattr(joined_table.this, 'name') else None
                if not table_name:
                    continue

                # Skip if the table is a CTE
                if table_name.lower() in cte_names:
                    continue

                # Extract alias if present
                alias = None
                if isinstance(joined_table.parent, Alias):
                    alias = joined_table.parent.alias
                elif joined_table.alias:
                    alias = joined_table.alias

                if alias:
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
                            logger.warning(f"Column '{column_name}' has alias '{table_alias}' which does not match any table.")
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
                    # Handle wildcard '*' and 'table_alias.*'
                    table_alias = None
                    if isinstance(node.parent, Table):
                        # Case: SELECT table_alias.*
                        table_alias = node.parent.alias_or_name
                    elif isinstance(node.parent, Alias):
                        # Case: SELECT table_alias.* AS alias
                        table_alias = node.parent.alias_or_name
                    elif hasattr(node.parent, 'alias_or_name'):
                        # General case: check if parent has alias_or_name
                        table_alias = node.parent.alias_or_name

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

        # Process nested SELECTs (e.g., subqueries in WHERE)
        for subquery in select_node.find_all(Subquery):
            # The subquery has its own SELECT node
            sub_select = subquery.this
            if isinstance(sub_select, Select):
                process_select(sub_select, alias_mapping)

        # Process nested SELECTs in EXISTS or other constructs
        for exists in select_node.find_all(Exists):
            exists_subquery = exists.this
            if isinstance(exists_subquery, Select):
                process_select(exists_subquery, alias_mapping)

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
        # Iterate over all top-level Select nodes
        for select_node in expr.find_all(Select):
            # Determine if this SELECT is part of a CTE
            is_cte = False
            parent = select_node.parent
            while parent:
                if isinstance(parent, CTE):
                    is_cte = True
                    break
                parent = parent.parent

            # If it's a CTE's SELECT, process it to extract base tables but don't add the CTE itself
            if is_cte:
                # Initialize an empty alias mapping for CTE's internal scope
                process_select(select_node, parent_alias_mapping={})
            else:
                # For main SELECT, initialize with an empty alias mapping
                process_select(select_node, parent_alias_mapping={})

    # Post-process to remove duplicates within each table entry
    for entry in components:
        entry['columns'] = sorted(list(set(entry['columns'])))
        entry['where_columns'] = sorted(list(set(entry['where_columns'])))
        entry['limits'] = sorted(list(set(entry['limits'])))

    return components


if __name__ == "__main__":
    sql = """
    SELECT 
        e.employee_id,
        e.full_name,
        e.salary,
        e.department_id
    FROM employees e
    WHERE e.salary > (
        SELECT AVG(e2.salaries)
        FROM employees e2
        WHERE e2.department_id = e.department_id
    );
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
