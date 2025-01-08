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
)
from sqlglot import parse_one
from collections import defaultdict
import logging

# Configure logging for debugging purposes
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def extract_sql_components_per_table_with_alias(
    expressions: List[Expression],
) -> List[Dict[str, Any]]:
    """
    Extracts SQL components (tables, columns, where_columns, limits) from parsed SQL expressions,
    associating LIMIT clauses with the specific tables involved in their respective SELECT statements.

    Args:
        expressions (List[Expression]): Parsed SQL expressions from sqlglot.parse().

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a table with its associated columns,
                              where_columns, and limits.
    """
    components = []
    alias_mapping = {}

    # Helper function to find or create a table entry
    def get_or_create_table_entry(table_name: str) -> Dict[str, Any]:
        table_entry = next((item for item in components if item["table"] == table_name), None)
        if not table_entry:
            table_entry = {"table": table_name, "columns": [], "where_columns": [], "limits": []}
            components.append(table_entry)
        return table_entry

    # Iterate over all expressions (statements)
    for expr in expressions:
        # Iterate over all Select nodes
        for select_node in expr.find_all(Select):
            # Determine if this Select is part of a CTE
            is_cte = False
            parent = select_node.parent
            while parent:
                if isinstance(parent, CTE):
                    is_cte = True
                    break
                parent = parent.parent
            # Only process LIMIT for main Select nodes (not in CTEs)
            # Alternatively, process all Select nodes and associate limits accordingly
            # But user wants 'LIMIT' associated with tables
            current_select_tables = set()

            # Extract tables from FROM clause
            from_clause = select_node.args.get("from")
            if from_clause:
                for table in from_clause.find_all(Table):
                    table_name = table.name
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

            # Extract columns
            for expr_col in select_node.expressions:
                if isinstance(expr_col, Column):
                    column_name = expr_col.name
                    table_alias = expr_col.table
                    if table_alias:
                        actual_table = alias_mapping.get(table_alias, table_alias)
                        table_entry = next(
                            (item for item in components if item["table"] == actual_table), None
                        )
                        if table_entry and column_name:
                            table_entry["columns"].append(column_name)
                        else:
                            logger.warning(
                                f"Column '{column_name}' has alias '{table_alias}' which does not match any table."
                            )
                    else:
                        if current_select_tables:
                            for table in current_select_tables:
                                table_entry = next(
                                    (item for item in components if item["table"] == table), None
                                )
                                if table_entry and column_name:
                                    table_entry["columns"].append(column_name)
                        else:
                            logger.warning(
                                f"Column '{column_name}' has no table alias and no tables found in SELECT."
                            )

                elif isinstance(expr_col, Alias):
                    if isinstance(expr_col.this, Column):
                        column_name = expr_col.this.name
                        table_alias = expr_col.this.table
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = next(
                                (item for item in components if item["table"] == actual_table), None
                            )
                            if table_entry and column_name:
                                table_entry["columns"].append(column_name)
                            else:
                                logger.warning(
                                    f"Aliased column '{column_name}' has alias '{table_alias}' which does not match any table."
                                )
                        else:
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = next(
                                        (item for item in components if item["table"] == table),
                                        None,
                                    )
                                    if table_entry and column_name:
                                        table_entry["columns"].append(column_name)
                            else:
                                logger.warning(
                                    f"Aliased column '{column_name}' has no table alias and no tables found in SELECT."
                                )
                    else:
                        # Handle expressions or functions aliased as columns
                        pass  # Can be extended if needed

            # Extract WHERE columns
            where_clause = select_node.args.get("where")
            if where_clause:
                for condition in where_clause.find_all(Column):
                    column_name = condition.name
                    table_alias = condition.table
                    if table_alias:
                        actual_table = alias_mapping.get(table_alias, table_alias)
                        table_entry = next(
                            (item for item in components if item["table"] == actual_table), None
                        )
                        if table_entry and column_name:
                            table_entry["where_columns"].append(column_name)
                        else:
                            logger.warning(
                                f"WHERE condition column '{column_name}' has alias '{table_alias}' which does not match any table."
                            )
                    else:
                        if current_select_tables:
                            for table in current_select_tables:
                                table_entry = next(
                                    (item for item in components if item["table"] == table), None
                                )
                                if table_entry and column_name:
                                    table_entry["where_columns"].append(column_name)
                        else:
                            logger.warning(
                                f"WHERE condition column '{column_name}' has no table alias and no tables found in SELECT."
                            )

            # Extract LIMIT only for main Select node
            limit_node = select_node.args.get("limit")
            if limit_node:
                limit_value = limit_node.this
                if isinstance(limit_value, Literal):
                    print("limti value ", limit_value)
                    limit_num = limit_value.this
                    # Only assign LIMIT to main Select node's tables, not CTEs
                    if not is_cte:
                        logger.debug(
                            f"Assigning LIMIT {limit_num} to tables: {current_select_tables}"
                        )
                        for table in current_select_tables:
                            table_entry = next(
                                (item for item in components if item["table"] == table), None
                            )
                            if table_entry:
                                table_entry["limits"].append(limit_num)
                else:
                    logger.warning(f"LIMIT value is not a Literal: {limit_value}")

    # Remove duplicates
    for entry in components:
        entry["columns"] = sorted(list(set(entry["columns"])))
        entry["where_columns"] = sorted(list(set(entry["where_columns"])))
        entry["limits"] = sorted(list(set(entry["limits"])))

    return components


if __name__ == "__main__":
    sql = """
WITH RECURSIVE EmployeeHierarchy AS (
    -- Base case: top-level managers
    SELECT 
        e.employee_id,
        e.full_name,
        e.manager_id,
        e.department_id,
        1 as level,
        CAST(e.full_name AS VARCHAR(1000)) as hierarchy_path
    FROM employees e
    WHERE e.manager_id IS NULL

    UNION ALL
    
    -- Recursive case: employees with managers
    SELECT 
        e.employee_id,
        e.full_name,
        e.manager_id,
        e.department_id,
        eh.level + 1,
        CAST(eh.hierarchy_path || ' -> ' || e.full_name AS VARCHAR(1000))
    FROM employees e
    INNER JOIN EmployeeHierarchy eh ON e.manager_id = eh.employee_id
),

DepartmentMetrics AS (
    SELECT 
        d.department_id,
        d.department_name,
        COUNT(DISTINCT e.employee_id) as employee_count,
        AVG(e.salary) as avg_salary,
        SUM(p.total_cost) as total_project_cost,
        DENSE_RANK() OVER (ORDER BY AVG(e.salary) DESC) as salary_rank
    FROM departments d
    LEFT JOIN employees e ON d.department_id = e.department_id
    LEFT JOIN project_assignments pa ON e.employee_id = pa.employee_id
    LEFT JOIN projects p ON pa.project_id = p.project_id
    WHERE d.active_status = 1
    GROUP BY d.department_id, d.department_name
),

ProjectPerformance AS (
    SELECT 
        p.project_id,
        p.project_name,
        p.start_date,
        p.end_date,
        COUNT(DISTINCT pa.employee_id) as team_size,
        SUM(p.total_cost) as project_cost,
        CASE 
            WHEN p.end_date < CURRENT_DATE THEN 'Completed'
            WHEN p.start_date > CURRENT_DATE THEN 'Not Started'
            ELSE 'In Progress'
        END as project_status,
        LAG(p.total_cost) OVER (PARTITION BY p.department_id ORDER BY p.start_date) as previous_project_cost
    FROM projects p
    LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
    GROUP BY p.project_id, p.project_name, p.start_date, p.end_date, p.department_id
)

SELECT 
    eh.hierarchy_path,
    eh.level as organization_depth,
    dm.department_name,
    dm.employee_count,
    ROUND(dm.avg_salary, 2) as average_salary,
    dm.salary_rank as department_salary_rank,
    pp.project_name,
    pp.team_size,
    pp.project_status,
    ROUND(pp.project_cost, 2) as current_project_cost,
    ROUND(pp.previous_project_cost, 2) as previous_project_cost,
    ROUND((pp.project_cost - COALESCE(pp.previous_project_cost, 0)) / 
          NULLIF(pp.previous_project_cost, 0) * 100, 2) as cost_change_percentage,
    FIRST_VALUE(pp.project_name) OVER (
        PARTITION BY dm.department_id 
        ORDER BY pp.project_cost DESC
    ) as most_expensive_project,
    COUNT(*) OVER (
        PARTITION BY eh.department_id
    ) as total_department_projects
FROM EmployeeHierarchy eh
INNER JOIN DepartmentMetrics dm ON eh.department_id = dm.department_id
LEFT JOIN ProjectPerformance pp ON eh.department_id = pp.department_id
WHERE 
    eh.level <= 3
    AND dm.employee_count >= 5
    AND dm.total_project_cost > (
        SELECT AVG(total_project_cost) * 1.2
        FROM DepartmentMetrics
    )
    AND EXISTS (
        SELECT 1
        FROM project_assignments pa
        WHERE pa.employee_id = eh.employee_id
        AND pa.end_date > CURRENT_DATE
    )
ORDER BY 
    eh.hierarchy_path,
    dm.salary_rank,
    pp.project_cost DESC
LIMIT 100;
    """

    # Parse the SQL query
    parsed = sqlglot.parse(sql, read="snowflake", error_level=None)

    # Extract components per table with alias handling
    components = extract_sql_components_per_table_with_alias(parsed)

    # Display the result
    from pprint import pprint

    for c in components:
        pprint(c)
        print("\n")
