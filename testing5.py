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


def ccextract_sql_components_per_table_with_alias(
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
    alias_mapping = {}  # Maps aliases to actual table names

    # Helper function to find or create a table entry
    def get_or_create_table_entry(table_name: str) -> Dict[str, Any]:
        table_entry = next((item for item in components if item["table"] == table_name), None)
        if not table_entry:
            table_entry = {"table": table_name, "columns": [], "where_columns": [], "limits": []}
            components.append(table_entry)
        return table_entry

    cte_names = set()

    for expr in expressions:
        with_clause = expr.args.get("with")
        if with_clause:
            for cte in with_clause.find_all(CTE):
                cte_name = cte.alias_or_name
                if cte_name:
                    cte_names.add(cte_name)  # Use lowercase for consistent comparison
    print("cte_names: ", cte_names)
    # Traverse the AST using the walk method provided by Expression class
    i = 0
    for expression in expressions:
        for node in expression.walk():
            i += 1
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

                for join in node.find_all(Join):
                    joined_table = join.this
                    if isinstance(joined_table, Table):
                        table_name = joined_table.name
                        if table_name:
                            # Check alias
                            if isinstance(joined_table.parent, Alias):
                                alias = joined_table.parent.alias
                            else:
                                alias = joined_table.alias

                            if alias:
                                alias_mapping[alias] = table_name

                            current_select_tables.add(table_name)
                            get_or_create_table_entry(table_name)

                alias_mapping = build_alias_mapping(expressions)

                # Handle JOIN tables
                for join in node.find_all(Join):
                    joined_table = join.this
                    if isinstance(joined_table, Table):
                        table_name = joined_table.name
                        alias = None
                        # Check if the joined table has an alias
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
                            # If no table alias, associate with all tables in the current SELECT (ambiguous)
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
                                    f"Column '{column_name}' has no table alias and no tables found in SELECT."
                                )
                    elif isinstance(expr, Star):
                        # Handle wildcard '*'
                        # Check if the Star has a table alias (e.g., 'e.*')
                        # print("expr.parent: ", expr)
                        table_alias = (
                            expr.parent.alias_or_name if isinstance(expr.parent, Alias) else None
                        )
                        if table_alias:
                            actual_table = alias_mapping.get(table_alias, table_alias)
                            table_entry = next(
                                (
                                    item
                                    for item in components
                                    if item["table"].lower() == actual_table.lower()
                                ),
                                None,
                            )
                            if table_entry:
                                if "*" not in table_entry["columns"]:
                                    table_entry["columns"].append("*")
                        else:
                            # Unqualified '*', associate with all current SELECT tables
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = next(
                                        (
                                            item
                                            for item in components
                                            if item["table"].lower() == table.lower()
                                        ),
                                        None,
                                    )
                                    if table_entry:
                                        if "*" not in table_entry["columns"]:
                                            table_entry["columns"].append("*")
                            else:
                                logger.warning(
                                    "Unqualified '*' found but no tables are associated with the current SELECT."
                                )

                    elif isinstance(expr, Alias):
                        # Handle aliased columns or expressions
                        if isinstance(expr.this, Column):
                            column_name = expr.this.name
                            table_alias = expr.this.table
                            if table_alias:
                                actual_table = alias_mapping.get(table_alias, table_alias)
                                table_entry = next(
                                    (item for item in components if item["table"] == actual_table),
                                    None,
                                )
                                # print("Table entry is ->: ",table_entry, "actual_table is ->: ",actual_table, "table_alias is ->: ",table_alias, "column_name is ->: ",column_name)
                                if table_entry and column_name:
                                    table_entry["columns"].append(column_name)
                                else:
                                    logger.warning(
                                        f"Aliased column '{column_name}' has an alias '{table_alias}' which does not match any table."
                                    )
                            else:
                                # If no table alias, associate with all tables in the current SELECT (ambiguous)
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
                            # print("values is ",table_entry['columns'])

                        elif isinstance(node, Star):
                            # Handle wildcard '*'
                            # Check if the Star has a table alias (e.g., 'e.*')
                            table_alias = (
                                node.parent.alias_or_name
                                if isinstance(node.parent, Alias)
                                else None
                            )
                            if table_alias:
                                actual_table = alias_mapping.get(table_alias, table_alias)
                                table_entry = next(
                                    (
                                        item
                                        for item in components
                                        if item["table"].lower() == actual_table.lower()
                                    ),
                                    None,
                                )
                                if table_entry:
                                    if "*" not in table_entry["columns"]:
                                        table_entry["columns"].append("*")
                            else:
                                # Unqualified '*', associate with all current SELECT tables
                                if current_select_tables:
                                    for table in current_select_tables:
                                        table_entry = next(
                                            (
                                                item
                                                for item in components
                                                if item["table"].lower() == table.lower()
                                            ),
                                            None,
                                        )
                                        if table_entry:
                                            if "*" not in table_entry["columns"]:
                                                table_entry["columns"].append("*")
                                else:
                                    logger.warning(
                                        "Unqualified '*' found but no tables are associated with the current SELECT."
                                    )

                        else:
                            # Handle expressions or functions aliased as columns
                            pass  # Can be extended if needed

                # Extract WHERE columns
                where_clause = node.args.get("where")
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
                                    f"WHERE condition column '{column_name}' has an alias '{table_alias}' which does not match any table."
                                )
                        else:
                            # If no table alias, associate with all tables in the current SELECT (ambiguous)
                            if current_select_tables:
                                for table in current_select_tables:
                                    table_entry = next(
                                        (item for item in components if item["table"] == table),
                                        None,
                                    )
                                    if table_entry and column_name:
                                        table_entry["where_columns"].append(column_name)
                            else:
                                logger.warning(
                                    f"WHERE condition column '{column_name}' has no table alias and no tables found in SELECT."
                                )

                # Extract LIMIT
                limit_clause = node.args.get("limit")
                if limit_clause:
                    # print("len is ",len(limit_clause))
                    for limit_value in limit_clause.find_all(Literal):
                        if isinstance(limit_value, Literal):
                            limit_num = limit_value.this
                            if current_select_tables:
                                print("limit current_select_tables: ", current_select_tables)
                                for table in current_select_tables:
                                    # print("limit table: ", table)
                                    table_entry = next(
                                        (item for item in components if item["table"] == table),
                                        None,
                                    )
                                    if table_entry:
                                        table_entry["limits"].append(limit_num)
                            else:
                                logger.warning(
                                    f"LIMIT '{limit_num}' found but no tables are associated with the current SELECT."
                                )

            elif isinstance(node, CTE):
                # CTEs are handled implicitly by traversing their SELECT statements
                pass  # Already handled via the walk

            elif isinstance(node, With):
                # WITH clauses are handled implicitly by traversing their CTEs
                pass  # Already handled via the walk

    # Post-process to remove duplicates within each table entry
    list_of_tables = []
    entries = []
    for entry in components:
        if entry["table"] in cte_names:
            print("found cte: ", entry["table"])
            continue
        list_of_tables.append(entry["table"])
        entries.append(entry)

    return entries


sql3 = """
SELECT 
    e.employee_id,
    e.full_name,
    e.salary,
    e.department_id
FROM employees e
WHERE e.salary > (
    SELECT salaries2
    FROM employees e2
    WHERE e2.salaries = e.department_id
    LIMIT 10
);

"""

sql2 = """
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
    LIMIT 10

    UNION ALL
    
    SELECT * from diddy WHERE diddy.id = 1

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
    parsed = sqlglot.parse(sql2, read="snowflake", error_level=None)

    # Extract components per table with alias handling
    # alias_mapping = build_alias_mapping(parsed)
    # print("alias_mapping: ", alias_mapping)
    components = ccextract_sql_components_per_table_with_alias(parsed)

    # Display the result
    from pprint import pprint

    for c in components:
        pprint(c)
        print("\n")
