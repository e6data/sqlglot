from typing import List, Dict, Any
import sqlglot
from sqlglot.expressions import (
    Expression,
    Select,
    Column,
    Table,
    Where,
    Limit,
    From,
    Subquery,
    Alias,
    Identifier,
    Literal,
    With,
    CTE,
)
from sqlglot import parse_one


def extract_sql_components_per_table_with_alias(
    expressions: List[Expression],
) -> List[Dict[str, Any]]:
    components = []
    alias_mapping = {}

    def traverse(node: Expression):
        if isinstance(node, Select):
            # Extract FROM tables and their aliases
            from_clause = node.args.get("from")
            if from_clause:
                for source in from_clause.find_all(Table):
                    table_name = source.name
                    alias = None
                    if isinstance(source, Alias):
                        alias = source.alias
                        alias_mapping[alias] = table_name
                    elif source.args.get("alias"):
                        alias = source.alias
                        alias_mapping[alias] = table_name

                    if not table_name:
                        continue

                    # Check if table is already in components
                    table_entry = next(
                        (item for item in components if item["table"] == table_name), None
                    )
                    if not table_entry:
                        table_entry = {
                            "table": table_name,
                            "columns": [],
                            "where_columns": [],
                            "limits": [],
                        }
                        components.append(table_entry)

                    # Extract columns
                    for expr in node.expressions:
                        if isinstance(expr, Column):
                            column_name = expr.name
                            if column_name:
                                table_entry["columns"].append(column_name)
                        elif isinstance(expr, Alias):
                            if isinstance(expr.this, Column):
                                column_name = expr.this.name
                                if column_name:
                                    table_entry["columns"].append(column_name)

                    # Extract WHERE columns
                    where_clause = node.args.get("where")
                    if where_clause:
                        for condition in where_clause.find_all(Column):
                            column_name = condition.name
                            if column_name:
                                # Determine the actual table if column is prefixed with alias
                                if condition.table:
                                    actual_table = alias_mapping.get(
                                        condition.table, condition.table
                                    )
                                    table_entry = next(
                                        (
                                            item
                                            for item in components
                                            if item["table"] == actual_table
                                        ),
                                        None,
                                    )
                                    if table_entry:
                                        table_entry["where_columns"].append(column_name)
                                # else:
                                #     table_entry['where_columns'].append(column_name)

                    # Extract LIMIT
                    limit_clause = node.args.get("limit")
                    if limit_clause:
                        limit_value = limit_clause.this
                        if isinstance(limit_value, Literal):
                            table_entry["limits"].append(limit_value.this)

            # Handle subqueries in WITH clauses or elsewhere
            with_clause = node.args.get("with")
            if with_clause:
                for cte in with_clause.expressions:
                    traverse(cte.this)

        elif isinstance(node, CTE):
            traverse(node.this)

        elif isinstance(node, With):
            for expression in node.expressions:
                traverse(expression.this)

        # Recursively traverse all child nodes
        for child in node.args.values():
            if isinstance(child, Expression):
                traverse(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, Expression):
                        traverse(item)

    for expr in expressions:
        traverse(expr)

    # Post-process to remove duplicates within each table entry
    for entry in components:
        entry["columns"] = list(set(entry["columns"]))
        entry["where_columns"] = list(set(entry["where_columns"]))
        entry["limits"] = list(set(entry["limits"]))

    return components


# # Example Usage
# if __name__ == "__main__":
#     sql_query = """
#     WITH cte_final AS (
#         SELECT
#             id,
#             RAND() * 100 AS random_value,
#             DATEADD(DAY, SEQ8(), CURRENT_DATE()) AS date_generated,
#             ARRAY_AGG(SEQ8()) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS array_example,
#             ROW_NUMBER() OVER (ORDER BY random_value DESC) AS row_num,
#             HASH(CAST(random_value AS TEXT)) AS hash_value
#         FROM TABLE(GENERATOR(ROWCOUNT => 10))
#     ),
#     cte_transformed AS (
#         SELECT
#             id,
#             random_value,
#             date_generated,
#             CASE
#                 WHEN MOD(id, 2) = 0 THEN 'even'
#                 ELSE 'odd'
#             END AS id_parity,
#             IFF(random_value > 50, 'high', 'low') AS random_category,
#             TO_VARCHAR(date_generated, 'YYYY-MM-DD') AS formatted_date,
#             DATE_PART(WEEK, date_generated) AS week_of_year,
#             ARRAY_SIZE(array_example) AS array_size,
#             row_num,
#             hash_value
#         FROM cte_final
#     )
#     SELECT
#         id,
#         random_value,
#         id_parity,
#         random_category,
#         formatted_date,
#         week_of_year,
#         array_size,
#         row_num,
#         hash_value,
#         OBJECT_AGG(random_category, random_value) OVER () AS aggregated_object
#     FROM cte_transformed
#     WHERE random_value > 20
#     ORDER BY random_value DESC
#     LIMIT 10
#     """

#     # Parse the SQL query
#     parsed = sqlglot.parse(sql_query, read='snowflake', error_level=None)

#     # Extract components per table
#     components = extract_sql_components_per_table(parsed)

#     # Display the result
#     from pprint import pprint
#     pprint(components)
if __name__ == "__main__":
    sql_query = """
    WITH sales_cte AS (
        SELECT
            s.sale_id,
            s.customer_id,
            s.product_id,
            s.sale_date,
            s.quantity,
            s.total_amount,
            ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.sale_date DESC) AS rn
        FROM sales s
        WHERE s.sale_date >= '2023-01-01'
    ),
    customer_cte AS (
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.signup_date,
            CASE
                WHEN c.status = 'active' THEN 1
                ELSE 0
            END AS is_active
        FROM customers c
        WHERE c.signup_date <= '2023-12-31'
    ),
    product_cte AS (
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.price,
            p.stock_quantity
        FROM products p
        WHERE p.discontinued = FALSE
    ),
    combined_data AS (
        SELECT
            sc.sale_id,
            sc.customer_id,
            sc.product_id,
            sc.sale_date,
            sc.quantity,
            sc.total_amount,
            sc.rn,
            cc.first_name,
            cc.last_name,
            cc.email,
            cc.is_active,
            pc.product_name,
            pc.category,
            pc.price
        FROM sales_cte sc
        INNER JOIN customer_cte cc ON sc.customer_id = cc.customer_id
        INNER JOIN product_cte pc ON sc.product_id = pc.product_id
        WHERE pc.stock_quantity > 0
    ),
    aggregated_sales AS (
        SELECT
            cd.customer_id,
            cd.first_name,
            cd.last_name,
            COUNT(cd.sale_id) AS total_purchases,
            SUM(cd.total_amount) AS total_spent,
            AVG(cd.total_amount) AS average_purchase
        FROM combined_data cd
        WHERE cd.is_active = 1
        GROUP BY cd.customer_id, cd.first_name, cd.last_name
    )
    SELECT
        asales.customer_id,
        asales.first_name,
        asales.last_name,
        asales.total_purchases,
        asales.total_spent,
        asales.average_purchase,
        cd.product_id,
        cd.product_name,
        cd.category,
        cd.price,
        cd.quantity,
        cd.sale_date
    FROM aggregated_sales asales
    LEFT JOIN combined_data cd ON asales.customer_id = cd.customer_id
    WHERE asales.total_spent > 1000
    ORDER BY asales.total_spent DESC
    LIMIT 50;
    """

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
    parsed = sqlglot.parse(sql, error_level=None)
    print(parsed, "\n\n")

    # Extract components per table with alias handling
    components = extract_sql_components_per_table_with_alias(parsed)

    # Display the result
    from pprint import pprint

    for c in components:
        pprint(
            c,
        )
        print("\n")
    # pprint(components)
