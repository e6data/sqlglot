from sqlglot import parse_one, exp, parse
from sqlglot.generator import Generator

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


def test_preprocess():
    # Create a test expression
    # sql = "SELECT id, name FROM users WHERE age > 18"
    gen = Generator()
    expression = parse(sql, dialect="snowflake", error_level=None)
    for e in expression:
        print("---------------***----------------")
        print("---------------***----------------")
        print(f"Original expression: {expression}")
        print("\n", "-" * 5)

        processed_expr = gen.preprocess(e)
        print(f"Processed expression: {processed_expr}")
        print("\n\n")
        for p in processed_expr:
            print(f"Processed expression: {p}")
            print("\n\n")

    # Generate SQL from processed expression
    # result = gen.generate(processed_expr)

    # print(f"Original SQL: {sql}")
    # print(f"Processed SQL: {result}")


if __name__ == "__main__":
    test_preprocess()
