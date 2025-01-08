# from typing import List, Dict, Any
# import sqlglot
# from sqlglot.expressions import (
#     Expression,
#     Select,
#     Column,
#     Table,
#     Where,
#     Limit,
#     From,
#     Subquery,
#     Alias,
#     Identifier,
#     Literal,
#     With,
#     CTE,
#     Join,
# )
# from sqlglot import parse_one

# def extract_sql_components_per_table_with_alias(expressions: List[Expression]) -> List[Dict[str, Any]]:
#     components = []
#     alias_mapping = {}

#     def traverse(node: Expression, current_select_tables: List[str] = None):
#         if current_select_tables is None:
#             current_select_tables = []

#         if isinstance(node, Select):
#             # Extract FROM tables and their aliases
#             from_clause = node.args.get('from')
#             if from_clause:
#                 for source in from_clause.find_all(Table):
#                     table_name = source.name
#                     alias = None
#                     # Check if the table has an alias
#                     if isinstance(source.parent, Alias):
#                         alias = source.parent.alias
#                         alias_mapping[alias] = table_name
#                     elif source.alias:
#                         alias = source.alias
#                         alias_mapping[alias] = table_name

#                     if not table_name:
#                         continue

#                     # Add table to the current SELECT's table list
#                     current_select_tables.append(table_name)

#                     # Ensure table entry exists
#                     table_entry = next((item for item in components if item['table'] == table_name), None)
#                     if not table_entry:
#                         table_entry = {
#                             'table': table_name,
#                             'columns': [],
#                             'where_columns': [],
#                             'limits': []
#                         }
#                         components.append(table_entry)

#             # Handle JOIN tables
#             for join in node.find_all(Join):
#                 joined_table = join.this
#                 if isinstance(joined_table, Table):
#                     table_name = joined_table.name
#                     alias = None
#                     # Check if the joined table has an alias
#                     if isinstance(joined_table.parent, Alias):
#                         alias = joined_table.parent.alias
#                         alias_mapping[alias] = table_name
#                     elif joined_table.alias:
#                         alias = joined_table.alias
#                         alias_mapping[alias] = table_name

#                     if table_name:
#                         current_select_tables.append(table_name)
#                         # Ensure table entry exists
#                         table_entry = next((item for item in components if item['table'] == table_name), None)
#                         if not table_entry:
#                             table_entry = {
#                                 'table': table_name,
#                                 'columns': [],
#                                 'where_columns': [],
#                                 'limits': []
#                             }
#                             components.append(table_entry)

#             # Extract columns from SELECT expressions
#             for expr in node.expressions:
#                 if isinstance(expr, Column):
#                     column_name = expr.name
#                     table_alias = expr.table
#                     if table_alias:
#                         actual_table = alias_mapping.get(table_alias, table_alias)
#                         table_entry = next((item for item in components if item['table'] == actual_table), None)
#                         if table_entry and column_name:
#                             table_entry['columns'].append(column_name)
#                     else:
#                         # If no table alias, associate with all tables in the current SELECT
#                         for table in current_select_tables:
#                             table_entry = next((item for item in components if item['table'] == table), None)
#                             if table_entry and column_name:
#                                 table_entry['columns'].append(column_name)
#                 elif isinstance(expr, Alias):
#                     # Handle aliased columns or expressions
#                     if isinstance(expr.this, Column):
#                         column_name = expr.this.name
#                         table_alias = expr.this.table
#                         if table_alias:
#                             actual_table = alias_mapping.get(table_alias, table_alias)
#                             table_entry = next((item for item in components if item['table'] == actual_table), None)
#                             if table_entry and column_name:
#                                 table_entry['columns'].append(column_name)
#                         else:
#                             # If no table alias, associate with all tables in the current SELECT
#                             for table in current_select_tables:
#                                 table_entry = next((item for item in components if item['table'] == table), None)
#                                 if table_entry and column_name:
#                                     table_entry['columns'].append(column_name)
#                     else:
#                         # Handle expressions or functions aliased as columns
#                         pass  # Can be extended if needed

#             # Extract WHERE columns
#             where_clause = node.args.get('where')
#             if where_clause:
#                 for condition in where_clause.find_all(Column):
#                     column_name = condition.name
#                     table_alias = condition.table
#                     if table_alias:
#                         actual_table = alias_mapping.get(table_alias, table_alias)
#                         table_entry = next((item for item in components if item['table'] == actual_table), None)
#                         if table_entry and column_name:
#                             table_entry['where_columns'].append(column_name)
#                     else:
#                         # If no table alias, associate with all tables in the current SELECT
#                         for table in current_select_tables:
#                             table_entry = next((item for item in components if item['table'] == table), None)
#                             if table_entry and column_name:
#                                 table_entry['where_columns'].append(column_name)

#             # Extract LIMIT
#             limit_clause = node.args.get('limit')
#             if limit_clause:
#                 limit_value = limit_clause.this
#                 if isinstance(limit_value, Literal):
#                     limit_num = limit_value.this
#                     for table in current_select_tables:
#                         table_entry = next((item for item in components if item['table'] == table), None)
#                         if table_entry:
#                             table_entry['limits'].append(limit_num)

#             # Handle subqueries in WITH clauses or elsewhere
#             with_clause = node.args.get('with')
#             if with_clause:
#                 for cte in with_clause.expressions:
#                     traverse(cte.this, current_select_tables.copy())

#         elif isinstance(node, CTE):
#             traverse(node.this, current_select_tables.copy())

#         elif isinstance(node, With):
#             for expression in node.expressions:
#                 traverse(expression.this, current_select_tables.copy())

#         # Recursively traverse all child nodes
#         for child in node.args.values():
#             if isinstance(child, Expression):
#                 traverse(child, current_select_tables.copy())
#             elif isinstance(child, list):
#                 for item in child:
#                     if isinstance(item, Expression):
#                         traverse(item, current_select_tables.copy())

#     for expr in expressions:
#         traverse(expr)

#     # Post-process to remove duplicates within each table entry
#     for entry in components:
#         entry['columns'] = list(set(entry['columns']))
#         entry['where_columns'] = list(set(entry['where_columns']))
#         entry['limits'] = list(set(entry['limits']))

#     return components

# if __name__ == "__main__":
#     sql = """
# WITH RECURSIVE EmployeeHierarchy AS (
#     -- Base case: top-level managers
#     SELECT
#         e.employee_id,
#         e.full_name,
#         e.manager_id,
#         e.department_id,
#         1 as level,
#         CAST(e.full_name AS VARCHAR(1000)) as hierarchy_path
#     FROM employees e
#     WHERE e.manager_id IS NULL

#     UNION ALL

#     -- Recursive case: employees with managers
#     SELECT
#         e.employee_id,
#         e.full_name,
#         e.manager_id,
#         e.department_id,
#         eh.level + 1,
#         CAST(eh.hierarchy_path || ' -> ' || e.full_name AS VARCHAR(1000))
#     FROM employees e
#     INNER JOIN EmployeeHierarchy eh ON e.manager_id = eh.employee_id
# ),

# DepartmentMetrics AS (
#     SELECT
#         d.department_id,
#         d.department_name,
#         COUNT(DISTINCT e.employee_id) as employee_count,
#         AVG(e.salary) as avg_salary,
#         SUM(p.total_cost) as total_project_cost,
#         DENSE_RANK() OVER (ORDER BY AVG(e.salary) DESC) as salary_rank
#     FROM departments d
#     LEFT JOIN employees e ON d.department_id = e.department_id
#     LEFT JOIN project_assignments pa ON e.employee_id = pa.employee_id
#     LEFT JOIN projects p ON pa.project_id = p.project_id
#     WHERE d.active_status = 1
#     GROUP BY d.department_id, d.department_name
# ),

# ProjectPerformance AS (
#     SELECT
#         p.project_id,
#         p.project_name,
#         p.start_date,
#         p.end_date,
#         COUNT(DISTINCT pa.employee_id) as team_size,
#         SUM(p.total_cost) as project_cost,
#         CASE
#             WHEN p.end_date < CURRENT_DATE THEN 'Completed'
#             WHEN p.start_date > CURRENT_DATE THEN 'Not Started'
#             ELSE 'In Progress'
#         END as project_status,
#         LAG(p.total_cost) OVER (PARTITION BY p.department_id ORDER BY p.start_date) as previous_project_cost
#     FROM projects p
#     LEFT JOIN project_assignments pa ON p.project_id = pa.project_id
#     GROUP BY p.project_id, p.project_name, p.start_date, p.end_date, p.department_id
# )

# SELECT
#     eh.hierarchy_path,
#     eh.level as organization_depth,
#     dm.department_name,
#     dm.employee_count,
#     ROUND(dm.avg_salary, 2) as average_salary,
#     dm.salary_rank as department_salary_rank,
#     pp.project_name,
#     pp.team_size,
#     pp.project_status,
#     ROUND(pp.project_cost, 2) as current_project_cost,
#     ROUND(pp.previous_project_cost, 2) as previous_project_cost,
#     ROUND((pp.project_cost - COALESCE(pp.previous_project_cost, 0)) /
#           NULLIF(pp.previous_project_cost, 0) * 100, 2) as cost_change_percentage,
#     FIRST_VALUE(pp.project_name) OVER (
#         PARTITION BY dm.department_id
#         ORDER BY pp.project_cost DESC
#     ) as most_expensive_project,
#     COUNT(*) OVER (
#         PARTITION BY eh.department_id
#     ) as total_department_projects
# FROM EmployeeHierarchy eh
# INNER JOIN DepartmentMetrics dm ON eh.department_id = dm.department_id
# LEFT JOIN ProjectPerformance pp ON eh.department_id = pp.department_id
# WHERE
#     eh.level <= 3
#     AND dm.employee_count >= 5
#     AND dm.total_project_cost > (
#         SELECT AVG(total_project_cost) * 1.2
#         FROM DepartmentMetrics
#     )
#     AND EXISTS (
#         SELECT 1
#         FROM project_assignments pa
#         WHERE pa.employee_id = eh.employee_id
#         AND pa.end_date > CURRENT_DATE
#     )
# ORDER BY
#     eh.hierarchy_path,
#     dm.salary_rank,
#     pp.project_cost DESC
# LIMIT 100;
#     """

#     # Parse the SQL query
#     parsed = sqlglot.parse(sql, read='snowflake', error_level=None)

#     # Extract components per table with alias handling
#     components = extract_sql_components_per_table_with_alias(parsed)

#     # Display the result
#     from pprint import pprint
#     for c in components:
#         pprint(c)
#         print("\n")


from typing import List, Dict, Any, Set, Optional
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
    Join,
    Binary,
    Func,
)


class TableContext:
    def __init__(self, name: str):
        self.name = name
        self.columns: Set[str] = set()
        self.where_columns: Set[str] = set()
        self.limits: Set[int] = set()
        self.referenced_in_joins: Set[str] = set()
        self.source_cte: Optional[str] = None


class SQLComponentExtractor:
    def __init__(self):
        self.contexts: Dict[str, TableContext] = {}
        self.alias_mapping: Dict[str, str] = {}
        self.cte_definitions: Dict[str, Set[str]] = {}

    def get_or_create_context(self, table_name: str) -> TableContext:
        if table_name not in self.contexts:
            self.contexts[table_name] = TableContext(table_name)
        return self.contexts[table_name]

    def resolve_table_name(self, alias: str) -> Optional[str]:
        return self.alias_mapping.get(alias, alias)

    def extract_column_references(self, expr: Expression, current_scope: List[str]):
        """Extract column references from expressions including functions and calculations"""
        if isinstance(expr, Column):
            table_alias = expr.table
            column_name = expr.name
            if table_alias:
                actual_table = self.resolve_table_name(table_alias)
                if actual_table:
                    context = self.get_or_create_context(actual_table)
                    context.columns.add(column_name)
            else:
                # Try to associate with tables in current scope
                for table in current_scope:
                    context = self.get_or_create_context(table)
                    context.columns.add(column_name)

        elif isinstance(expr, Func):
            # Handle function arguments
            for arg in expr.args.values():
                if isinstance(arg, (Expression, list)):
                    if isinstance(arg, list):
                        for item in arg:
                            self.extract_column_references(item, current_scope)
                    else:
                        self.extract_column_references(arg, current_scope)

        elif isinstance(expr, Binary):
            # Handle binary operations
            self.extract_column_references(expr.left, current_scope)
            self.extract_column_references(expr.right, current_scope)

    def process_where_condition(self, condition: Expression, current_scope: List[str]):
        """Process WHERE conditions including subqueries"""
        if isinstance(condition, Binary):
            self.process_where_condition(condition.left, current_scope)
            self.process_where_condition(condition.right, current_scope)

        elif isinstance(condition, Column):
            table_alias = condition.table
            column_name = condition.name
            if table_alias:
                actual_table = self.resolve_table_name(table_alias)
                if actual_table:
                    context = self.get_or_create_context(actual_table)
                    context.where_columns.add(column_name)
            else:
                # Only add to tables that are explicitly referenced in the current scope
                for table in current_scope:
                    context = self.get_or_create_context(table)
                    context.where_columns.add(column_name)

        elif isinstance(condition, Subquery):
            self.process_select(condition.this, [])  # Process subquery with empty scope

    def process_join(self, join: Join, current_scope: List[str]):
        """Process JOIN clauses and their conditions"""
        if isinstance(join.this, Table):
            table_name = join.this.name
            if join.this.alias:
                self.alias_mapping[join.this.alias] = table_name

            current_scope.append(table_name)
            context = self.get_or_create_context(table_name)

            # Process join conditions
            if join.on:
                self.process_where_condition(join.on, current_scope)

    def process_select(self, select: Select, parent_scope: List[str]) -> List[str]:
        """Process SELECT statement and return list of referenced tables"""
        current_scope = parent_scope.copy()

        # Process FROM clause
        if select.args.get("from"):
            for source in select.args["from"].find_all(Table):
                table_name = source.name
                if source.alias:
                    self.alias_mapping[source.alias] = table_name
                current_scope.append(table_name)

        # Process JOINs
        for join in select.find_all(Join):
            self.process_join(join, current_scope)

        # Process SELECT expressions
        for expr in select.expressions:
            if isinstance(expr, Alias):
                self.extract_column_references(expr.this, current_scope)
            else:
                self.extract_column_references(expr, current_scope)

        # Process WHERE clause
        where_clause = select.args.get("where")
        if where_clause:
            self.process_where_condition(where_clause, current_scope)

        # Process LIMIT
        limit_clause = select.args.get("limit")
        if limit_clause and isinstance(limit_clause.this, Literal):
            limit_value = limit_clause.this.this
            for table in current_scope:
                context = self.get_or_create_context(table)
                context.limits.add(limit_value)

        return current_scope

    def process_cte(self, cte: CTE):
        """Process CTE definition"""
        cte_name = cte.alias
        self.cte_definitions[cte_name] = set()

        if isinstance(cte.this, Select):
            referenced_tables = self.process_select(cte.this, [])
            self.cte_definitions[cte_name].update(referenced_tables)

    def extract_components(self, sql: str) -> List[Dict[str, Any]]:
        """Main method to extract SQL components"""
        parsed = sqlglot.parse_one(sql)
        print(parsed)
        print("\n")
        print("\n")
        print("\n")
        print("\n")
        # Process WITH clause first
        with_clause = parsed.args.get("with")
        if with_clause:
            for cte in with_clause.expressions:
                self.process_cte(cte)

        # Process main SELECT
        if isinstance(parsed, Select):
            self.process_select(parsed, [])

        # Convert to final format
        result = []
        for table_name, context in self.contexts.items():
            entry = {
                "table": table_name,
                "columns": sorted(list(context.columns)),
                "where_columns": sorted(list(context.where_columns)),
                "limits": sorted(list(context.limits)),
            }
            result.append(entry)

        return result


def analyze_sql_query(sql: str) -> List[Dict[str, Any]]:
    extractor = SQLComponentExtractor()
    return extractor.extract_components(sql)


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


if __name__ == "__main__":
    # Test with your SQL query
    components = analyze_sql_query(sql)
    from pprint import pprint

    for component in components:
        pprint(component)
        print("\n")
