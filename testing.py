from typing import Dict, List, Any
from sqlglot import parse_one, parse
from sqlglot import exp


def parse_sql(sql: str, dialect: str = "snowflake") -> List[Dict[str, Any]]:
    """
    Parses the given SQL string and returns the structured `infos` list.

    Args:
        sql (str): The SQL query string to parse.
        dialect (str): The SQL dialect to use for parsing (default is "snowflake").

    Returns:
        List[Dict[str, Any]]: The structured information extracted from the SQL query.
    """
    # Parse the SQL into a list of expressions (handles multiple statements)
    parsed = parse(sql, read=dialect)
    infos = []

    for statement in parsed:
        if isinstance(statement, exp.Select):
            infos.extend(parse_select(statement))
        else:
            # Handle other SQL statement types if necessary
            pass

    return infos


def parse_select(select_expr: exp.Select) -> List[Dict[str, Any]]:
    """
    Parses a SELECT expression and extracts the relevant information.

    Args:
        select_expr (exp.Select): The SELECT expression to parse.

    Returns:
        List[Dict[str, Any]]: The extracted information from the SELECT expression.
    """
    infos = []
    main_info = {"tables": [], "columns": [], "where_columns": [], "limits": []}

    # Extract tables from FROM clause
    from_expr = select_expr.args.get("from")
    if from_expr:
        for table in from_expr.find_all(exp.Table):
            main_info["tables"].append(table.name)

    # Extract columns from SELECT expressions
    for select_exp in select_expr.expressions:
        column = extract_column(select_exp)
        if column:
            main_info["columns"].append(column)

    # Extract WHERE clause
    where_expr = select_expr.args.get("where")
    if where_expr:
        main_info["where_columns"] = process_where(where_expr.this)

    # Extract LIMIT clause
    limit_expr = select_expr.args.get("limit")
    if limit_expr:
        main_info["limits"].append(limit_expr.sql())

    # Append main_info to infos
    infos.append(main_info)

    # Process JOINs
    joins = select_expr.args.get("joins", [])
    for join in joins:
        join_info = {"tables": [], "columns": [], "where_columns": [], "limits": []}
        join_table = join.this
        if isinstance(join_table, exp.Table):
            join_info["tables"].append(join_table.name)
        elif isinstance(join_table, exp.Subquery):
            # Recursively process the subquery in the JOIN
            sub_infos = parse_select(join_table.this)
            if sub_infos:
                # Assuming the first dictionary corresponds to the subquery
                sub_info = sub_infos[0]
                join_info["tables"].extend(sub_info["tables"])
                join_info["columns"].extend(sub_info["columns"])
                join_info["where_columns"].extend(sub_info["where_columns"])
                join_info["limits"].extend(sub_info["limits"])

        # Process the JOIN condition (ON clause)
        on_expr = join.args.get("on")
        if on_expr:
            join_info["where_columns"] = process_where(on_expr.this)

        # Append join_info to infos
        infos.append(join_info)

    return infos


def extract_column(select_exp: exp.Expression) -> str:
    """
    Extracts the column name from a SELECT expression.

    Args:
        select_exp (exp.Expression): The SELECT expression.

    Returns:
        str: The column name or expression.
    """
    if isinstance(select_exp, exp.Column):
        return select_exp.name
    elif isinstance(select_exp, exp.Star):
        return "*"
    else:
        # Handle expressions like functions or aliases
        return select_exp.sql()


def process_where(where_condition: exp.Expression) -> List[Dict[str, Any]]:
    """
    Processes the WHERE condition to extract subqueries and their information.

    Args:
        where_condition (exp.Expression): The WHERE condition expression.

    Returns:
        List[Dict[str, Any]]: A list of condition dictionaries extracted from the WHERE clause.
    """
    where_columns = []

    # Handle logical operators (AND, OR)
    if isinstance(where_condition, (exp.And, exp.Or)):
        # Recursively process each part of the logical condition
        for arg in where_condition.args.values():
            if isinstance(arg, list):
                for expr in arg:
                    where_columns.extend(process_where(expr))
            else:
                where_columns.extend(process_where(arg))
    else:
        # Check for subqueries within the condition
        subqueries = where_condition.find_all(exp.Subquery)
        for subquery in subqueries:
            sub_select = subquery.this
            if isinstance(sub_select, exp.Select):
                sub_info = {"tables": [], "columns": [], "where_columns": [], "limits": []}
                # Extract tables from the subquery's FROM clause
                from_expr = sub_select.args.get("from")
                if from_expr:
                    for table in from_expr.find_all(exp.Table):
                        sub_info["tables"].append(table.name)

                # Extract columns from the subquery's SELECT expressions
                for select_exp in sub_select.expressions:
                    column = extract_column(select_exp)
                    if column:
                        sub_info["columns"].append(column)

                # Extract WHERE clause from the subquery
                where_expr = sub_select.args.get("where")
                if where_expr:
                    sub_info["where_columns"] = process_where(where_expr.this)

                # Extract LIMIT clause from the subquery
                limit_expr = sub_select.args.get("limit")
                if limit_expr:
                    sub_info["limits"].append(limit_expr.sql())

                # Append the subquery info to where_columns
                where_columns.append(sub_info)

    return where_columns


# Example Usage
if __name__ == "__main__":
    test_sql = """
       SELECT a, b 
       FROM table
       WHERE colc > (
           SELECT d 
           FROM t2
       );

       SELECT a, b 
       FROM table2;

       SELECT x, y 
       FROM table3 
       WHERE x>10 and colx IN (
           SELECT * 
           FROM t4
       );
    """

    print(repr(parse_one(test_sql, read="snowflake")))
    # Parse the SQL and extract infos
    infos = parse_sql(test_sql, dialect="snowflake")

    # Print the resulting infos
    import pprint

    pprint.pprint(infos)
