import json
import pandas as pd
import sqlglot
from sqlglot.expressions import Join, Column, Table, EQ
from collections import Counter


def parse_schema_txt(file_path):
    """
    Parse the .txt schema into a dictionary structure.
    """
    schema = {}
    current_table = None

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("--"):
                continue  # Skip empty lines or comments
            if line.endswith(":DELTA:2000"):  # Detect table declarations
                current_table = line.split(":")[0].strip().lower()
                schema[current_table] = []
            elif current_table and line.startswith("{") and line.endswith("}"):
                try:
                    line_content = line.strip("{}").split(":")
                    column_name = line_content[0].strip().lower().strip('"')
                    column_type = line_content[1].strip().lower()
                    schema[current_table].append({"name": column_name, "type": column_type})
                except (IndexError, ValueError) as e:
                    print(f"Failed to parse line: {line}, Error: {e}")
    return schema


def parse_schema_json(file_path):
    """
    Parse the .json schema into a dictionary structure.
    """
    with open(file_path, 'r') as file:
        schema = json.load(file)

    normalized_schema = {
        table_info["table_name"].lower(): [
            {"name": col_name.lower(), "type": "unknown"}  # No type information in JSON
            for col_name in table_info["column_names"]
        ]
        for table_info in schema
    }

    return normalized_schema


def merge_schemas(txt_schema, json_schema):
    """
    Merge the .txt and .json schema dictionaries, using the .txt schema as the source for types.
    """
    merged_schema = {}

    for table, columns in json_schema.items():
        if table in txt_schema:
            txt_columns = {col["name"]: col["type"] for col in txt_schema[table]}
            merged_schema[table] = [
                {"name": col["name"], "type": txt_columns.get(col["name"], "unknown")}
                for col in columns
            ]
        else:
            merged_schema[table] = columns

    for table, columns in txt_schema.items():
        if table not in merged_schema:
            merged_schema[table] = columns

    return merged_schema


def parse_queries_csv(file_path):
    """
    Load the CSV file containing SQL queries and their associated metadata.
    """
    queries_df = pd.read_csv(file_path)
    print("Available Columns in CSV:", queries_df.columns)
    
    queries = queries_df['Original_Query'].tolist()
    table_lists = queries_df['list_of_tables'].apply(eval).tolist()  # Parse stringified lists into actual lists
    
    return queries, table_lists


def validate_query(query, schema, expected_tables=None):
    parsed = sqlglot.parse(query, error_level=None)
    invalid_tables = []
    invalid_columns = []
    joins = []
    extracted_tables = set()

    alias_mapping = {}
    cte_tables = set()

    def find_table_for_column(column_name, schema):
        """Find the table in the schema that contains the given column."""
        for table, columns in schema.items():
            if any(col["name"] == column_name for col in columns):
                return table
        return ""

    for expression in parsed:
        for cte in expression.find_all(sqlglot.expressions.CTE):
            cte_name = cte.alias_or_name.lower()
            cte_tables.add(cte_name)

        # Extract tables and handle aliases
        for table in expression.find_all(Table):
            table_name = table.name.lower()
            alias = table.alias
            if alias:
                alias_mapping[alias] = table_name
            if table_name not in schema and table_name not in cte_tables:
                invalid_tables.append(table_name)
            extracted_tables.add(table_name)

        # Extract columns
        for column in expression.find_all(Column):
            table_name = alias_mapping.get(column.table, column.table)
            column_name = column.name.lower()

            if table_name and table_name.lower() not in schema and table_name not in cte_tables:
                invalid_tables.append(table_name)
            elif table_name and column_name not in [col["name"] for col in schema.get(table_name.lower(), [])]:
                invalid_columns.append(f"{table_name.lower()}.{column_name}")

        # Extract joins
        for join in expression.find_all(Join):
            on_expression = join.args.get("on")
            if on_expression and isinstance(on_expression, EQ):
                left = on_expression.this
                right = on_expression.expression
                if isinstance(left, Column) and isinstance(right, Column):
                    left_table = alias_mapping.get(left.table, left.table).lower() or find_table_for_column(left.name, schema)
                    right_table = alias_mapping.get(right.table, right.table).lower() or find_table_for_column(right.name, schema)

                    # Add the resolved join
                    joins.append({
                        "left_table": left_table,
                        "left_column": left.name,
                        "right_table": right_table,
                        "right_column": right.name,
                    })

        # Handle implicit joins in WHERE clause
        where_clause = expression.args.get("where")
        if where_clause:
            for condition in where_clause.find_all(EQ):
                left = condition.this
                right = condition.expression
                if isinstance(left, Column) and isinstance(right, Column):
                    left_table = alias_mapping.get(left.table, left.table).lower() or find_table_for_column(left.name, schema)
                    right_table = alias_mapping.get(right.table, right.table).lower() or find_table_for_column(right.name, schema)

                    # Add the resolved join
                    joins.append({
                        "left_table": left_table,
                        "left_column": left.name,
                        "right_table": right_table,
                        "right_column": right.name,
                    })

    # Compare extracted tables with expected tables
    mismatched_tables = []
    if expected_tables:
        extracted_tables = set(extracted_tables)
        expected_tables = set(map(str.lower, expected_tables))
        mismatched_tables = list(expected_tables.symmetric_difference(extracted_tables))

    return {
        "invalid_tables": list(set(invalid_tables)),
        "invalid_columns": list(set(invalid_columns)),
        "joins": joins,
        "extracted_tables": list(extracted_tables),
        "mismatched_tables": mismatched_tables
    }
def validate_query(query, schema, expected_tables=None):
    parsed = sqlglot.parse(query, error_level=None)
    invalid_tables = []
    invalid_columns = []
    joins = []
    extracted_tables = set()

    alias_mapping = {}
    cte_tables = set()

    def find_table_for_column(column_name, schema):
        """Find the table in the schema that contains the given column."""
        for table, columns in schema.items():
            if any(col["name"] == column_name for col in columns):
                return table
        return ""

    for expression in parsed:
        for cte in expression.find_all(sqlglot.expressions.CTE):
            cte_name = cte.alias_or_name.lower()
            cte_tables.add(cte_name)

        # Extract tables and handle aliases
        for table in expression.find_all(Table):
            table_name = table.name.lower()
            alias = table.alias
            if alias:
                alias_mapping[alias] = table_name
            if table_name not in schema and table_name not in cte_tables:
                invalid_tables.append(table_name)
            extracted_tables.add(table_name)

        # Extract columns
        for column in expression.find_all(Column):
            table_name = alias_mapping.get(column.table, column.table)
            column_name = column.name.lower()

            if table_name and table_name.lower() not in schema and table_name not in cte_tables:
                invalid_tables.append(table_name)
            elif table_name and column_name not in [col["name"] for col in schema.get(table_name.lower(), [])]:
                invalid_columns.append(f"{table_name.lower()}.{column_name}")

        # Extract joins
        for join in expression.find_all(Join):
            on_expression = join.args.get("on")
            if on_expression and isinstance(on_expression, EQ):
                left = on_expression.this
                right = on_expression.expression
                if isinstance(left, Column) and isinstance(right, Column):
                    left_table = alias_mapping.get(left.table, left.table).lower() or find_table_for_column(left.name, schema)
                    right_table = alias_mapping.get(right.table, right.table).lower() or find_table_for_column(right.name, schema)

                    # Add the resolved join
                    joins.append({
                        "left_table": left_table,
                        "left_column": left.name,
                        "right_table": right_table,
                        "right_column": right.name,
                    })

        # Handle implicit joins in WHERE clause
        where_clause = expression.args.get("where")
        if where_clause:
            for condition in where_clause.find_all(EQ):
                left = condition.this
                right = condition.expression
                if isinstance(left, Column) and isinstance(right, Column):
                    left_table = alias_mapping.get(left.table, left.table).lower() or find_table_for_column(left.name, schema)
                    right_table = alias_mapping.get(right.table, right.table).lower() or find_table_for_column(right.name, schema)

                    # Add the resolved join
                    joins.append({
                        "left_table": left_table,
                        "left_column": left.name,
                        "right_table": right_table,
                        "right_column": right.name,
                    })

    # Compare extracted tables with expected tables
    mismatched_tables = []
    if expected_tables:
        extracted_tables = set(extracted_tables)
        expected_tables = set(map(str.lower, expected_tables))
        mismatched_tables = list(expected_tables.symmetric_difference(extracted_tables))

    return {
        "invalid_tables": list(set(invalid_tables)),
        "invalid_columns": list(set(invalid_columns)),
        "joins": joins,
        "extracted_tables": list(extracted_tables),
        "mismatched_tables": mismatched_tables
    }





def analyze_queries(queries, list_of_tables_column, schema):
    """
    Analyze a list of queries for validation and components.
    """
    results = []
    valid_queries = 0
    invalid_queries = 0
    mismatched_queries = 0
    table_usage = Counter()
    column_usage = Counter()
    table_interactions = Counter()  # To track table-table interactions
    column_interactions = Counter()  # To track column-column interactions

    for query, expected_tables in zip(queries, list_of_tables_column):
        result = validate_query(query, schema, expected_tables)
        if not result["invalid_tables"] and not result["invalid_columns"]:
            valid_queries += 1
        else:
            invalid_queries += 1

        if result["mismatched_tables"]:
            mismatched_queries += 1

        # Track both valid and invalid table usage
        for table in result["extracted_tables"]:
            table_usage.update([table])

        # Track column usage
        for column in [f"{join['left_table']}.{join['left_column']}" for join in result["joins"]] + \
                      [f"{join['right_table']}.{join['right_column']}" for join in result["joins"]]:
            column_usage.update([column])

        # Track join interactions
        for join in result["joins"]:
            table_pair = tuple(sorted([join["left_table"], join["right_table"]]))
            if table_pair[0] and table_pair[1]:  # Exclude empty table names
                table_interactions.update([table_pair])
            else:
                print(f"Empty table names found in join: {join}")


            # Column-column interactions
            column_pair = tuple(sorted([
                f"{join['left_table']}.{join['left_column']}",
                f"{join['right_table']}.{join['right_column']}"
            ]))
            column_interactions.update([column_pair])

        results.append(result)

    return results, valid_queries, invalid_queries, mismatched_queries, table_usage, column_usage, table_interactions, column_interactions

def parse_schema_json_with_comments(file_path):
    """
    Parse the .json schema into a dictionary structure, with table comments included.
    """
    with open(file_path, 'r') as file:
        schema = json.load(file)

    # Transform schema into a dictionary with table_name as the key and include comments
    schema_with_comments = {
        table_info["table_name"].lower(): {
            "columns": [{"name": col_name.lower(), "type": "unknown"} for col_name in table_info["column_names"]],
            "comment": table_info.get("table_comment", "No comment available")  # Include the table comment
        }
        for table_info in schema
    }

    # Create a simplified schema for validation (only columns)
    schema_without_comments = {
        table: details["columns"] for table, details in schema_with_comments.items()
    }

    return schema_without_comments, schema_with_comments

def display_statistics(valid_queries, invalid_queries, mismatched_queries, table_usage, column_usage, table_interactions, column_interactions):
    """
    Display statistical results from the analysis, including mismatched queries and interactions.
    """
    print("\n--- Query Analysis Statistics ---")
    print(f"Total Queries: {valid_queries + invalid_queries}")
    print(f"Valid Queries: {valid_queries}")
    print(f"Invalid Queries: {invalid_queries}")
    print(f"Mismatched Queries (parsed vs expected): {mismatched_queries}")

    print("\nMost Used Tables:")
    for table, count in table_usage.most_common(5):
        print(f"  {table}: {count} times")

    print("\nMost Used Columns:")
    for column, count in column_usage.most_common(5):
        print(f"  {column}: {count} times")

    print("\nMost Frequent Table-Table Interactions:")
    for (table1, table2), count in table_interactions.most_common():
        if table1 and table2:  # Exclude empty table names
            print(f"  {table1} - {table2}: {count} times")

    print("\nMost Frequent Column-Column Interactions:")
    for (column1, column2), count in column_interactions.most_common(5):
        print(f"  {column1} - {column2}: {count} times")



if __name__ == "__main__":
    # File paths
    txt_schema_path = "/Users/prakalp/Downloads/tpcds_1000_delta_schema.txt"
    json_schema_path =  "/Users/prakalp/Downloads/tpcds_1000_delta_transformed_schema.json" 
    csv_path = "/Users/prakalp/Downloads/processed_results (17).csv" 

    # Parse schema and queries
    txt_schema = parse_schema_txt(txt_schema_path)
    schema_without_comments, _ = parse_schema_json_with_comments(json_schema_path)
    combined_schema = merge_schemas(txt_schema, schema_without_comments)

    queries_df = pd.read_csv(csv_path)
    queries = queries_df["Original_Query"].tolist()
    list_of_tables_column = queries_df["list_of_tables"].apply(eval).tolist()

    # Analyze queries
    analysis_results, valid_queries, invalid_queries, mismatched_queries, table_usage, column_usage, table_interactions, column_interactions = analyze_queries(
        queries, list_of_tables_column, combined_schema
    )

    # Display statistics
    display_statistics(
        valid_queries,
        invalid_queries,
        mismatched_queries,
        table_usage,
        column_usage,
        table_interactions,
        column_interactions
    )


