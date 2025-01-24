import json
import pandas as pd
import sqlglot
from sqlglot.expressions import Join, Column, Table, EQ
from collections import Counter
from collections import Counter, defaultdict

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

    # Transform schema into a dictionary with table_name as the key
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
    Load the CSV file containing SQL queries.
    """
    queries_df = pd.read_csv(file_path)
    print("Available Columns in CSV:", queries_df.columns)
    queries = queries_df['Original_Query'].tolist()  # Replace with the actual column name
    return queries


def validate_query(query, schema):
    """
    Validate that tables and columns in a query exist in the schema.
    """
    parsed = sqlglot.parse(query, error_level=None)
    invalid_tables = []
    invalid_columns = []
    joins = []

    alias_mapping = {}
    cte_tables = set()

    for expression in parsed:
        for cte in expression.find_all(sqlglot.expressions.CTE):
            cte_name = cte.alias_or_name.lower()
            cte_tables.add(cte_name)

        for table in expression.find_all(Table):
            table_name = table.name.lower()
            alias = table.alias
            if alias:
                alias_mapping[alias] = table_name
            if table_name not in schema and table_name not in cte_tables:
                invalid_tables.append(table_name)

        for column in expression.find_all(Column):
            table_name = alias_mapping.get(column.table, column.table)
            column_name = column.name.lower()

            if table_name and table_name.lower() not in schema and table_name not in cte_tables:
                invalid_tables.append(table_name)
            elif table_name and column_name not in [col["name"] for col in schema.get(table_name.lower(), [])]:
                invalid_columns.append(f"{table_name.lower()}.{column_name}")

        for join in expression.find_all(Join):
            on_expression = join.args.get("on")
            if not on_expression:
                print(f"Join without ON clause detected: {join}")
            else:
                print(f"Valid join detected with ON condition: {on_expression}")
                if isinstance(on_expression, EQ):
                    left = on_expression.this
                    right = on_expression.expression
                    if isinstance(left, Column) and isinstance(right, Column):
                        left_table = alias_mapping.get(left.table, left.table).lower()
                        right_table = alias_mapping.get(right.table, right_table).lower()
                        joins.append({
                            "left_table": left_table,
                            "left_column": left.name,
                            "right_table": right_table,
                            "right_column": right.name,
                        })

        # Check for implicit joins in the WHERE clause
        where_clause = expression.args.get("where")
        if where_clause:
            for condition in where_clause.find_all(EQ):
                left = condition.this
                right = condition.expression
                if isinstance(left, Column) and isinstance(right, Column):
                    left_table = alias_mapping.get(left.table, left.table).lower()
                    right_table = alias_mapping.get(right.table, right.table).lower()
                    joins.append({
                        "left_table": left_table,
                        "left_column": left.name,
                        "right_table": right_table,
                        "right_column": right.name,
                    })

    return {
        "invalid_tables": list(set(invalid_tables)),
        "invalid_columns": list(set(invalid_columns)),
        "joins": joins
    }




def analyze_queries(queries, schema):
    """
    Analyze a list of queries for validation and components.
    """
    results = []
    valid_queries = 0
    invalid_queries = 0
    table_usage = Counter()
    column_usage = Counter()
    table_interactions = Counter()  # To track table-table interactions
    column_interactions = Counter()  # To track column-column interactions

    for query in queries:
        result = validate_query(query, schema)
        print("Joins detected:", result["joins"])
        if not result["invalid_tables"] and not result["invalid_columns"]:
            valid_queries += 1
        else:
            invalid_queries += 1

        # Track both valid and invalid table usage
        for table in [table.name.lower() for table in sqlglot.parse(query, error_level=None)[0].find_all(Table)]:
            table_usage.update([table])

        # Track both valid and invalid column usage
        for column in [col.name.lower() for col in sqlglot.parse(query, error_level=None)[0].find_all(Column)]:
            column_usage.update([column])

        # Update invalid-specific counters for debugging
        for table in result["invalid_tables"]:
            table_usage.update([table])

        for column in result["invalid_columns"]:
            column_usage.update([column])

        # Track join interactions
        for join in result["joins"]:
            # Table-table interactions
            table_pair = tuple(sorted([join["left_table"], join["right_table"]]))  # Sort to avoid duplicates
            table_interactions.update([table_pair])

            # Column-column interactions
            column_pair = tuple(sorted([
                f"{join['left_table']}.{join['left_column']}",
                f"{join['right_table']}.{join['right_column']}"
            ]))
            column_interactions.update([column_pair])

        results.append({
            "query": query,
            "invalid_tables": result["invalid_tables"],
            "invalid_columns": result["invalid_columns"],
            "joins": result["joins"]
        })

    return results, valid_queries, invalid_queries, table_usage, column_usage, table_interactions, column_interactions

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
def display_statistics(valid_queries, invalid_queries, table_usage, column_usage, table_interactions, column_interactions, schema_with_comments):
    """
    Display statistical results from the analysis, including table comments and interactions.
    """
    print("\n--- Query Analysis Statistics ---")
    print(f"Total Queries: {valid_queries + invalid_queries}")
    print(f"Valid Queries: {valid_queries}")
    print(f"Invalid Queries: {invalid_queries}")
    
    print("\nMost Used Tables:")
    for table, count in table_usage.most_common(5):
        table_comment = schema_with_comments.get(table, {}).get("comment", "No comment available")
        print(f"  {table} ({table_comment}): {count} times")

    print("\nMost Used Columns:")
    for column, count in column_usage.most_common(5):
        print(f"  {column}: {count} times")

    print("\nMost Frequent Table-Table Interactions:")
    for (table1, table2), count in table_interactions.most_common(5):
        print(f"  {table1} - {table2}: {count} times")

    print("\nMost Frequent Column-Column Interactions:")
    for (column1, column2), count in column_interactions.most_common(5):
        print(f"  {column1} - {column2}: {count} times")


if __name__ == "__main__":
    # File paths
    txt_schema_path = "/Users/prakalp/Downloads/tpcds_1000_delta_schema.txt"
    json_schema_path = "/Users/prakalp/Downloads/tpcds_1000_delta_transformed_schema.json"
    csv_path = "/Users/prakalp/Downloads/processed_results (17).csv"

    # Parse schema and queries
    txt_schema = parse_schema_txt(txt_schema_path)
    schema_without_comments, schema_with_comments = parse_schema_json_with_comments(json_schema_path)
    combined_schema = merge_schemas(txt_schema, schema_without_comments)
    queries = parse_queries_csv(csv_path)

    # Analyze queries
    analysis_results, valid_queries, invalid_queries, table_usage, column_usage, table_interactions, column_interactions = analyze_queries(
        queries, combined_schema
    )

    # Display results for a subset of queries
    for res in analysis_results[:5]:
        print(f"Query: {res['query']}")
        print(f"Invalid Tables: {res['invalid_tables']}")
        print(f"Invalid Columns: {res['invalid_columns']}")
        print(f"Joins: {res['joins']}")
        print("-" * 50)

    # Display overall statistics with table comments and join interactions
    display_statistics(
        valid_queries,
        invalid_queries,
        table_usage,
        column_usage,
        table_interactions,
        column_interactions,
        schema_with_comments
    )
