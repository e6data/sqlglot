import unittest
import re

import sqlglot
from sqlglot import parse_one
from sqlglot.optimizer.qualify_columns import quote_identifiers


class TestE6Dialect(unittest.TestCase):
    def test_for_double_quotes_on_identifiers(self):
        def replace_struct_in_query(query):
            # Define the regex pattern to match Struct(Struct(anything))
            pattern = re.compile(r"Struct\s*\(\s*Struct\s*\(\s*([^\(\)]+)\s*\)\s*\)", re.IGNORECASE)

            # Function to perform the replacement
            def replace_match(match):
                return f"{{{{{match.group(1)}}}}}"

            # Process the query
            if query is not None:
                modified_query = pattern.sub(replace_match, query)
                return modified_query
            return query
        e6_query = """ 
    ARRAY_SLICE(array(1,2), B, C);
            """
        tree = parse_one(e6_query, read="E6")
        print("-------------- DBR AST---------------")
        print(repr(tree))

        converted_query = sqlglot.transpile(e6_query, "E6", "snowflake", identify=False)[0]
        print("-------------First transpiler query----------------------")
        print(converted_query)
        # converted_query_ast = parse_one(converted_query, read="E6")
        # double_quotes_added_query = quote_identifiers(converted_query_ast, dialect="E6").sql(
        #     dialect="E6"
        # )
        # tree1 = parse_one(double_quotes_added_query, read="E6")
        # print("----------E6 AST------------------")
        # print(repr(tree1))
        # double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)
        #
        # print("--------------Actual E6 query-------------------")
        # print(double_quotes_added_query)