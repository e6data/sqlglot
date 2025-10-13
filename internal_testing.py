import pandas as pd
import requests
import sqlglot
from sqlglot import pretty
from sqlglot.optimizer.qualify_columns import quote_identifiers
from apis.utils.helpers import replace_struct_in_query
from_sql = 'databricks'
to_sql = 'e6'
query = """SELECT UNIX_TIMESTAMP()
"""
tree = sqlglot.parse_one(query, read=from_sql, error_level=None)
tree2 = quote_identifiers(tree, dialect=to_sql)
print(f'AST {repr(tree2)}')
double_quotes_added_query = tree2.sql(dialect=to_sql, from_dialect=from_sql)
# print(f'Query\n: {double_quotes_added_query}')
double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)
print(f'Transpiled Query {double_quotes_added_query}')