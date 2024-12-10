import unittest
import re
import sqlparse

import sqlglot
from sqlglot import parse_one, ParseError, transpile
from sqlglot import dialects
from sqlglot import exp
from tests.dialects.test_dialect import Validator
from sqlglot.optimizer.qualify_columns import quote_identifiers


class TestE6Dialect(unittest.TestCase):
    def test_for_double_quotes_on_identifiers(self):
        def replace_struct_in_query(query):
            # Define the regex pattern to match Struct(Struct(anything))
            pattern = re.compile(r'Struct\s*\(\s*Struct\s*\(\s*([^\(\)]+)\s*\)\s*\)', re.IGNORECASE)

            # Function to perform the replacement
            def replace_match(match):
                return f"{{{{{match.group(1)}}}}}"

            # Process the query
            if query is not None:
                modified_query = pattern.sub(replace_match, query)
                return modified_query
            return query

        e6_query_1 = """
        SELECT pi(),
        sin(x), sinh(x), cos(x), cosh(x), tan(x), tanh(x), degrees(y), radians(z), Slice([a,b,c,d,e],2,2), Hour(a), minute(a), second(a), soundex(a), regexp_extract_all('1a 2b 14m', '\d+'), regexp_count('1a 2b 14m', '\s*[a-z]+\s*'); 
        """

        e6_query = """
SELECT ARRAY_APPEND(ARRAY_CONSTRUCT(5,6,7,8),NULL);
          """

        e6_query_2 = """
SELECT with_timezone(TIMESTAMP '2022-11-01 09:08:07.321', 'America/Los_Angeles') AS WITH_TIMEZONE,
        bitwise_left_shift(1, 2) AS SHIFT_LEFT, 
        bitwise_right_shift(1, 2) AS SHIFT_RIGHT, 
        CONCAT([1,2],[3,4]) AS ARRAY_CONCAT, 
        JSON_VALUE(A,B) as json_value,
        listagg(value, ',') as list_agg,
        pow(x,2) as power,
        STRPOS('hahahahehehe','ehe') as starpos,
        from_unixtime(unixtime/1000) as FROM_UNIXTIME,
        to_unixtime('09-11-2024 09:08:07') as to_unix_time,
        DATE_PARSE('2022/10/20/05','%Y/%m/%d/%H') as date_parse,
        date_diff('DAY', '09-11-2024', '11-11-2024') as DATE_DIFF,
        DAY('09-11-2024') AS DAY,
        last_day_of_month('09-11-2024') as last_day,
        day_of_week('09-11-2024') as day_Of_Week,
        DOW('09-11-2024') as dayOfWeek,
        week('09-11-2024') as week,
        format_datetime('09-11-2024 09:08:07', '%d-%m-%y') as format_datetime,
        array_position(array[1,2,3,1.9],1.9) as array_position,
        cardinality([1,2,3]) as cardinality_size,
        CONTAINS([1.2.3],2) as array_contains,
        filter(ARRAY[5, -6, NULL, 7], x -> x > 0) as array_filter,
        approx_distinct(x) as approx_count_distinct,
        json_query(description, 'lax $.children'),
        slice([a,b,c,d,e],2,2),
        rtrim(x),
        trim(x),
        ltrim(x),
        lpad('ab',5,'0'),
        rpad('ab',5,'0')
                """
        tree = parse_one(e6_query, read="snowflake")
        print(repr(tree))

        converted_query = sqlglot.transpile(e6_query, "snowflake",
                                            "E6", identify=False)[0]
        print(converted_query)
        converted_query_ast = parse_one(converted_query, read="E6")
        # print(repr(converted_query_ast))
        # print(converted_query_ast.sql(dialect="E6"))
        double_quotes_added_query = quote_identifiers(converted_query_ast, dialect="E6").sql(dialect="E6")
        tree1 = parse_one(double_quotes_added_query, read="E6")
        print(repr(tree1))
        double_quotes_added_query = replace_struct_in_query(double_quotes_added_query)

        print(double_quotes_added_query)

    def test_for_table_extractor(self):
        query = ""
        from_sql = "athena"
        tables_list = []
        tree = parse_one(query, from_sql)
        if query:
            print(repr(tree))
            for table in tree.find_all(exp.Table):
                if table.db:
                    tables_list.append(f"{table.db}.{table.name}")
                else:
                    tables_list.append(table.name)
            tables_list = list(set(tables_list))
            for alias in tree.find_all(exp.TableAlias):
                if isinstance(alias.parent, exp.CTE) and alias.name in tables_list:
                    tables_list.remove(alias.name)
                    print(alias.name)
        print(tables_list)
