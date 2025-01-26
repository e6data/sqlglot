from tests.dialects.test_dialect import Validator


class TestE6(Validator):
    maxDiff = None
    dialect = "E6"

    def test_E6(self):
        self.validate_all(
            "SELECT DATETIME(CAST('2022-11-01 09:08:07.321' AS TIMESTAMP), 'America/Los_Angeles')",
            read={
                "snowflake": "Select convert_timezone('America/Los_Angeles', '2022-11-01 09:08:07.321' ::TIMESTAMP)",
                "databricks": "Select convert_timezone('America/Los_Angeles', '2022-11-01 09:08:07.321' ::TIMESTAMP)",
            },
        )

        self.validate_all(
            "SHIFTLEFT(x, 1)",
            read={
                "trino": "bitwise_left_shift(x, 1)",
                "duckdb": "x << 1",
                "hive": "x << 1",
                "spark": "SHIFTLEFT(x, 1)",
                "snowflake": "BITSHIFTLEFT(x, 1)",
            },
            write={
                "snowflake": "BITSHIFTLEFT(x, 1)",
                "spark": "SHIFTLEFT(x, 1)",
                "trino": "BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)",
            },
        )

        self.validate_all(
            "SHIFTRIGHT(x, 1)",
            read={
                "trino": "bitwise_right_shift(x, 1)",
                "duckdb": "x >> 1",
                "hive": "x >> 1",
                "spark": "SHIFTRIGHT(x, 1)",
                "snowflake": "BITSHIFTRIGHT(x, 1)",
            },
            write={
                "snowflake": "BITSHIFTRIGHT(x, 1)",
                "spark": "SHIFTRIGHT(x, 1)",
                "databricks": "SHIFTRIGHT(x, 1)",
                "trino": "BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)",
            },
        )

        self.validate_all(
            "SELECT ARRAY_CONCAT(ARRAY[1, 2], ARRAY[3, 4])",
            read={
                "trino": "SELECT CONCAT(ARRAY[1,2], ARRAY[3,4])",
                "snowflake": "SELECT ARRAY_CAT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4))",
            },
        )

        self.validate_all(
            "POWER(x, 2)",
            read={
                "bigquery": "POWER(x, 2)",
                "clickhouse": "POWER(x, 2)",
                "databricks": "POWER(x, 2)",
                "drill": "POW(x, 2)",
                "duckdb": "POWER(x, 2)",
                "hive": "POWER(x, 2)",
                "mysql": "POWER(x, 2)",
                "oracle": "POWER(x, 2)",
                "postgres": "x ^ 2",
                "presto": "POWER(x, 2)",
                "redshift": "POWER(x, 2)",
                "snowflake": "POWER(x, 2)",
                "spark": "POWER(x, 2)",
                "sqlite": "POWER(x, 2)",
                "starrocks": "POWER(x, 2)",
                "teradata": "x ** 2",
                "trino": "POWER(x, 2)",
                "tsql": "POWER(x, 2)",
            },
        )

        self.validate_all(
            "SELECT TO_TIMESTAMP('2024-11-09', 'dd-MM-YY')",
            read={"trino": "SELECT date_parse('2024-11-09', '%d-%m-%y')"},
        )

        self.validate_all("SELECT DAYS('2024-11-09')", read={"trino": "SELECT DAYS('2024-11-09')"})

        self.validate_all(
            "SELECT LAST_DAY(CAST('2024-11-09' AS TIMESTAMP))",
            read={"trino": "SELECT LAST_DAY_OF_MONTH(CAST('2024-11-09' AS TIMESTAMP))"},
        )

        self.validate_identity("SELECT LAST_DAY(CAST('2024-11-09' AS TIMESTAMP), UNIT)")

        self.validate_all(
            "SELECT DAYOFWEEKISO('2024-11-09')",
            read={
                "trino": "SELECT day_of_week('2024-11-09')",
            },
        )

        self.validate_all(
            "POSITION(needle in haystack from c)",
            write={
                "spark": "LOCATE(needle, haystack, c)",
                "clickhouse": "position(haystack, needle, c)",
                "snowflake": "POSITION(needle, haystack, c)",
                "mysql": "LOCATE(needle, haystack, c)",
            },
        )

        self.validate_all(
            "SELECT FORMAT_DATE('2024-11-09 09:08:07', 'dd-MM-YY')",
            read={"trino": "SELECT format_datetime('2024-11-09 09:08:07', '%d-%m-%y')"},
        )

        self.validate_all(
            "SELECT ARRAY_POSITION(1.9, ARRAY[1, 2, 3, 1.9])",
            read={
                "trino": "SELECT ARRAY_position(ARRAY[1, 2, 3, 1.9],1.9)",
                "snowflake": "SELECT ARRAY_position(1.9, ARRAY[1, 2, 3, 1.9])",
                "databricks": "SELECT ARRAY_position(ARRAY[1, 2, 3, 1.9],1.9)",
                "postgres": "SELECT ARRAY_position(ARRAY[1, 2, 3, 1.9],1.9)",
                "starrocks": "SELECT ARRAY_position(ARRAY[1, 2, 3, 1.9],1.9)",
            },
            write={
                "trino": "SELECT ARRAY_POSITION(ARRAY[1, 2, 3, 1.9], 1.9)",
                "snowflake": "SELECT ARRAY_POSITION(1.9, [1, 2, 3, 1.9])",
                "databricks": "SELECT ARRAY_POSITION(ARRAY(1, 2, 3, 1.9), 1.9)",
                "postgres": "SELECT ARRAY_POSITION(ARRAY[1, 2, 3, 1.9], 1.9)",
                "starrocks": "SELECT ARRAY_POSITION([1, 2, 3, 1.9], 1.9)",
            },
        )

        self.validate_all(
            "SELECT SIZE(ARRAY[1, 2, 3])",
            read={
                "trino": "SELECT CARDINALITY(ARRAY[1, 2, 3])",
                "snowflake": "SELECT ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2, 3))",
                "databricks": "SELECT ARRAY_SIZE(ARRAY[1, 2, 3])",
            },
            write={
                "trino": "SELECT CARDINALITY(ARRAY[1, 2, 3])",
                "snowflake": "SELECT ARRAY_SIZE([1, 2, 3])",
            },
        )

        self.validate_all(
            "ARRAY_CONTAINS(x, 1)",
            read={
                "duckdb": "LIST_HAS(x, 1)",
                "snowflake": "ARRAY_CONTAINS(1, x)",
                "trino": "CONTAINS(x, 1)",
                "presto": "CONTAINS(x, 1)",
                "hive": "ARRAY_CONTAINS(x, 1)",
                "spark": "ARRAY_CONTAINS(x, 1)",
            },
            write={
                "duckdb": "ARRAY_CONTAINS(x, 1)",
                "presto": "CONTAINS(x, 1)",
                "hive": "ARRAY_CONTAINS(x, 1)",
                "spark": "ARRAY_CONTAINS(x, 1)",
                "snowflake": "ARRAY_CONTAINS(1, x)",
            },
        )

        # This functions tests the `_parse_filter_array` functions that we have written.
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[5, -6, NULL, 7], x -> x > 0)",
            read={"trino": "SELECT filter(ARRAY[5, -6, NULL, 7], x -> x > 0)"},
        )

        self.validate_all(
            "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            read={
                "trino": "SELECT approx_distinct(a) FROM foo",
                "duckdb": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "presto": "SELECT APPROX_DISTINCT(a) FROM foo",
                "hive": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
                "spark": "SELECT APPROX_COUNT_DISTINCT(a) FROM foo",
            },
        )

        self.validate_all(
            "SELECT LOCATE('ehe', 'hahahahehehe')",
            read={"trino": "SELECT STRPOS('hahahahehehe','ehe')"},
        )

        self.validate_all(
            "SELECT JSON_EXTRACT(x, '$.name')",
            read={
                "trino": "SELECT JSON_QUERY(x, '$.name')",
                "presto": "SELECT JSON_EXTRACT(x, '$.name')",
                "hive": "SELECT GET_JSON_OBJECT(x, '$.name')",
                "spark": "SELECT GET_JSON_OBJECT(x, '$.name')",
            },
        )

        self.validate_all(
            "SELECT DATE_DIFF('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
            read={
                "trino": "SELECT date_diff('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
                "snowflake": "SELECT DATEDIFF(DAY, CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
                "presto": "SELECT date_diff('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
                "spark": "SELECT DATEDIFF(DAY, '2024-11-11', '2024-11-09')",
            },
            write={
                "E6": "SELECT DATE_DIFF('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))"
            },
        )

        self.validate_all(
            "SELECT FROM_UNIXTIME_WITHUNIT(1674797653, 'milliseconds')",
            read={
                "trino": "SELECT from_unixtime(1674797653)",
            },
        )

        self.validate_all(
            "SELECT FROM_UNIXTIME_WITHUNIT(unixtime / 1000, 'seconds')",
            read={"trino": "SELECT from_unixtime(unixtime/1000)"},
        )

        self.validate_all("SELECT AVG(x)", read={"trino": "SELECT AVG(x)"})

        self.validate_all(
            "SELECT MAX_BY(a.id, a.timestamp) FROM a",
            read={
                "bigquery": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "clickhouse": "SELECT argMax(a.id, a.timestamp) FROM a",
                "duckdb": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "snowflake": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "spark": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "teradata": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
            },
            write={
                "E6": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "bigquery": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "clickhouse": "SELECT argMax(a.id, a.timestamp) FROM a",
                "duckdb": "SELECT ARG_MAX(a.id, a.timestamp) FROM a",
                "presto": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "snowflake": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "spark": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "teradata": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
            },
        )

        self.validate_all(
            "ARBITRARY(x)",
            read={
                "bigquery": "ANY_VALUE(x)",
                "clickhouse": "any(x)",
                "databricks": "ANY_VALUE(x)",
                "doris": "ANY_VALUE(x)",
                "drill": "ANY_VALUE(x)",
                "duckdb": "ANY_VALUE(x)",
                "mysql": "ANY_VALUE(x)",
                "oracle": "ANY_VALUE(x)",
                "redshift": "ANY_VALUE(x)",
                "snowflake": "ANY_VALUE(x)",
                "spark": "ANY_VALUE(x)",
            },
        )

        self.validate_all(
            "STARTS_WITH('abc', 'a')",
            read={
                "spark": "STARTSWITH('abc', 'a')",
                "presto": "STARTS_WITH('abc', 'a')",
                "snowflake": "STARTSWITH('abc', 'a')",
            },
        )

        self.validate_all(
            "SELECT CONTAINS_SUBSTR('X', 'Y')",
            read={"snowflake": "SELECT CONTAINS('X','Y')"},
            write={"snowflake": "SELECT CONTAINS('X', 'Y')"},
        )

        self.validate_all(
            "SELECT ELEMENT_AT(X, 4)",
            read={
                "snowflake": "SELECT get(X, 3)",
                "trino": "SELECT ELEMENT_AT(X, 4)",
                "databricks": "SELECT TRY_ELEMENT_AT(X, 4)",
                "spark": "SELECT TRY_ELEMENT_AT(X, 4)",
                "duckdb": "SELECT X[4]",
            },
            write={
                "snowflake": "SELECT X[3]",
                "trino": "SELECT ELEMENT_AT(X, 4)",
                "databricks": "SELECT TRY_ELEMENT_AT(X, 4)",
                "spark": "SELECT TRY_ELEMENT_AT(X, 4)",
                "duckdb": "SELECT X[4]",
            },
        )

        self.validate_all(
            "SELECT TO_UNIX_TIMESTAMP(A)",
            read={"databricks": "SELECT TO_UNIX_TIMESTAMP(A)"},
            write={"databricks": "SELECT TO_UNIX_TIMESTAMP(A)"},
        )

        self.validate_all(
            "SELECT CONVERT_TIMEZONE('Asia/Seoul', 'UTC', CAST('2016-08-31' AS TIMESTAMP))",
            read={"databricks": "SELECT to_utc_timestamp('2016-08-31', 'Asia/Seoul')"},
        )

        self.validate_all(
            "TO_JSON(x)",
            read={
                "spark": "TO_JSON(x)",
                "bigquery": "TO_JSON_STRING(x)",
                "presto": "JSON_FORMAT(x)",
            },
            write={
                "bigquery": "TO_JSON_STRING(x)",
                "duckdb": "CAST(TO_JSON(x) AS TEXT)",
                "presto": "JSON_FORMAT(x)",
                "spark": "TO_JSON(x)",
            },
        )

        self.validate_all(
            "SELECT EXTRACT(fieldStr FROM date_expr)",
            read={
                "databricks": "SELECT DATE_PART(fieldStr, date_expr)",
                "E6": "SELECT DATEPART(fieldStr, date_expr)",
            },
        )

        self.validate_all(
            "SELECT NOT A IS NULL",
            read={"databricks": "SELECT ISNOTNULL(A)"},
            write={"databricks": "SELECT NOT A IS NULL"},
        )

        self.validate_all(
            "SELECT A IS NULL",
            read={"databricks": "SELECT ISNULL(A)"},
            write={"databricks": "SELECT A IS NULL"},
        )

        self.validate_all(
            "TO_CHAR(CAST(x AS TIMESTAMP),'y')",
            read={
                "snowflake": "TO_VARCHAR(x, y)",
                "databricks": "TO_CHAR(x, y)",
                "oracle": "TO_CHAR(x, y)",
                "teradata": "TO_CHAR(x, y)",
            },
            write={
                "databricks": "TO_CHAR(CAST(x AS TIMESTAMP), y)",
                "drill": "TO_CHAR(CAST(x AS TIMESTAMP), y)",
                "oracle": "TO_CHAR(CAST(x AS TIMESTAMP), y)",
                "postgres": "TO_CHAR(CAST(x AS TIMESTAMP), y)",
                "snowflake": "TO_CHAR(CAST(x AS TIMESTAMP), y)",
                "teradata": "TO_CHAR(CAST(x AS TIMESTAMP), y)",
            },
        )

    def test_regex(self):
        self.validate_all(
            "REGEXP_REPLACE('abcd', 'ab', '')",
            read={
                "presto": "REGEXP_REPLACE('abcd', 'ab', '')",
                "spark": "REGEXP_REPLACE('abcd', 'ab', '')",
            },
        )
        self.validate_all(
            "REGEXP_LIKE(a, 'x')",
            read={
                "duckdb": "REGEXP_MATCHES(a, 'x')",
                "presto": "REGEXP_LIKE(a, 'x')",
                "hive": "a RLIKE 'x'",
                "spark": "a RLIKE 'x'",
            },
        )
        self.validate_all(
            "SPLIT(x, 'a.')",
            read={
                "duckdb": "STR_SPLIT(x, 'a.')",
                "presto": "SPLIT(x, 'a.')",
                "hive": "SPLIT(x, 'a.')",
                "spark": "SPLIT(x, 'a.')",
            },
        )
        self.validate_all(
            "SPLIT(x, 'a.')",
            read={
                "duckdb": "STR_SPLIT_REGEX(x, 'a.')",
                "presto": "REGEXP_SPLIT(x, 'a.')",
            },
        )
        self.validate_all(
            "SIZE(x)",
            read={
                "duckdb": "ARRAY_LENGTH(x)",
                "presto": "CARDINALITY(x)",
                "trino": "CARDINALITY(x)",
                "hive": "SIZE(x)",
                "spark": "SIZE(x)",
            },
        )

        self.validate_all(
            "LOCATE('A', 'ABC')",
            read={
                "trino": "STRPOS('ABC', 'A')",
                "snowflake": "POSITION('A', 'ABC')",
                "presto": "STRPOS('ABC', 'A')",
            },
        )

    def test_parse_filter_array(self):
        self.validate_all(
            "FILTER_ARRAY(the_array, x -> x > 0)",
            write={
                "presto": "FILTER(the_array, x -> x > 0)",
                "hive": "FILTER(the_array, x -> x > 0)",
                "spark": "FILTER(the_array, x -> x > 0)",
                "snowflake": "FILTER(the_array, x -> x > 0)",
            },
        )

        # Test FILTER_ARRAY with positive numbers
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[1, 2, 3, 4, 5], x -> x > 3)",
            read={
                "trino": "SELECT filter(ARRAY[1, 2, 3, 4, 5], x -> x > 3)",
                "snowflake": "SELECT filter(ARRAY_CONSTRUCT(1, 2, 3, 4, 5), x -> x > 3)",
                "databricks": "SELECT filter(ARRAY[1, 2, 3, 4, 5], x -> x > 3)",
            },
        )

        # Test FILTER_ARRAY with negative numbers
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[-5, -4, -3, -2, -1], x -> x <= -3)",
            read={
                "trino": "SELECT filter(ARRAY[-5, -4, -3, -2, -1], x -> x <= -3)",
                "snowflake": "SELECT filter(ARRAY_CONSTRUCT(-5, -4, -3, -2, -1), x -> x <= -3)",
                "databricks": "SELECT filter(ARRAY[-5, -4, -3, -2, -1], x -> x <= -3)",
            },
        )

        # Test FILTER_ARRAY with NULL values
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[NULL, 1, NULL, 2], x -> NOT x IS NULL)",
            read={
                "trino": "SELECT filter(ARRAY[NULL, 1, NULL, 2], x -> x IS NOT NULL)",
                "snowflake": "SELECT filter(ARRAY_CONSTRUCT(NULL, 1, NULL, 2), x -> x IS NOT NULL)",
                "databricks": "SELECT filter(ARRAY[NULL, 1, NULL, 2], x -> x IS NOT NULL)",
            },
        )

        # Test FILTER_ARRAY with complex condition
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[1, 2, 3, 4, 5], x -> MOD(x, 2) = 0)",
            read={
                "trino": "SELECT filter(ARRAY[1, 2, 3, 4, 5], x -> x % 2 = 0)",
                "snowflake": "SELECT filter(ARRAY_CONSTRUCT(1, 2, 3, 4, 5), x -> x % 2 = 0)",
                "databricks": "SELECT filter(ARRAY[1, 2, 3, 4, 5], x -> x % 2 = 0)",
            },
        )

        # Test FILTER_ARRAY with nested arrays
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], x -> SIZE(x) = 2)",
            read={
                "trino": "SELECT filter(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], x -> cardinality(x) = 2)",
                "snowflake": "SELECT filter(ARRAY_CONSTRUCT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4)), x -> ARRAY_SIZE(x) = 2)",
            },
        )

        self.validate_all(
            "SELECT FILTER_ARRAY(the_array, (e, i) -> MOD(i, 2) = 0 AND e > 0)",
            read={
                "databricks": "select filter(the_array, (e, i) -> i % 2 = 0 AND e > 0)",
                "snowflake": "select filter(the_array, (e, i) -> i % 2 = 0 AND e > 0)",
                "trino": "select filter(the_array, (e, i) -> i % 2 = 0 AND e > 0)",
            },
            write={
                "databricks": "SELECT FILTER(the_array, (e, i) -> i % 2 = 0 AND e > 0)",
                "snowflake": "SELECT FILTER(the_array, (e, i) -> i % 2 = 0 AND e > 0)",
                "trino": "SELECT FILTER(the_array, (e, i) -> i % 2 = 0 AND e > 0)",
            },
        )

    def test_group_concat(self):
        self.validate_all(
            "SELECT c_birth_country AS country, LISTAGG(c_first_name, '')",
            read={"snowflake": "SELECT c_birth_country as country, listagg(c_first_name)"},
        )

        self.validate_all(
            "SELECT c_birth_country AS country, LISTAGG(DISTINCT c_first_name, '')",  # We are expecting
            read={"snowflake": "SELECT c_birth_country as country, listagg(distinct c_first_name)"},
        )

        self.validate_all(
            "SELECT c_birth_country AS country, LISTAGG(DISTINCT c_first_name, ', ')",  # We are expecting
            read={
                "snowflake": "SELECT c_birth_country as country, listagg(distinct c_first_name, ', ')"
            },
        )

        self.validate_all(
            "SELECT c_birth_country AS country, LISTAGG(c_first_name, ' | ')",  # We are expecting
            read={"snowflake": "SELECT c_birth_country as country, listagg(c_first_name, ' | ')"},
        )

    def test_named_struct(self):
        self.validate_identity("SELECT NAMED_STRUCT('key_1', 'one', 'key_2', NULL)")

        self.validate_all(
            "NAMED_STRUCT('key_1', 'one', 'key_2', NULL)",
            read={
                "bigquery": "JSON_OBJECT(['key_1', 'key_2'], ['one', NULL])",
                "duckdb": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
            },
            write={
                "bigquery": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
                "duckdb": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
                "snowflake": "OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL)",
            },
        )

    def test_json_extract(self):
        self.validate_identity(
            """SELECT JSON_EXTRACT('{"fruits": [{"apples": 5, "oranges": 10}, {"apples": 2, "oranges": 4}], "vegetables": [{"lettuce": 7, "kale": 8}]}', '$.fruits.apples') AS string_array"""
        )

        self.validate_all(
            """SELECT JSON_EXTRACT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$farm.barn.color')""",
            write={
                "bigquery": """SELECT JSON_EXTRACT_SCALAR('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
                "databricks": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}':farm.barn.color""",
                "duckdb": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}' ->> '$.farm.barn.color'""",
                "postgres": """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm', 'barn', 'color')""",
                "presto": """SELECT JSON_EXTRACT_SCALAR('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
                "redshift": """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm', 'barn', 'color')""",
                "spark": """SELECT GET_JSON_OBJECT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm.barn.color')""",
                "sqlite": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}' ->> '$.farm.barn.color'""",
            },
        )
        #
        self.validate_all(
            """SELECT JSON_VALUE('{"fruits": [{"apples": 5, "oranges": 10}, {"apples": 2, "oranges": 4}], "vegetables": [{"lettuce": 7, "kale": 8}]}', '$.fruits.apples') AS string_array""",
            write={
                "E6": """SELECT JSON_EXTRACT('{"fruits": [{"apples": 5, "oranges": 10}, {"apples": 2, "oranges": 4}], "vegetables": [{"lettuce": 7, "kale": 8}]}', '$.fruits.apples') AS string_array"""
            },
        )

        self.validate_all(
            """SELECT JSON_VALUE('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm')""",
            write={
                "bigquery": """SELECT JSON_EXTRACT_SCALAR('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm')""",
                "databricks": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}':farm""",
                "duckdb": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}' ->> '$.farm'""",
                "postgres": """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm')""",
                "presto": """SELECT JSON_EXTRACT_SCALAR('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm')""",
                "redshift": """SELECT JSON_EXTRACT_PATH_TEXT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', 'farm')""",
                "spark": """SELECT GET_JSON_OBJECT('{ "farm": {"barn": { "color": "red", "feed stocked": true }}}', '$.farm')""",
                "sqlite": """SELECT '{ "farm": {"barn": { "color": "red", "feed stocked": true }}}' ->> '$.farm'""",
            },
        )

    def test_array_slice(self):
        self.validate_all(
            "SELECT ARRAY_SLICE(A, B, C + B)",
            read={
                "databricks": "SELECT SLICE(A,B,C)",
                "presto": "SELECT SLICE(A,B,C)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_SLICE(A,B,C)",
            write={
                "databricks": "SELECT SLICE(A, B, C - B)",
                "presto": "SELECT SLICE(A, B, C - B)",
                "snowflake": "SELECT ARRAY_SLICE(A, B, C)",
            },
        )

    def test_trim(self):
        self.validate_all(
            "TRIM('a' FROM 'abc')",
            read={
                "bigquery": "TRIM('abc', 'a')",
                "snowflake": "TRIM('abc', 'a')",
                "databricks": "TRIM('a' FROM 'abc')",
            },
            write={
                "bigquery": "TRIM('abc', 'a')",
                "snowflake": "TRIM('abc', 'a')",
                "databricks": "TRIM('a' FROM 'abc')",
            },
        )

        self.validate_all(
            "LTRIM('H', 'Hello World')",
            read={
                "oracle": "LTRIM('Hello World', 'H')",
                "clickhouse": "TRIM(LEADING 'H' FROM 'Hello World')",
                "databricks": "TRIM(LEADING 'H' FROM 'Hello World')",
                "snowflake": "LTRIM('Hello World', 'H')",
                "bigquery": "LTRIM('Hello World', 'H')",
            },
            write={
                "clickhouse": "TRIM(LEADING 'H' FROM 'Hello World')",
                "oracle": "LTRIM('Hello World', 'H')",
                "snowflake": "LTRIM('Hello World', 'H')",
                "bigquery": "LTRIM('Hello World', 'H')",
                "databricks": "LTRIM('H', 'Hello World')",
            },
        )

        self.validate_all(
            "RTRIM('d', 'Hello World')",
            read={
                "clickhouse": "TRIM(TRAILING 'd' FROM 'Hello World')",
                "databricks": "TRIM(TRAILING 'd' FROM 'Hello World')",
                "oracle": "RTRIM('Hello World', 'd')",
                "snowflake": "RTRIM('Hello World', 'd')",
                "bigquery": "RTRIM('Hello World', 'd')",
            },
            write={
                "clickhouse": "TRIM(TRAILING 'd' FROM 'Hello World')",
                "databricks": "RTRIM('d', 'Hello World')",
                "oracle": "RTRIM('Hello World', 'd')",
                "snowflake": "RTRIM('Hello World', 'd')",
                "bigquery": "RTRIM('Hello World', 'd')",
            },
        )

        self.validate_all(
            "TRIM('abcSpark')",
            read={
                "databricks": "TRIM(BOTH from 'abcSpark')",
                "snowflake": "TRIM('abcSpark')",
                "oracle": "TRIM(BOTH from 'abcSpark')",
                "bigquery": "TRIM('abcSpark')",
                "clickhouse": "TRIM(BOTH from 'abcSpark')",
            },
            write={
                "databricks": "TRIM('abcSpark')",
                "snowflake": "TRIM('abcSpark')",
                "oracle": "TRIM('abcSpark')",
                "bigquery": "TRIM('abcSpark')",
                "clickhouse": "TRIM('abcSpark')",
            },
        )

        self.validate_all(
            "TRIM(BOTH trimstr FROM 'abcSpark')",
            read={
                "databricks": "TRIM(BOTH trimstr FROM 'abcSpark')",
                "oracle": "TRIM(BOTH trimstr FROM 'abcSpark')",
                "clickhouse": "TRIM(BOTH trimstr FROM 'abcSpark')",
            },
            write={
                "databricks": "TRIM(BOTH trimstr FROM 'abcSpark')",
                "oracle": "TRIM(BOTH trimstr FROM 'abcSpark')",
                "clickhouse": "TRIM(BOTH trimstr FROM 'abcSpark')",
            },
        )
