from tests.dialects.test_dialect import Validator


class TestE6(Validator):
    maxDiff = None
    dialect = "E6"

    def test_E6(self):
        self.validate_all(
            "SELECT DATETIME(CAST('2022-11-01 09:08:07.321' AS TIMESTAMP), 'America/Los_Angeles')",
            read={
                "trino": "SELECT with_timezone(TIMESTAMP '2022-11-01 09:08:07.321', 'America/Los_Angeles')",
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
            },
        )

        self.validate_all(
            "SHIFTRIGHT(x, 1)",
            read={
                "trino": "bitwise_right_shift(x, 1)",
                "duckdb": "x >> 1",
                "hive": "x >> 1",
                "spark": "SHIFTRIGHT(x, 1)",
            },
        )

        self.validate_all(
            "SELECT ARRAY_CONCAT(ARRAY[1, 2], ARRAY[3, 4])",
            read={"trino": "SELECT CONCAT(ARRAY[1,2], ARRAY[3,4])"},
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

        self.validate_all(
            "SELECT DAYOFWEEKISO('2024-11-09')",
            read={
                "trino": "SELECT day_of_week('2024-11-09')",
            },
        )

        self.validate_all(
            "SELECT FORMAT_DATE('2024-11-09 09:08:07', 'dd-MM-YY')",
            read={"trino": "SELECT format_datetime('2024-11-09 09:08:07', '%d-%m-%y')"},
        )

        self.validate_all(
            "SELECT ARRAY_POSITION(1.9, ARRAY[1, 2, 3, 1.9])",
            read={"trino": "SELECT ARRAY_position(ARRAY[1, 2, 3, 1.9],1.9)"},
        )

        self.validate_all(
            "SELECT SIZE(ARRAY[1, 2, 3])", read={"trino": "SELECT cardinality(ARRAY[1, 2, 3])"}
        )

        self.validate_all(
            "SELECT ARRAY_CONTAINS(ARRAY[1, 2, 3], 2)",
            read={"trino": "SELECT contains(ARRAY[1, 2, 3], 2)"},
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
            "SELECT CONTAINS_SUBSTR('This is sql', 'sql')",
            read={
                "snowflake": "SELECT CONTAINS('This is sql','sql')"
            }
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
        # Test FILTER_ARRAY with positive numbers
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[1, 2, 3, 4, 5], x -> x > 3)",
            read={"trino": "SELECT filter(ARRAY[1, 2, 3, 4, 5], x -> x > 3)"},
        )

        # Test FILTER_ARRAY with negative numbers
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[-5, -4, -3, -2, -1], x -> x <= -3)",
            read={"trino": "SELECT filter(ARRAY[-5, -4, -3, -2, -1], x -> x <= -3)"},
        )

        # Test FILTER_ARRAY with NULL values
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[NULL, 1, NULL, 2], x -> NOT x IS NULL)",
            read={"trino": "SELECT filter(ARRAY[NULL, 1, NULL, 2], x -> x IS NOT NULL)"},
        )

        # Test FILTER_ARRAY with complex condition
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[1, 2, 3, 4, 5], x -> MOD(x, 2) = 0)",
            read={"trino": "SELECT filter(ARRAY[1, 2, 3, 4, 5], x -> x % 2 = 0)"},
        )

        # Test FILTER_ARRAY with nested arrays
        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], x -> SIZE(x) = 2)",
            read={
                "trino": "SELECT filter(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], x -> cardinality(x) = 2)"
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
