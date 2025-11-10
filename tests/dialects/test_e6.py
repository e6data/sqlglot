import os

from tests.dialects.test_dialect import Validator


class TestE6(Validator):
    maxDiff = None
    dialect = "e6"

    def test_E6(self):
        self.validate_all(
            "SELECT DATETIME(CAST('2022-11-01 09:08:07.321' AS TIMESTAMP), 'America/Los_Angeles')",
            read={
                "snowflake": "Select convert_timezone('America/Los_Angeles', '2022-11-01 09:08:07.321' ::TIMESTAMP)",
                "databricks": "Select convert_timezone('America/Los_Angeles', '2022-11-01 09:08:07.321' ::TIMESTAMP)",
            },
        )

        self.validate_all(
            "SELECT CAST(DATETIME(datetime_date_718, 'Asia/Calcutta') AS DATE) IS NOT NULL",
            read={
                "athena": "SELECT cast(datetime_date_718 AT TIME ZONE 'Asia/Calcutta' as date) is not null",
            },
        )

        self.validate_all(
            "NVL(x, y, z)",
            read={
                "spark": "NVL(x,y,z)",
                "snowflake": "NVL(x,y,z)",
            },
            write={
                "snowflake": "NVL(x, y, z)",
                "spark": "NVL(x, y, z)",
            },
        )

        self.validate_all(
            "SELECT REDUCE(ARRAY[1, 2, 3], 0, (acc, x) -> acc + x)",
            read={
                "databricks": "SELECT REDUCE(ARRAY(1, 2, 3), 0, (acc, x) -> acc + x)",
                "snowflake": "SELECT REDUCE(ARRAY(1, 2, 3), 0, (acc, x) -> acc + x)",
                "athena": "SELECT REDUCE(ARRAY(1, 2, 3), 0, (acc, x) -> acc + x)",
            },
        )

        self.validate_all(
            "SELECT ARRAY_CONCAT(ARRAY[1, 2], ARRAY[3, 4])",
            read={
                "snowflake": "SELECT ARRAY_CAT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4))",
            },
        )

        self.validate_all(
            "SELECT TYPEOF('hello')",
            read={
                "databricks": "SELECT TYPEOF('hello');",
                "spark": "SELECT TYPEOF('hello');",
                "spark2": "SELECT TYPEOF('hello');",
                "snowflake": "SELECT TYPEOF('hello');",
            },
        )
        self.validate_all(
            "SELECT ARRAY_INTERSECT(ARRAY[1, 2, 3], ARRAY[1, 3, 3, 5])",
            read={
                "databricks": "SELECT ARRAY_INTERSECT(ARRAY(1, 2, 3), ARRAY(1, 3, 3, 5))",
                "athena": "SELECT ARRAY_INTERSECT(ARRAY(1, 2, 3), ARRAY(1, 3, 3, 5))",
                "trino": "SELECT ARRAY_INTERSECT(ARRAY(1, 2, 3), ARRAY(1, 3, 3, 5))",
                "snowflake": "SELECT ARRAY_INTERSECT(ARRAY(1, 2, 3), ARRAY(1, 3, 3, 5))",
            },
        )

        # Concat in dbr can accept many datatypes of args, but we map it to array_concat if type is of array. So we decided to put it as it is.
        self.validate_all(
            "SELECT CONCAT(TRANSFORM(ARRAY[1, 2], x -> x * 10), ARRAY[30, 40])",
            read={
                "databricks": "SELECT concat(transform(array(1, 2), x -> x * 10), array(30, 40))",
            },
        )

        self.validate_all(
            'SELECT SUM(CASE WHEN week_Day = 7 THEN a END) AS "Saturday"',
            read={"databricks": "SELECT sum(case when week_Day = 7 then a end) as Saturday"},
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
            "MAP[ARRAY[a, c],ARRAY[b, d]]",
            read={
                "clickhouse": "map(a, b, c, d)",
                "duckdb": "MAP([a, c], [b, d])",
                "hive": "MAP(a, b, c, d)",
                "presto": "MAP(ARRAY[a, c], ARRAY[b, d])",
                "spark": "MAP(a, b, c, d)",
            },
        )

        self.validate_all(
            "SELECT MAP['test','-18000']",
            read={
                "trino": "SELECT map(split('test',','), split('-18000',','))",
            },
        )

        self.validate_all(
            "SELECT MAP[ARRAY['test'],ARRAY['-18000']]",
            read={
                "databricks": "SELECT map(split('test',','), split('-18000',','))",
            },
        )

        self.validate_all(
            "SELECT TO_TIMESTAMP('2024-11-09', 'dd-MM-YY')",
            read={"trino": "SELECT date_parse('2024-11-09', '%d-%m-%y')"},
        )

        self.validate_all(
            "SELECT LATERAL VIEW EXPLODE(a)", read={"databricks": "SELECT LATERAL VIEW EXPLODE(a)"}
        )

        self.validate_all(
            "SELECT LATERAL VIEW EXPLODE(a) t2 AS date_column",
            read={"databricks": "SELECT LATERAL VIEW EXPLODE(a) t2 AS date_column"},
        )

        self.validate_identity("SELECT a FROM tab WHERE a != 5")

        self.validate_all("SELECT DAYS('2024-11-09')", read={"trino": "SELECT DAYS('2024-11-09')"})

        self.validate_all(
            "SELECT LAST_DAY(CAST('2024-11-09' AS TIMESTAMP))",
            read={"trino": "SELECT LAST_DAY_OF_MONTH(CAST('2024-11-09' AS TIMESTAMP))"},
        )

        self.validate_identity("SELECT LAST_DAY(CAST('2024-11-09' AS TIMESTAMP), UNIT)")

        self.validate_identity("SELECT A IS NOT NULL")

        self.validate_all(
            "SELECT A IS NOT NULL",
            read={
                "trino": "SELECT A IS NOT NULL",
                "snowflake": "SELECT A IS NOT NULL",
                "databricks": "SELECT A IS NOT NULL",
            },
        )

        self.validate_all(
            "CAST(A AS VARCHAR)",
            read={
                "snowflake": "AS_VARCHAR(A)",
            },
        )

        # Snippet taken from an Airmeet query
        self.validate_all(
            "CAST(JSON_EXTRACT(f.value, '$.value') AS VARCHAR)",
            read={"snowflake": "as_varchar(f.value : value)"},
        )

        self.validate_all(
            "COALESCE(CAST(discount_percentage AS VARCHAR), '0%')",
            read={
                "snowflake": "COALESCE(AS_VARCHAR(discount_percentage), '0%')",
            },
        )

        self.validate_all(
            "SELECT DAYOFWEEKISO('2024-11-09')",
            read={
                "trino": "SELECT day_of_week('2024-11-09')",
            },
        )

        self.validate_all(
            "SELECT EXTRACT(DOY FROM CAST('2024-08-09' AS TIMESTAMP))",
            read={
                "databricks": "SELECT dayofyear('2024-08-09')",
            },
        )

        self.validate_all(
            "SELECT EXTRACT(DOY FROM CAST('2024-08-09' AS TIMESTAMP))",
            read={
                "databricks": "SELECT DAY_OF_YEAR('2024-08-09')",
            },
        )

        self.validate_all(
            "SELECT CURRENT_DATE",
            read={
                "databricks": "select curdate()",
            },
        )

        self.validate_all(
            "SELECT CURRENT_TIMESTAMP",
            read={
                "databricks": "SELECT CURRENT_TIMESTAMP()",
                "snowflake": "select current_timestamp()",
            },
        )

        self.validate_all(
            "POSITION(needle in haystack from c)",
            write={
                "spark": "LOCATE(needle, haystack, c)",
                "clickhouse": "POSITION(haystack, needle, c)",
                "snowflake": "CHARINDEX(needle, haystack, c)",
                "mysql": "LOCATE(needle, haystack, c)",
            },
        )

        # check it onece
        # self.validate_all(
        #     "SELECT FORMAT_DATE('2024-11-09 09:08:07', 'dd-MM-YY')",
        #     read={"trino": "SELECT format_datetime('2024-11-09 09:08:07', '%d-%m-%y')"},
        # )
        self.validate_all(
            "SELECT FORMAT_DATETIME(CAST('2025-07-21 15:30:00' AS TIMESTAMP), '%Y-%m-%d')",
            read={
                "trino": "SELECT FORMAT_DATETIME(TIMESTAMP '2025-07-21 15:30:00', '%Y-%m-%d')",
                "athena": "SELECT FORMAT_DATETIME(TIMESTAMP '2025-07-21 15:30:00', '%Y-%m-%d')",
            },
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
            "SELECT SIZE(TRANSFORM(ARRAY[1, 2, 3], x -> x * 2))",
            read={
                "databricks": "SELECT ARRAY_SIZE(transform(array(1, 2, 3), x -> x * 2))",
                "athena": "SELECT ARRAY_SIZE(transform(array(1, 2, 3), x -> x * 2))",
                "snowflake": "SELECT ARRAY_SIZE(transform(array(1, 2, 3), x -> x * 2))",
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

        self.validate_all(
            "SELECT ARRAY_CONTAINS(ARRAY[100, 200, 300], 200)",
            read={
                "databricks": "SELECT array_contains(array(100, 200, 300), 200)",
                "snowflake": "SELECT array_contains(200, array_construct(100, 200, 300))",
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
            "SELECT APPROX_COUNT_DISTINCT(col1) FILTER(WHERE col2 = 10) FROM (VALUES (1, 10), (1, 10), (2, 10), (2, 10), (3, 10), (1, 12)) AS tab(col1, col2)",
            read={
                "databricks": "SELECT APPROX_COUNT_DISTINCT(col1) FILTER(WHERE col2 = 10) FROM (VALUES (1, 10), (1, 10), (2, 10), (2, 10), (3, 10), (1, 12)) AS tab(col1, col2)",
            },
        )

        self.validate_all(
            "SELECT LATERAL VIEW EXPLODE(input => mv.content) AS f",
            read={"snowflake": "select lateral flatten(input => mv.content) f"},
        )

        self.validate_all(
            "SELECT LOCATE('ehe', 'hahahahehehe')",
            read={"trino": "SELECT STRPOS('hahahahehehe','ehe')"},
        )

        # we are not writing test for databricks below here because implementation of function is such that it is based on from_dialect
        # and in the test below we do not send from dialect as arg which would result in wrong or failed test.
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
            "SELECT CURRENT_TIMESTAMP",
            read={
                "databricks": "select GETDATE()",
            },
        )

        self.validate_all(
            "SELECT DATE_DIFF('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
            read={
                "trino": "SELECT date_diff('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
                "snowflake": "SELECT DATEDIFF(DAY, CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
                "presto": "SELECT date_diff('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
                "databricks": "SELECT DATEDIFF(DAY, CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
            },
            write={
                "e6": "SELECT DATE_DIFF('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))"
            },
        )

        self.validate_all(
            "SELECT DATE_DIFF('DAY', CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
            read={
                "databricks": "SELECT DATEDIFF(SQL_TSI_DAY, CAST('2024-11-11' AS DATE), CAST('2024-11-09' AS DATE))",
            },
        )

        self.validate_all(
            "SELECT TIMESTAMP_ADD('HOUR', 1, CAST('2003-01-02 11:59:59' AS TIMESTAMP))",
            read={
                "databricks": "SELECT TIMESTAMPADD(SQL_TSI_HOUR, 1, TIMESTAMP'2003-01-02 11:59:59')",
            },
        )

        self.validate_all(
            "SELECT WIDTH_BUCKET(5.3, 0.2, 10.6, 5)",
            read={
                "databricks": "SELECT width_bucket(5.3, 0.2, 10.6, 5)",
                "snowflake": "SELECT width_bucket(5.3, 0.2, 10.6, 5)",
                "trino": "SELECT width_bucket(5.3, 0.2, 10.6, 5)",
                "athena": "SELECT width_bucket(5.3, 0.2, 10.6, 5)",
            },
        )

        self.validate_all(
            "SELECT FROM_UNIXTIME(1674797653)",
            read={
                "trino": "SELECT from_unixtime(1674797653)",
            },
        )

        self.validate_all(
            "SELECT FROM_UNIXTIME(unixtime / 1000)",
            read={"trino": "SELECT from_unixtime(unixtime/1000)"},
        )

        self.validate_all(
            "SELECT FROM_UTC_TIMESTAMP(CAST('2016-08-31' AS TIMESTAMP), 'Asia/Seoul')",
            read={
                "databricks": "SELECT FROM_UTC_TIMESTAMP('2016-08-31', 'Asia/Seoul')",
            },
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
                "databricks": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "teradata": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
            },
            write={
                "e6": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "bigquery": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "clickhouse": "SELECT argMax(a.id, a.timestamp) FROM a",
                "duckdb": "SELECT ARG_MAX(a.id, a.timestamp) FROM a",
                "presto": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "snowflake": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "spark": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
                "databricks": "SELECT MAX_BY(a.id, a.timestamp) FROM a",
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
                "databricks": "SELECT ELEMENT_AT(X, 4)",
                "spark": "SELECT ELEMENT_AT(X, 4)",
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
            "ELEMENT_AT(X, 5)",
            read={
                "databricks": "GET(X, 4)",
            },
        )

        self.validate_all(
            "SELECT CASE WHEN SIZE(arr) > 3 THEN TRY_ELEMENT_AT(TRANSFORM(arr, x -> x * 2), -2) ELSE TRY_ELEMENT_AT(arr, 1) END AS resul FROM (VALUES (ARRAY[1, 2, 3, 4]), (ARRAY[10, 20])) AS tab(arr)",
            read={
                "databricks": "SELECT CASE WHEN size(arr) > 3 THEN try_element_at(transform(arr, x -> x * 2), -2) ELSE try_element_at(arr, 1) END AS resul FROM VALUES (array(1, 2, 3, 4)), (array(10, 20)) AS tab(arr)",
            },
        )

        self.validate_all(
            "SELECT FILTER_ARRAY(ARRAY[1, 2, 3, 4], x -> TRY_ELEMENT_AT(ARRAY[TRUE, FALSE, TRUE], x) = TRUE) AS filtered",
            read={
                "databricks": "SELECT filter(array(1, 2, 3, 4), x -> try_element_at(array(true, false, true), x) = true) AS filtered",
            },
        )

        self.validate_all(
            "SELECT TRY_ELEMENT_AT(f.CustomTargeting, 'kpeid')",
            read={
                "databricks": "SELECT TRY_ELEMENT_AT(f.CustomTargeting, 'kpeid')",
            },
        )

        self.validate_all(
            'SELECT X."B"',
            read={
                "snowflake": "SELECT X['B']",
                "trino": "SELECT X['B']",
                "databricks": "SELECT X['B']",
                "spark": "SELECT X['B']",
                "duckdb": "SELECT X['B']",
            },
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
                "presto": "JSON_FORMAT(CAST(x AS JSON))",
                "spark": "TO_JSON(x)",
            },
        )

        self.validate_all(
            "SELECT JSON_EXTRACT(c1, '$.item[1].price')",
            read={"databricks": "SELECT GET_JSON_OBJECT(c1, '$.item[1].price')"},
        )
        self.validate_all(
            "SELECT JSON_EXTRACT(c1, '$.box[1].price')",
            read={"databricks": "SELECT c1:box[1].price"},
        )

        self.validate_all(
            "SELECT meta:bincounttaskmeta FROM silver_mongo.tms.tasks",
            read={"databricks": "SELECT meta:bincounttaskmeta FROM silver_mongo.tms.tasks "},
        )

        self.validate_all(
            'SELECT CAST(STAFF:is_active AS BOOLEAN) AS "staff_is_active" FROM silver_mongo.tms.staffs LIMIT 100',
            read={
                "databricks": "SELECT STAFF:is_active::boolean AS `staff_is_active` FROM silver_mongo.tms.staffs LIMIT 100"
            },
        )

        self.validate_all(
            "TO_JSON(X)",
            read={
                "presto": "JSON_FORMAT(CAST(X as JSON))",
            },
        )
        self.validate_all(
            "SELECT FORMAT('%s%%', 123)",
            read={
                "presto": "SELECT FORMAT('%s%%', 123)",
                "trino": "SELECT FORMAT('%s%%', 123)",
            },
        )

        self.validate_all(
            "SELECT EXTRACT(FIELDSTR FROM date_expr)",
            read={
                "databricks": "SELECT DATE_PART(FIELDSTR, date_expr)",
                "e6": "SELECT EXTRACT(FIELDSTR FROM date_expr)",
            },
        )

        self.validate_all(
            "SELECT A IS NOT NULL",
            read={"databricks": "SELECT ISNOTNULL(A)"},
            write={"databricks": "SELECT NOT A IS NULL"},
        )

        self.validate_all(
            "SELECT EXTRACT(QUARTER FROM CAST('2016-08-31' AS DATE))",
            read={"databricks": "SELECT QUARTER('2016-08-31')"},
        )

        self.validate_all(
            "SELECT MIN(DATE) AS C1 FROM (SELECT DATE FROM cdr_adhoc_analysis.default.gr_3p_demand_ix_revenue WHERE (((mappedflag = 'mapped' AND parent_advertiser_name_clean = 'toronto-dominion bank (td bank group)') AND seller_defined = 'yes') AND COALESCE(YEAR(TO_DATE(DATE)), 0) = 2025) AND COALESCE(EXTRACT(QUARTER FROM CAST(DATE AS DATE)), 0) = 3) AS ITBL",
            read={
                "databricks": "SELECT MIN(DATE) AS C1 FROM (SELECT DATE FROM cdr_adhoc_analysis.default.gr_3p_demand_ix_revenue WHERE (((mappedflag = 'mapped' AND parent_advertiser_name_clean = 'toronto-dominion bank (td bank group)') AND seller_defined = 'yes') AND COALESCE(YEAR(DATE), 0) = 2025) AND COALESCE(QUARTER(DATE), 0) = 3) AS ITBL"
            },
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

        self.validate_all(
            "SELECT TIMESTAMP_DIFF(CAST('1900-03-28' AS DATE), CAST('2021-01-01' AS DATE), 'YEAR')",
            read={
                "databricks": "SELECT timestampdiff(SQL_TSI_YEAR, DATE'2021-01-01', DATE'1900-03-28')"
            },
        )

        self.validate_all(
            "SPLIT_PART(attr.RPT_SHORT_DESC, ' ', 1) = CAST(LEFT(dc.div_no, 3) AS BIGINT)",
            read={
                "databricks": "SPLIT_PART(attr.RPT_SHORT_DESC, ' ', 1) = BIGINT(LEFT(dc.div_no, 3))"
            },
        )

        self.validate_all(
            "SELECT CAST('2023-12-25T10:30:00Z' AS timestamp_tz)",
            read={"databricks": "SELECT FROM_ISO8601_TIMESTAMP('2023-12-25T10:30:00Z')"},
        )

        self.validate_all(
            "SELECT CAST(col AS JSON)",
            read={"databricks": "select cast(col as JSON)"},
        )
        for unit in ["SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "YEAR"]:
            self.validate_all(
                f"SELECT TIMESTAMP_DIFF(date1, date2, '{unit}')",
                read={
                    "databricks": f"SELECT TIMEDIFF('{unit}', date1, date2)",
                },
                write={
                    "e6": f"SELECT TIMESTAMP_DIFF(date1, date2, '{unit}')",
                },
            )

            self.validate_all(
                "SELECT TIMESTAMP_DIFF(start1, end1, 'HOUR'), TIMESTAMP_DIFF(start2, end2, 'MINUTE')",
                read={
                    "databricks": "SELECT TIMEDIFF('HOUR', start1, end1), TIMEDIFF('MINUTE', start2, end2)",
                },
                write={
                    "e6": "SELECT TIMESTAMP_DIFF(start1, end1, 'HOUR'), TIMESTAMP_DIFF(start2, end2, 'MINUTE')",
                },
            )

            self.validate_all(
                "SELECT ABS(TIMESTAMP_DIFF(start_time, end_time, 'MINUTE'))",
                read={
                    "databricks": "SELECT ABS(TIMEDIFF('MINUTE', start_time, end_time))",
                },
                write={
                    "e6": "SELECT ABS(TIMESTAMP_DIFF(start_time, end_time, 'MINUTE'))",
                },
            )
            self.validate_all(
                "SELECT AVG(TIMESTAMP_DIFF(start_time, end_time, 'HOUR')) FROM sessions",
                read={
                    "databricks": "SELECT AVG(TIMEDIFF('HOUR', start_time, end_time)) FROM sessions",
                },
                write={
                    "e6": "SELECT AVG(TIMESTAMP_DIFF(start_time, end_time, 'HOUR')) FROM sessions",
                },
            )

        self.validate_all(
            "SELECT CORR(c1, c2) FROM (VALUES (3, 2), (3, 3), (3, 3), (6, 4)) AS tab(c1, c2)",
            read={
                "databricks": "SELECT corr(c1, c2) FROM VALUES (3, 2), (3, 3), (3, 3), (6, 4) as tab(c1, c2)"
            },
        )

        self.validate_all(
            "SELECT COVAR_POP(c1, c2) FROM (VALUES (1, 1), (2, 2), (2, 2), (3, 3)) AS tab(c1, c2)",
            read={
                "databricks": "SELECT covar_pop(c1, c2) FROM VALUES (1, 1), (2, 2), (2, 2), (3, 3) AS tab(c1, c2)"
            },
        )

        self.validate_all(
            "SELECT URL_DECODE('http%3A%2F%2Fspark.apache.org%2Fpath%3Fquery%3D1')",
            read={
                "databricks": "SELECT URL_DECODE('http%3A%2F%2Fspark.apache.org%2Fpath%3Fquery%3D1')",
                "athena": "SELECT URL_DECODE('http%3A%2F%2Fspark.apache.org%2Fpath%3Fquery%3D1')",
                "trino": "SELECT URL_DECODE('http%3A%2F%2Fspark.apache.org%2Fpath%3Fquery%3D1')",
            },
        )

        # FIND_IN_SET function tests - Databricks to E6 transpilation
        self.validate_all(
            "SELECT ARRAY_POSITION('ab', SPLIT('abc,b,ab,c,def', ','))",
            read={
                "databricks": "SELECT FIND_IN_SET('ab', 'abc,b,ab,c,def')",
            },
        )

        self.validate_all(
            "SELECT ARRAY_POSITION('test', SPLIT('hello,world,test', ','))",
            read={
                "databricks": "SELECT FIND_IN_SET('test', 'hello,world,test')",
            },
        )

        # Test FIND_IN_SET with column references
        self.validate_all(
            "SELECT ARRAY_POSITION(search_col, SPLIT(list_col, ',')) FROM table1",
            read={
                "databricks": "SELECT FIND_IN_SET(search_col, list_col) FROM table1",
            },
        )

    def test_regex(self):
        self.validate_all(
            "REGEXP_REPLACE('abcd', 'ab', '')",
            read={
                "presto": "REGEXP_REPLACE('abcd', 'ab', '')",
                "spark": "REGEXP_REPLACE('abcd', 'ab', '')",
                "databricks": "REGEXP_REPLACE('abcd', 'ab', '')",
                "postgres": "REGEXP_REPLACE('abcd', 'ab', '')",
                "duckdb": "REGEXP_REPLACE('abcd', 'ab', '')",
                "snowflake": "REGEXP_REPLACE('abcd', 'ab', '')",
            },
            write={
                "presto": "REGEXP_REPLACE('abcd', 'ab', '')",
                "spark": "REGEXP_REPLACE('abcd', 'ab', '')",
                "postgres": "REGEXP_REPLACE('abcd', 'ab', '')",
                "duckdb": "REGEXP_REPLACE('abcd', 'ab', '')",
                "snowflake": "REGEXP_REPLACE('abcd', 'ab', '')",
                "databricks": "REGEXP_REPLACE('abcd', 'ab', '')",
            },
        )

        self.validate_all(
            "SELECT REGEXP_REPLACE('100-200', pattern, 'num', 4)",
            read={
                "databricks": "SELECT REGEXP_REPLACE('100-200', pattern, 'num', 4)",
            },
        )

        self.validate_all(
            "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
            read={
                "databricks": "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
                "snowflake": "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
                "trino": "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
            },
            write={
                "databricks": "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
                "snowflake": "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
                "trino": "SELECT REGEXP_COUNT('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en')",
            },
        )

        self.validate_all(
            "SELECT REGEXP_COUNT(a, '[[:punct:]][[:alnum:]]+[[:punct:]]', 1, 'i')",
            read={
                "databricks": "SELECT REGEXP_COUNT(a, '[[:punct:]][[:alnum:]]+[[:punct:]]', 1, 'i')",
            },
        )

        self.validate_all(
            "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
            read={
                "bigquery": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "trino": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "presto": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "snowflake": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "duckdb": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "spark": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "databricks": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
            },
            write={
                "bigquery": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "trino": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "presto": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "snowflake": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "duckdb": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]', 0)",
                "spark": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
                "databricks": "REGEXP_EXTRACT_ALL('a1_a2a3_a4A5a6', 'a[0-9]')",
            },
        )

        self.validate_all(
            "REGEXP_EXTRACT('abc', '(a)(b)(c)', 1)",
            read={
                "hive": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark2": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "databricks": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
            },
            write={
                "hive": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark2": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "spark": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "databricks": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "presto": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "trino": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
                "duckdb": "REGEXP_EXTRACT('abc', '(a)(b)(c)')",
            },
        )

        self.validate_all(
            "REGEXP_LIKE(a, 'x')",
            read={
                "duckdb": "REGEXP_MATCHES(a, 'x')",
                "presto": "REGEXP_LIKE(a, 'x')",
                "hive": "a RLIKE 'x'",
                "spark": "a RLIKE 'x'",
                "databricks": "a RLIKE 'x'",
                "bigquery": "REGEXP_CONTAINS(a, 'x')",
            },
            write={
                "spark": "a RLIKE 'x'",
                "databricks": "a RLIKE 'x'",
                "duckdb": "REGEXP_MATCHES(a, 'x')",
                "presto": "REGEXP_LIKE(a, 'x')",
                "bigquery": "REGEXP_CONTAINS(a, 'x')",
            },
        )

        self.validate_all(
            "SELECT FROM gold_us_prod.content.gld_cross_brand_live WHERE affiliate_links_count >= 5 AND REGEXP_LIKE(full_url, 'gift') AND REGEXP_REPLACE(s.page, '^(.*?)$', '$1') = CONCAT('https://', full_url)",
            read={
                "databricks": "SELECT FROM gold_us_prod.content.gld_cross_brand_live WHERE affiliate_links_count >= 5 AND full_url RLIKE 'gift' AND regexp_replace(s.page, '^(.*?)$', '$1') = CONCAT('https://', full_url)",
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
            "SELECT FILTER_ARRAY(ARRAY[NULL, 1, NULL, 2], x -> x IS NOT NULL)",
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
                "snowflake": "OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL)",
                "databricks": "struct ('one' as key_1, NULL as key_2)",
            },
            write={
                "bigquery": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
                "duckdb": "JSON_OBJECT('key_1', 'one', 'key_2', NULL)",
                "snowflake": "OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL)",
            },
        )

        self.validate_all(
            "SELECT a.*, TO_JSON(ARRAY[NAMED_STRUCT('x', x_start, 'y', y_start), NAMED_STRUCT('x', x_end, 'y', y_start), NAMED_STRUCT('x', x_end, 'y', y_end), NAMED_STRUCT('x', x_start, 'y', y_end)]) AS geometry",
            read={
                "databricks": "select a.*, to_json(array(struct (x_start as x, y_start as y),struct (x_end as x, y_start as y),struct (x_end as x, y_end as y),struct (x_start as x, y_end as y))) as geometry",
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
                "e6": """SELECT JSON_EXTRACT('{"fruits": [{"apples": 5, "oranges": 10}, {"apples": 2, "oranges": 4}], "vegetables": [{"lettuce": 7, "kale": 8}]}', '$.fruits.apples') AS string_array"""
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

        self.validate_all(
            "SELECT ARRAY_SLICE(ARRAY['a', 'b', 'c', 'd', 'e'], -3, 5 + -3) AS result",
            read={"databricks": "SELECT slice(array('a', 'b', 'c', 'd', 'e'), -3, 5) AS result"},
        )

        self.validate_all(
            "SELECT ARRAY_SLICE(TRANSFORM(SEQUENCE(1, 6), x -> x * 2), 2, 3 + 2)",
            read={
                "databricks": "SELECT slice(transform(sequence(1, 6), x -> x * 2), 2, 3)",
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

        self.validate_all(
            "SELECT EXTRACT(DAY FROM CAST('2025-04-08' AS DATE))",
            read={
                "snowflake": "SELECT DATE_PART(day, '2025-04-08'::DATE)",
                "databricks": "SELECT date_part('day', DATE'2025-04-08')",
            },
        )

    def test_aggregate(self):
        self.validate_all(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col) FROM (VALUES (1), (2), (2), (3), (4), (NULL)) AS tab(col)",
            read={
                "databricks": "SELECT median(col) FROM VALUES (1), (2), (2), (3), (4), (NULL) AS tab(col)"
            },
        )

        self.validate_all(
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN cc.cost_type = 'relative' THEN (cd.'value' * rt.'value') ELSE cd.'value' END)",
            read={
                "databricks": "MEDIAN(CASE WHEN cc.cost_type = 'relative' THEN (cd.'value' * rt.'value') ELSE cd.'value' END)"
            },
        )

        self.validate_all(
            "SELECT MEDIAN(DISTINCT col) FROM (VALUES (1), (2), (2), (3), (4), (NULL)) AS tab(col)",
            read={
                "databricks": "SELECT median(DISTINCT col) FROM VALUES (1), (2), (2), (3), (4), (NULL) AS tab(col)"
            },
        )

        self.validate_all(
            """ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cd."value" * CASE WHEN cc.cost_type = 'relative' THEN 100 ELSE 1 END), 6)""",
            read={
                "databricks": "ROUND(MEDIAN(cd.`value` * CASE WHEN cc.cost_type = 'relative' THEN 100 ELSE 1 END), 6)"
            },
        )

        self.validate_all(
            """ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cd."value" * CASE WHEN COUNT(DISTINCT (col)) > 10 THEN 100 ELSE 1 END), 6)""",
            read={
                "databricks": "ROUND(MEDIAN(cd.`value` * CASE WHEN count(distinct(col)) > 10 THEN 100 ELSE 1 END), 6)"
            },
        )

        self.validate_all(
            "SELECT AVG(DISTINCT col) FROM (VALUES (1), (1), (2)) AS tab(col)",
            read={"databricks": "SELECT avg(DISTINCT col) FROM VALUES (1), (1), (2) AS tab(col);"},
        )

        self.validate_all(
            "SELECT MIN(colA) FROM table1",
            read={"databricks": "select min(colA) from table1"},
            write={"databricks": "SELECT MIN(colA) FROM table1"},
        )

        self.validate_all(
            "SELECT COUNT(DISTINCT colA, colB) FROM table1",
            read={"databricks": "SELECT count(distinct colA, colB) FROM table1"},
            write={"databricks": "SELECT COUNT(DISTINCT colA, colB) FROM table1"},
        )

        self.validate_all(
            "SELECT MAX(col) FROM table1", read={"databricks": "SELECT max(col) FROM table1"}
        )

        self.validate_all(
            "SELECT DISTINCT (colA) FROM table1",
            read={"databricks": "select distinct(colA) from table1"},
        )

        self.validate_all(
            "SELECT SUM(colA) FROM table1", read={"databricks": "select sum(colA) from table1;"}
        )

        self.validate_all(
            "SELECT ARBITRARY(colA) FROM table1",
            read={"databricks": "select arbitrary(colA) from table1;"},
        )

        self.validate_all(
            "SELECT ARBITRARY(col) IGNORE NULLS FROM (VALUES (NULL), (5), (20)) AS tab(col)",
            read={
                "databricks": "SELECT any_value(col) IGNORE NULLS FROM VALUES (NULL), (5), (20) AS tab(col)"
            },
        )

        self.validate_all(
            "SELECT department, LISTAGG(employee_name, ', ') FROM employees GROUP BY department",
            read={
                "postgres": "SELECT department, STRING_AGG(employee_name, ', ') FROM employees GROUP BY department"
            },
        )

    def test_math(self):
        self.validate_all("SELECT CEIL(5.4)", read={"databricks": "SELECT ceil(5.4)"})

        self.validate_all("SELECT CEIL(3345.1, -2)", read={"databricks": "SELECT ceil(3345.1, -2)"})
        self.validate_all("SELECT FLOOR(5.4)", read={"databricks": "SELECT floor(5.4)"})

        self.validate_all(
            "SELECT FLOOR(3345.1, -2)", read={"databricks": "SELECT floor(3345.1, -2)"}
        )

        self.validate_all(
            """SELECT transaction_id, amount, transaction_date, CEIL(MONTH(TO_DATE(transaction_date)) / 3.0) AS qtr, CEIL(amount / 1000) * 1000 AS amount_rounded_up, CASE WHEN CEIL(amount / 1000) * 1000 > 10000 THEN 'Large' WHEN CEIL(amount / 1000) * 1000 > 5000 THEN 'Medium' ELSE 'Small' END AS transaction_size FROM financial_transactions WHERE YEAR(TO_DATE(transaction_date)) = 2023""",
            read={
                "databricks": "SELECT transaction_id, amount, transaction_date, CEIL(MONTH(transaction_date)/3.0) AS "
                "qtr, CEIL(amount/1000) * 1000 AS amount_rounded_up, CASE WHEN CEIL(amount/1000) * "
                "1000 > 10000 THEN 'Large' WHEN CEIL(amount/1000) * 1000 > 5000 THEN 'Medium' ELSE "
                "'Small' END AS transaction_size FROM financial_transactions WHERE YEAR("
                "transaction_date) = 2023"
            },
        )

        self.validate_all("SELECT ROUND(5.678)", read={"databricks": "select round(5.678)"})

        self.validate_all("SELECT ROUND(5.678, 2)", read={"databricks": "select round(5.678, 2)"})

        self.validate_all(
            "SELECT account_id, transaction_type, ROUND(SUM(amount), 2) AS total_amount, ROUND(AVG(amount), "
            "2) AS avg_amount, ROUND(SUM(amount) * CASE WHEN currency = 'EUR' THEN 1.08 WHEN currency = 'GBP' THEN "
            "1.23 ELSE 1.0 END, 2) AS usd_equivalent, ROUND(SUM(amount) / NULLIF(COUNT(DISTINCT MONTH(TO_DATE("
            "transaction_date))), 0), 2) AS monthly_avg FROM transactions WHERE YEAR(TO_DATE(transaction_date)) = "
            "2023 GROUP BY account_id, transaction_type, currency HAVING ROUND(SUM(amount), 2) > 1000",
            read={
                "databricks": "SELECT account_id, transaction_type, ROUND(SUM(amount), 2) AS total_amount, ROUND(AVG("
                "amount), 2) AS avg_amount, ROUND(SUM(amount) * CASE WHEN currency = 'EUR' THEN 1.08 "
                "WHEN currency = 'GBP' THEN 1.23 ELSE 1.0 END, 2) AS usd_equivalent, ROUND(SUM(amount) "
                "/ NULLIF(COUNT(DISTINCT MONTH(transaction_date)), 0), 2) AS monthly_avg FROM "
                "transactions WHERE YEAR(transaction_date) = 2023 GROUP BY account_id, "
                "transaction_type, currency HAVING ROUND(SUM(amount), 2) > 1000"
            },
        )

        self.validate_all(
            "CASE WHEN prev_mrr IS NOT NULL THEN ABS(mrr - prev_mrr) ELSE mrr END AS mrr_diff",
            read={
                "databricks": "CASE WHEN prev_mrr is not null then abs(mrr - prev_mrr) ELSE mrr END as mrr_diff"
            },
        )

        self.validate_all(
            "SELECT customer_id, COUNT(*) AS invoices, ROUND(AVG(ABS(DATE_DIFF('DAY', payment_date, due_date))), "
            "1) AS avg_days_deviation, ROUND(STDDEV(ABS(DATE_DIFF('DAY', payment_date, due_date))), "
            "1) AS stddev_days_deviation, SUM(CASE WHEN payment_date > due_date THEN 1 ELSE 0 END) AS late_payments, "
            "SUM(CASE WHEN payment_date < due_date THEN 1 ELSE 0 END) AS early_payments, ROUND(100.0 * SUM(CASE WHEN "
            "payment_date > due_date THEN 1 ELSE 0 END) / COUNT(*), 1) AS late_payment_rate FROM invoices WHERE "
            "payment_date IS NOT NULL AND YEAR(TO_DATE(TO_DATE(due_date))) = 2023 GROUP BY customer_id HAVING COUNT("
            "*) > 5 ORDER BY avg_days_deviation DESC",
            read={
                "databricks": """SELECT customer_id, COUNT(*) AS invoices, ROUND(AVG(ABS(DATE_DIFF('DAY', 
                payment_date, due_date))), 1) AS avg_days_deviation, ROUND(STDDEV(ABS(DATE_DIFF('DAY', payment_date, 
                due_date))), 1) AS stddev_days_deviation, SUM(CASE WHEN payment_date > due_date THEN 1 ELSE 0 END) AS 
                late_payments, SUM(CASE WHEN payment_date < due_date THEN 1 ELSE 0 END) AS early_payments, 
                ROUND(100.0 * SUM(CASE WHEN payment_date > due_date THEN 1 ELSE 0 END) / COUNT(*), 
                1) AS late_payment_rate FROM invoices WHERE payment_date IS NOT NULL AND YEAR(TO_DATE(due_date)) = 
                2023 GROUP BY customer_id HAVING COUNT(*) > 5 ORDER BY avg_days_deviation DESC"""
            },
        )

        self.validate_all(
            "CASE WHEN SIGN(actual_value - predicted_value) = SIGN(actual_value - LAG(predicted_value) OVER ("
            "PARTITION BY model_id ORDER BY forecast_date)) THEN 1 ELSE 0 END",
            read={
                "databricks": """ CASE WHEN SIGN(actual_value - predicted_value) = SIGN(actual_value - 
                LAG(predicted_value) OVER (PARTITION BY model_id ORDER BY forecast_date)) THEN 1 ELSE 0 END """
            },
        )

        # self.validate_all(
        #     "SELECT SIGN(INTERVAL -1 DAY)", read={"databricks": "SELECT sign(INTERVAL'-1' DAY)"}
        # )

        self.validate_all(
            "SELECT CURRENT_TIMESTAMP + INTERVAL '1 WEEK' + INTERVAL '2 HOUR'",
            read={
                "databricks": "SELECT CURRENT_TIMESTAMP + INTERVAL '1 week 2 hours'",
            },
        )

        self.validate_all(
            "INTERVAL '5 MINUTE' + INTERVAL '30 SECOND' + INTERVAL '500 MILLISECOND'",
            read={"databricks": "INTERVAL '5 minutes 30 seconds 500 milliseconds'"},
        )

        self.validate_all("SELECT MOD(2, 1.8)", read={"databricks": "SELECT mod(2, 1.8)"})

        self.validate_all("SELECT MOD(2, 1.8)", read={"databricks": "SELECT 2 % 1.8"})

        self.validate_all(
            "ROUND(SQRT(AVG(POWER(actual_value - predicted_value, 2))), 2)",
            read={"databricks": "ROUND(SQRT(AVG(POWER(actual_value - predicted_value, 2))), 2)"},
        )

        self.validate_all("SELECT POWER(2, 3)", read={"databricks": "SELECT pow(2, 3)"})

        self.validate_all(
            "SELECT c1, FACTORIAL(c1) FROM RANGE(-1, 22) AS t(c1)",
            read={"databricks": "SELECT c1, factorial(c1) FROM range(-1, 22) AS t(c1)"},
        )

        self.validate_all("SELECT CBRT(27.0)", read={"databricks": "SELECT cbrt(27.0)"})

        self.validate_all("SELECT EXP(0)", read={"databricks": "SELECT exp(0)"})

        self.validate_all("SELECT SIN(0)", read={"databricks": "SELECT sin(0)"})

        self.validate_all("SELECT SINH(0)", read={"databricks": "SELECT sinh(0)"})

        self.validate_all("SELECT COS(PI())", read={"databricks": "SELECT cos(pi())"})

        self.validate_all("SELECT COSH(0)", read={"databricks": "SELECT cosh(0)"})

        self.validate_all("SELECT ACOSH(0)", read={"databricks": "SELECT acosh(0)"})

        self.validate_all("SELECT TAN(0)", read={"databricks": "SELECT tan(0)"})

        self.validate_all("SELECT TANH(0)", read={"databricks": "SELECT tanh(0)"})

        self.validate_all("SELECT COT(1)", read={"databricks": "SELECT cot(1)"})

        self.validate_all("SELECT DEGREES(1)", read={"databricks": "select degrees(1)"})

        self.validate_all("SELECT RADIANS(1)", read={"databricks": "SELECT radians(1)"})

        self.validate_all("SELECT PI()", read={"databricks": "SELECT pi()"})

        self.validate_all("SELECT LN(1)", read={"databricks": "SELECT ln(1)"})

    def test_string(self):
        self.validate_all(
            "SELECT '%SystemDrive%/Users/John' LIKE '/%SystemDrive/%//Users%' ESCAPE '/'",
            read={
                "databricks": "SELECT '%SystemDrive%/Users/John' like '/%SystemDrive/%//Users%' ESCAPE '/'"
            },
        )

        # In the following test case when argument "Some" is provided in databricks, space is removed after "Some" and it is treated as function
        self.validate_all(
            "SELECT 'Spark' LIKE SOME ('_park', '_ock')",
            read={"databricks": "SELECT 'Spark' LIKE SOME ('_park', '_ock')"},
        )

        self.validate_all(
            """CASE WHEN LOWER(product_name) LIKE '%premium%' OR LOWER(info) LIKE '%premium%' THEN 1 ELSE 0 END""",
            read={
                "databricks": "CASE WHEN LOWER(product_name) LIKE '%premium%' OR LOWER(info) LIKE '%premium%' THEN 1 ELSE 0 END"
            },
        )

        self.validate_all(
            "SELECT ILIKE('Spark', '_PARK')", read={"databricks": "SELECT ilike('Spark', '_PARK')"}
        )

        self.validate_all(
            "SELECT REGEXP_LIKE('%SystemDrive%John', '%SystemDrive%.*')",
            read={"databricks": """SELECT r'%SystemDrive%John' rlike r'%SystemDrive%.*'"""},
        )

        # Getting Assertion Error because of "\"
        # self.validate_all(
        #     """CASE WHEN REGEXP_LIKE(url, '(?i)/products/\\d+') THEN 'Product Page' ELSE 'Other Page' END AS page_type""",
        #     read={
        #         'databricks': """CASE WHEN url RLIKE '(?i)/products/\\d+' THEN 'Product Page' ELSE 'Other Page' END AS page_type"""
        #     }
        # )

        self.validate_all(
            "SELECT LENGTH('Spark SQL ')", read={"databricks": "SELECT length('Spark SQL ')"}
        )

        self.validate_all(
            "SELECT LENGTH('Spark SQL ')", read={"databricks": "SELECT len('Spark SQL ')"}
        )

        self.validate_all(
            "SELECT LENGTH('Spark SQL ')",
            read={"databricks": "SELECT character_length('Spark SQL ')"},
        )

        self.validate_all(
            "SELECT LENGTH('Spark SQL ')", read={"databricks": "SELECT char_length('Spark SQL ')"}
        )

        self.validate_all(
            "SELECT REPLACE('ABCabc' COLLATE UTF8_LCASE, 'abc', 'DEF')",
            read={"databricks": "SELECT replace('ABCabc' COLLATE UTF8_LCASE, 'abc', 'DEF')"},
        )

        self.validate_all(
            "CASE WHEN REPLACE(LOWER(table1), 'fact_', '') != table1 THEN ' WHERE event_date >= DATE_SUB(CURRENT_DATE(), 365)' WHEN REPLACE(LOWER(table1), 'dim_', '') != table1 THEN ' WHERE is_active = TRUE' ELSE '' END",
            read={
                "databricks": "CASE WHEN REPLACE(LCASE(table1), 'fact_', '') != table1 THEN ' WHERE event_date >= DATE_SUB(CURRENT_DATE(), 365)' WHEN REPLACE(LOWER(table1), 'dim_', '') != table1 THEN ' WHERE is_active = TRUE' ELSE '' END"
            },
        )

        self.validate_all(
            "SELECT product_id, UPPER(product_name) AS product_name_upper FROM products WHERE UPPER(product_name) LIKE UPPER('%laptop%')",
            read={
                "databricks": "SELECT product_id, UCASE(product_name) AS product_name_upper FROM products WHERE UCASE(product_name) LIKE UPPER('%laptop%')"
            },
        )

        self.validate_all(
            "SELECT SUBSTRING('Spark SQL', 5, 1)",
            read={"databricks": "SELECT substring('Spark SQL' FROM 5 FOR 1)"},
        )

        self.validate_all(
            "SELECT SUBSTRING('Spark SQL', 5, 1)",
            read={"databricks": "SELECT substring('Spark SQL', 5, 1)"},
        )

        self.validate_all(
            """SELECT email, SUBSTRING(email, LOCATE('@', email) + 1) AS dmn FROM users""",
            read={
                "databricks": "SELECT email, substring(email, locate('@', email) + 1) AS dmn FROM users"
            },
        )

        self.validate_all(
            """SELECT email, SUBSTRING(email, LOCATE('@', email) + 1) AS dmn FROM users""",
            read={
                "databricks": "SELECT email, substr(email, locate('@', email) + 1) AS dmn FROM users"
            },
        )

        self.validate_all(
            "SELECT INITCAP('sPark sql')", read={"databricks": "SELECT initcap('sPark sql')"}
        )

        self.validate_all(
            "SELECT LOCATE('bar', 'abcbarbar', 5)",
            read={"databricks": "SELECT charindex('bar', 'abcbarbar', 5)"},
        )

        self.validate_all(
            "SELECT LOCATE('bar', 'abcbarbar', 5)",
            read={"databricks": "SELECT locate('bar', 'abcbarbar', 5)"},
        )

        self.validate_all(
            "SELECT LOCATE('6', 'e6data-e6data')",
            read={"databricks": "select position('6' in 'e6data-e6data')"},
        )

        self.validate_all(
            "SELECT LEFT('Spark SQL', 3)", read={"databricks": "SELECT left('Spark SQL', 3)"}
        )

        self.validate_all(
            "SELECT RIGHT('Spark SQL', 3)", read={"databricks": "SELECT right('Spark SQL', 3)"}
        )

        self.validate_all(
            "SELECT CONTAINS_SUBSTR('SparkSQL', 'Spark')",
            read={"databricks": "SELECT contains('SparkSQL', 'Spark')"},
        )

        self.validate_all(
            "SELECT LOCATE('SQL', 'SparkSQL')",
            read={"databricks": "SELECT instr('SparkSQL', 'SQL')"},
        )

        self.validate_all(
            "SELECT SOUNDEX('Miller')", read={"databricks": "SELECT soundex('Miller')"}
        )

        self.validate_all(
            "SELECT SPLIT('oneAtwoBthreeC', '[ABC]', 2)",
            read={"databricks": "SELECT split('oneAtwoBthreeC', '[ABC]', 2)"},
        )

        self.validate_all(
            "SPLIT_PART('Hello,world,!', ',', 1)",
            read={"databricks": "split_part('Hello,world,!', ',', 1)"},
        )

        self.validate_all("SELECT REPEAT('a', 4)", read={"databricks": "select repeat('a', 4)"})

        self.validate_all("SELECT ASCII('234')", read={"databricks": "SELECT ascii('234')"})

        self.validate_all(
            "SELECT ENDSWITH('SparkSQL', 'sql')",
            read={"databricks": "SELECT endswith('SparkSQL', 'sql')"},
        )

        self.validate_all(
            "SELECT STARTS_WITH('SparkSQL', 'spark')",
            read={"databricks": "SELECT startswith('SparkSQL', 'spark')"},
        )

        self.validate_all(
            "SELECT LOCATE('6', 'e6data')", read={"databricks": "SELECT STRPOS('e6data','6')"}
        )

        self.validate_all(
            "SELECT LPAD('hi', 1, '??')", read={"databricks": "SELECT lpad('hi', 1, '??')"}
        )

        self.validate_all(
            "SELECT RPAD('hi', 5, 'ab')", read={"databricks": "SELECT rpad('hi', 5, 'ab')"}
        )

        self.validate_all(
            "SELECT REVERSE(ARRAY[2, 1, 4, 3])",
            read={"databricks": "SELECT reverse(array(2, 1, 4, 3))"},
        )

        self.validate_all(
            "SELECT REVERSE('Spark SQL')", read={"databricks": "SELECT reverse('Spark SQL')"}
        )

        self.validate_all(
            "SELECT TO_CHAR(CAST(111.11 AS TIMESTAMP),'$99.9')",
            read={"databricks": "SELECT to_char(111.11, '$99.9')"},
        )

        self.validate_all(
            "SELECT TO_CHAR(CAST(12454 AS TIMESTAMP),'99,999')",
            read={"databricks": "SELECT to_char(12454, '99,999')"},
        )

        self.validate_all(
            "SELECT TO_CHAR(CAST(CAST('2016-04-08' AS DATE) AS TIMESTAMP),'y')",
            read={"databricks": "SELECT to_char(date'2016-04-08', 'y')"},
        )

        self.validate_all(
            "SELECT TO_VARCHAR(1539177637527311044940, 'hex')",
            read={"databricks": "SELECT to_varchar(x'537061726b2053514c', 'hex')"},
        )

        # CONCAT_WS tests - based on Databricks documentation
        # Basic string concatenation: concat_ws(' ', 'Spark', 'SQL') -> 'Spark SQL'
        self.validate_all(
            "SELECT ARRAY_TO_STRING(ARRAY['Spark', 'SQL'], ' ')",
            read={"databricks": "SELECT concat_ws(' ', 'Spark', 'SQL')"},
        )

        # Only separator provided: concat_ws('s') -> ''
        self.validate_all(
            "SELECT ''",
            read={"databricks": "SELECT concat_ws('s')"},
        )

        # Mixed strings, arrays and NULLs: concat_ws(',', 'Spark', array('S', 'Q', NULL, 'L'), NULL) -> 'Spark,S,Q,L'
        self.validate_all(
            "SELECT ARRAY_TO_STRING(ARRAY['Spark', 'S', 'Q', 'L'], ',')",
            read={"databricks": "SELECT concat_ws(',', 'Spark', array('S', 'Q', NULL, 'L'), NULL)"},
        )

        # Single string argument with separator
        self.validate_all(
            "SELECT 'test'",
            read={"databricks": "SELECT concat_ws('-', 'test')"},
        )

        # Multiple string arguments
        self.validate_all(
            "SELECT ARRAY_TO_STRING(ARRAY['a', 'b', 'c'], '-')",
            read={"databricks": "SELECT concat_ws('-', 'a', 'b', 'c')"},
        )

        # Empty separator
        self.validate_all(
            "SELECT ARRAY_TO_STRING(ARRAY['hello', 'world'], '')",
            read={"databricks": "SELECT concat_ws('', 'hello', 'world')"},
        )

        # Array with all valid elements (no NULLs)
        self.validate_all(
            "SELECT ARRAY_TO_STRING(ARRAY['x', 'y', 'z'], '|')",
            read={"databricks": "SELECT concat_ws('|', array('x', 'y', 'z'))"},
        )

    def test_to_utf(self):
        self.validate_all(
            "TO_UTF8(x)",
            read={
                "duckdb": "ENCODE(x)",
                "spark": "ENCODE(x, 'utf-8')",
                "databricks": "ENCODE(x, 'utf-8')",
                "presto": "TO_UTF8(x)",
            },
            write={
                "duckdb": "ENCODE(x)",
                "presto": "TO_UTF8(x)",
                "spark": "ENCODE(x, 'utf-8')",
                "databricks": "ENCODE(x, 'utf-8')",
            },
        )

        self.validate_all(
            """SELECT emoji, TO_UTF8(emoji) AS utf8_bytes, LENGTH(TO_UTF8(emoji)) AS byte_length, LENGTH(emoji) AS char_length FROM (VALUES ('🔥'), ('🌍'), ('😊'), ('⚡')) AS t(emoji)""",
            read={
                "spark": "SELECT emoji, ENCODE(emoji, 'utf-8') AS utf8_bytes, LENGTH(ENCODE(emoji, 'utf-8')) AS byte_length, LENGTH(emoji) AS char_length FROM (VALUES ('🔥'), ('🌍'), ('😊'), ('⚡')) AS t(emoji)",
            },
        )

    def test_md5(self):
        self.validate_all(
            "SELECT MD5('E6')",
            read={
                "duckdb": "SELECT MD5('E6')",
                "spark": "SELECT MD5('E6')",
                "databricks": "SELECT MD5('E6')",
                "clickhouse": "SELECT MD5('E6')",
                "presto": "SELECT MD5('E6')",
                "trino": "SELECT MD5('E6')",
                "snowflake": "select MD5_HEX('E6')",
            },
            write={
                "bigquery": "SELECT MD5('E6')",
                "duckdb": "SELECT UNHEX(MD5('E6'))",
                "clickhouse": "SELECT MD5('E6')",
                "presto": "SELECT MD5('E6')",
                "trino": "SELECT MD5('E6')",
                "snowflake": "SELECT MD5('E6')",
                "databricks": "SELECT UNHEX(MD5('E6'))",
            },
        )

    def test_array_prepend_append(self):
        self.validate_all(
            "ARRAY_APPEND(ARRAY[1, 2], 3)",
            read={
                "databricks": "ARRAY_APPEND(ARRAY(1, 2),3)",
                "snowflake": "ARRAY_APPEND(ARRAY_CONSTRUCT(1, 2),3)",
            },
            write={
                "databricks": "ARRAY_APPEND(ARRAY(1, 2), 3)",
                "snowflake": "ARRAY_APPEND([1, 2], 3)",
            },
        )

        self.validate_all(
            "ARRAY_PREPEND(ARRAY[1, 2], 3)",
            read={
                "databricks": "ARRAY_PREPEND(ARRAY(1, 2),3)",
                "snowflake": "ARRAY_PREPEND(ARRAY_CONSTRUCT(1, 2),3)",
            },
            write={
                "databricks": "ARRAY_PREPEND(ARRAY(1, 2), 3)",
                "snowflake": "ARRAY_PREPEND([1, 2], 3)",
            },
        )

        self.validate_all(
            "SELECT ARRAY_APPEND(ARRAY_PREPEND(ARRAY[10, 20], 5 * 2), 100 + 23)",
            read={
                "databricks": "SELECT array_append(array_prepend(array(10, 20), 5 * 2), 100 + 23)",
                "snowflake": "SELECT array_append(array_prepend(array_construct(10, 20), 5 * 2), 100 + 23)",
            },
        )

    def test_array_to_string(self):
        self.validate_all(
            "SELECT ARRAY_JOIN(ARRAY[1, 2, 3], '+')",
            read={
                "snowflake": "SELECT ARRAY_TO_STRING(ARRAY_CONSTRUCT(1,2,3),'+')",
                "databricks": "SELECT ARRAY_JOIN(ARRAY[1, 2, 3], '+')",
            },
            write={
                "snowflake": "SELECT ARRAY_TO_STRING([1, 2, 3], '+')",
                "databricks": "SELECT ARRAY_JOIN(ARRAY(1, 2, 3), '+')",
            },
        )

        self.validate_all(
            "SELECT ARRAY_JOIN(ARRAY[1, 2, 3, NULL], '+', '@')",
            read={
                "databricks": "SELECT ARRAY_JOIN(ARRAY[1, 2, 3, NULL], '+', '@')",
            },
        )

    def test_date_time(self):
        self.validate_all("SELECT NOW()", read={"databricks": "SELECT now()"})

        self.validate_all(
            "SELECT event_string, CAST(SUBSTRING(event_string, 1, 10) AS DATE) AS event_date FROM events",
            read={
                "databricks": "SELECT event_string, date(substring(event_string, 1, 10)) AS event_date FROM events"
            },
        )

        self.validate_all(
            "SELECT CAST('2020-04-30 12:25:13.45' AS TIMESTAMP)",
            read={"databricks": "SELECT timestamp('2020-04-30 12:25:13.45')"},
        )

        self.validate_all(
            "SELECT TO_DATE('2016-12-31', 'y-MM-dd')",
            read={"databricks": "SELECT to_date('2016-12-31', 'yyyy-MM-dd')"},
        )

        self.validate_all(
            "SELECT TO_DATE('2016-12-31', 'y-m-d')",
            read={"databricks": "SELECT to_date('2016-12-31', '%y-%m-%d')"},
        )

        self.validate_all(
            "SELECT TO_TIMESTAMP('2016-12-31', 'y-MM-dd')",
            read={"databricks": "SELECT to_timestamp('2016-12-31', 'yyyy-MM-dd')"},
        )

        self.validate_all(
            "SELECT TO_TIMESTAMP_NTZ('1997-09-06 12:29:34')",
            read={"databricks": "select to_timestamp_ntz('1997-09-06 12:29:34')"},
        )

        # This transpilation is incorrect as format is not considered.
        self.validate_all(
            "SELECT FORMAT_TIMESTAMP(FROM_UNIXTIME(0), 'y-MM-dd HH:mm:ss')",
            read={"databricks": "SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss')"},
        )

        self.validate_all(
            "SELECT DATE_TRUNC('YEAR', '2015-03-05T09:32:05.359')",
            read={"databricks": "SELECT date_trunc('YEAR', '2015-03-05T09:32:05.359')"},
        )

        self.validate_all(
            "SELECT DATE_TRUNC('MM', '2015-03-05T09:32:05.359')",
            read={"databricks": "SELECT date_trunc('MM', '2015-03-05T09:32:05.359')"},
        )

        self.validate_all(
            "SELECT DATE_TRUNC('WEEK', '2019-08-04')",
            read={"databricks": "SELECT trunc('2019-08-04', 'week')"},
        )

        self.validate_all(
            "SELECT DATE_ADD('YEAR', 5, CAST('2000-08-05' AS DATE))",
            read={"databricks": "select date_add('year', 5, cast('2000-08-05' as date))"},
        )

        self.validate_all(
            "SELECT DATE_DIFF('YEAR', '2021-01-01', '2022-03-28')",
            read={"databricks": """SELECT date_diff("YEAR", '2021-01-01', '2022-03-28')"""},
        )

        self.validate_all(
            "SELECT DATE_DIFF('DAY', '2009-07-30', '2009-07-31')",
            read={"databricks": "SELECT datediff('2009-07-31', '2009-07-30')"},
        )

        self.validate_all(
            "SELECT TIMESTAMP_ADD('MONTH', -1, CAST('2022-03-31 00:00:00' AS TIMESTAMP))",
            read={"databricks": "SELECT timestampadd(MONTH, -1, TIMESTAMP'2022-03-31 00:00:00')"},
        )

        self.validate_all(
            "SELECT TIMESTAMP_DIFF(CAST('1900-03-28' AS DATE), CAST('2021-01-01' AS DATE), 'YEAR')",
            read={"databricks": "SELECT timestampdiff(YEAR, DATE'2021-01-01', DATE'1900-03-28')"},
        )

        self.validate_all(
            "SELECT EXTRACT(YEAR FROM CAST('2019-08-12 01:00:00.123456' AS TIMESTAMP))",
            read={"databricks": "SELECT extract(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456')"},
        )

        self.validate_all(
            "SELECT EXTRACT(DAY FROM CAST('2019-08-12' AS DATE))",
            read={"databricks": "SELECT date_part('day', DATE'2019-08-12')"},
        )

        self.validate_all(
            "SELECT YEAR(TO_DATE('2008-02-20'))", read={"databricks": "SELECT year('2008-02-20')"}
        )

        self.validate_all(
            "SELECT MONTH(TO_DATE('2008-02-20'))", read={"databricks": "SELECT month('2008-02-20')"}
        )

        self.validate_all(
            "SELECT DAYS(TO_DATE('2016-07-30'))", read={"databricks": "SELECT DAY('2016-07-30')"}
        )

        self.validate_all(
            "SELECT LAST_DAY(CAST('2009-01-12' AS DATE))",
            read={"databricks": "SELECT last_day('2009-01-12')"},
        )

        self.validate_all(
            "SELECT DAYNAME(CAST('2024-11-01' AS DATE))",
            read={"databricks": "SELECT dayname(DATE'2024-11-01')"},
        )

        self.validate_all(
            "SELECT HOUR('2009-07-30 12:58:59')",
            read={"databricks": "SELECT hour('2009-07-30 12:58:59')"},
        )

        self.validate_all(
            "SELECT MINUTE('2009-07-30 12:58:59')",
            read={"databricks": "SELECT minute('2009-07-30 12:58:59')"},
        )

        self.validate_all(
            "SELECT SECOND('2009-07-30 12:58:59')",
            read={"databricks": "SELECT second('2009-07-30 12:58:59')"},
        )

        self.validate_all(
            "SELECT DAYOFWEEK(TO_DATE('2009-07-30'))",
            read={"databricks": "SELECT dayofweek('2009-07-30')"},
        )

        self.validate_all(
            "SELECT WEEKOFYEAR(TO_DATE('2008-02-20'))",
            read={"databricks": "SELECT weekofyear('2008-02-20')"},
        )

        self.validate_all(
            "SELECT DATE_SUB('2016-07-30', 1)",
            read={"databricks": "SELECT date_sub('2016-07-30', 1)"},
        )

        self.validate_all(
            "SELECT FORMAT_TIMESTAMP(CAST('2024-08-26 22:38:11' AS TIMESTAMP), 'm-d-y H')",
            read={
                "databricks": "select date_format(cast('2024-08-26 22:38:11' as timestamp), '%m-%d-%Y %H')"
            },
        )

        self.validate_all(
            "SELECT DATETIME(DATETIME(CAST('2022-05-01 07:10:12' AS TIMESTAMP), 'America/Los_Angeles'), 'Africa/Cairo')",
            read={
                "databricks": "select convert_timezone('America/Los_Angeles','Africa/Cairo','2022-05-01 07:10:12')"
            },
        )

    def test_conditional_expression(self):
        self.validate_all(
            "SELECT SUM(COALESCE(CASE WHEN performance_rating > 7 THEN 1 END, 0))",
            read={
                "databricks": "SELECT SUM(COALESCE( CASE WHEN performance_rating > 7 THEN 1 END, 0 ))"
            },
        )

        self.validate_all("SELECT NULLIF(12, NULL)", read={"databricks": "select nullif(12, null)"})

        self.validate_all("SELECT NULLIF(12, 12)", read={"databricks": "select nullif(12, 12)"})

        self.validate_all(
            "SELECT GREATEST(100, 12, 23, 1999, 2)",
            read={"databricks": "select greatest(100, 12, 23, 1999, 2)"},
        )

        self.validate_all(
            "SELECT LEAST(100, 12, 23, 1999, 2)",
            read={"databricks": "select least(100, 12, 23, 1999, 2)"},
        )

        self.validate_all("SELECT NVL(NULL, 2)", read={"databricks": "SELECT nvl(NULL, 2)"})

        self.validate_all("SELECT NVL(3, 2)", read={"databricks": "SELECT nvl(3, 2)"})

        self.validate_all("SELECT NVL2(NULL, 2, 1)", read={"databricks": "SELECT nvl2(NULL, 2, 1)"})

        self.validate_all(
            "SELECT NVL2('spark', 2, 1)", read={"databricks": "SELECT nvl2('spark', 2, 1)"}
        )

        self.validate_all(
            "SELECT TRY_CAST('45.6789' AS DOUBLE)",
            read={"databricks": "select try_cast('45.6789' AS double)"},
        )

    def test_window_funcs(self):
        self.validate_all(
            "SELECT a, b, DENSE_RANK() OVER (PARTITION BY a ORDER BY b), RANK() OVER (PARTITION BY a ORDER BY b), ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) FROM (VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1)) AS tab(a, b)",
            read={
                "databricks": "SELECT a, b, dense_rank() OVER(PARTITION BY a ORDER BY b), rank() OVER(PARTITION BY a ORDER BY b), row_number() OVER(PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b)"
            },
        )

        self.validate_all(
            "SELECT a, b, NTILE(2) OVER (PARTITION BY a ORDER BY b) FROM (VALUES ('A1', 2), ('A1', 1))",
            read={
                "databricks": "SELECT a, b, ntile(2) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1)"
            },
        )

        self.validate_all(
            "SELECT FIRST_VALUE(col) IGNORE NULLS FROM (VALUES (NULL), (5), (20)) AS tab(col)",
            read={
                "databricks": "SELECT first_value(col, true) FROM VALUES (NULL), (5), (20) AS tab(col)"
            },
        )

        self.validate_all(
            "SELECT ARRAY_AGG(DISTINCT col) FROM (VALUES (1), (2), (NULL), (1)) AS tab(col)",
            read={
                "databricks": "SELECT collect_list(DISTINCT col) FROM VALUES (1), (2), (NULL), (1) AS tab(col)"
            },
        )

        self.validate_all(
            "SELECT FIRST_VALUE(col) FILTER(WHERE col > 5) FROM (VALUES (5), (20)) AS tab(col)",
            read={
                "databricks": "SELECT first_value(col) FILTER (WHERE col > 5) FROM VALUES (5), (20) AS tab(col)"
            },
        )

        # self.validate_all(
        #     "SELECT LAST_VALUE(col) FILTER(WHERE col > 5) FROM (VALUES (5), (20)) AS tab(col)",
        #     read={
        #         'databricks': "SELECT last_value(col) FILTER (WHERE col > 5) FROM VALUES (5), (20) AS tab(col)"
        #     }
        # )
        #
        self.validate_all(
            "SELECT a, b, LEAD(b) OVER (PARTITION BY a ORDER BY b)",
            read={"databricks": "SELECT a, b, lead(b) OVER (PARTITION BY a ORDER BY b)"},
        )

        self.validate_all(
            "SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b)",
            read={"databricks": "SELECT a, b, lag(b) OVER (PARTITION BY a ORDER BY b)"},
        )

        self.validate_all(
            "SELECT 1 IN (SELECT * FROM (VALUES (1), (2)))",
            read={"databricks": "SELECT 1 IN (SELECT * FROM VALUES(1), (2))"},
        )

        self.validate_all(
            "SELECT (1, 2) IN ((1, 2), (2, 3))",
            read={"databricks": "SELECT (1, 2) IN ((1, 2), (2, 3))"},
        )

    def test_statistical_funcs(self):
        self.validate_all(
            "SELECT STDDEV(DISTINCT col) FROM (VALUES (1), (2), (3), (3)) AS tab(col)",
            read={
                "databricks": "SELECT stddev(DISTINCT col) FROM VALUES (1), (2), (3), (3) AS tab(col)"
            },
        )

        self.validate_all(
            "SELECT STDDEV_POP(DISTINCT col) FROM (VALUES (1), (2), (3), (3)) AS tab(col)",
            read={
                "databricks": "SELECT stddev_pop(DISTINCT col) FROM VALUES (1), (2), (3), (3) AS tab(col)"
            },
        )

        self.validate_all(
            "SELECT PERCENTILE_CONT(ARRAY[0.5, 0.4, 0.1]) WITHIN GROUP (ORDER BY col)",
            read={
                "databricks": "SELECT percentile_cont(array(0.5, 0.4, 0.1)) WITHIN GROUP (ORDER BY col)"
            },
        )

        self.validate_all(
            "SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY col) FROM (VALUES (0), (6), (6), (7), (9), (10)) AS tab(col)",
            read={
                "databricks": "SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY col) FROM VALUES (0), (6), (6), (7), (9), (10) AS tab(col)"
            },
        )

        # Additional STDDEV tests from multiple dialects
        self.validate_all(
            "SELECT STDDEV(col) FROM (VALUES (1), (2), (3), (4)) AS tab(col)",
            read={
                "databricks": "SELECT stddev(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
                "snowflake": "SELECT STDDEV(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
                "postgres": "SELECT STDDEV(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
            },
        )
        self.validate_all(
            "SELECT STDDEV_SAMP(col) FROM (VALUES (1), (2), (3), (4)) AS tab(col)",
            read={
                "databricks": "SELECT stddev_samp(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
                "snowflake": "SELECT STDDEV_SAMP(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
                "postgres": "SELECT STDDEV_SAMP(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
            },
        )

        # COVAR_SAMP tests from multiple dialects
        self.validate_all(
            "SELECT COVAR_SAMP(x, y) FROM (VALUES (1, 10), (2, 20), (3, 30)) AS tab(x, y)",
            read={
                "databricks": "SELECT covar_samp(x, y) FROM VALUES (1, 10), (2, 20), (3, 30) AS tab(x, y)",
                "snowflake": "SELECT COVAR_SAMP(x, y) FROM VALUES (1, 10), (2, 20), (3, 30) AS tab(x, y)",
                "postgres": "SELECT COVAR_SAMP(x, y) FROM VALUES (1, 10), (2, 20), (3, 30) AS tab(x, y)",
            },
        )

        # VARIANCE_SAMP tests from multiple dialects
        self.validate_all(
            "SELECT VARIANCE_SAMP(col) FROM (VALUES (1), (2), (3), (4), (5)) AS tab(col)",
            read={
                "databricks": "SELECT variance_samp(col) FROM VALUES (1), (2), (3), (4), (5) AS tab(col)",
                "snowflake": "SELECT VARIANCE_SAMP(col) FROM VALUES (1), (2), (3), (4), (5) AS tab(col)",
            },
        )
        self.validate_all(
            "SELECT VARIANCE_SAMP(DISTINCT col) FROM (VALUES (1), (2), (2), (3), (3), (3)) AS tab(col)",
            read={
                "databricks": "SELECT variance_samp(DISTINCT col) FROM VALUES (1), (2), (2), (3), (3), (3) AS tab(col)"
            },
        )

        # VAR_SAMP tests from multiple dialects
        self.validate_all(
            "SELECT VAR_SAMP(col) FROM (VALUES (1), (2), (3), (4)) AS tab(col)",
            read={
                "databricks": "SELECT var_samp(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
                "snowflake": "SELECT VAR_SAMP(col) FROM VALUES (1), (2), (3), (4) AS tab(col)",
            },
        )

    def test_unixtime_functions(self):
        self.validate_all(
            "FORMAT_TIMESTAMP(X, 'y')",
            read={
                "databricks": "FROM_UNIXTIME(UNIX_TIMESTAMP(X), 'yyyy')",
            },
        )

        self.validate_all(
            "FROM_UNIXTIME(A)",
            read={
                "databricks": "FROM_UNIXTIME(A)",
            },
        )

        self.validate_all(
            "FORMAT_TIMESTAMP(FROM_UNIXTIME(A), 'y')",
            read={
                "databricks": "FROM_UNIXTIME(A, 'yyyy')",
            },
        )

        self.validate_all(
            "SELECT TO_UNIX_TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '2016-04-08'))/1000",
            read={"databricks": "SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd')"},
        )

        self.validate_all(
            "SELECT TO_UNIX_TIMESTAMP('2016-04-08')/1000",
            read={"databricks": "SELECT to_unix_timestamp('2016-04-08')"},
        )

        self.validate_all(
            "SELECT TO_UNIX_TIMESTAMP(A)/1000",
            read={"databricks": "SELECT UNIX_TIMESTAMP(A)", "trino": "SELECT TO_UNIXTIME(A)"},
            write={
                "databricks": "SELECT TO_UNIX_TIMESTAMP(A) / 1000",
                "snowflake": "SELECT EXTRACT(epoch_second FROM A) / 1000",
            },
        )

        self.validate_all(
            "SELECT TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP)/1000",
            read={"databricks": "SELECT UNIX_TIMESTAMP()"},
        )

        self.validate_all(
            "SELECT * FROM events WHERE event_time >= TO_UNIX_TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '2023-01-01'))/1000 AND event_time < TO_UNIX_TIMESTAMP(PARSE_DATETIME('%Y-%m-%d', '2023-02-01'))/1000",
            read={
                "databricks": "SELECT * FROM events WHERE event_time >= unix_timestamp('2023-01-01', 'yyyy-MM-dd') AND event_time < unix_timestamp('2023-02-01', 'yyyy-MM-dd')"
            },
        )

        self.validate_all(
            "SELECT TO_UNIX_TIMESTAMP(PARSE_DATETIME('%Y-%m-%d %h:%i:%S', '2016-04-08 12:10:15'))/1000",
            read={
                "databricks": "SELECT to_unix_timestamp('2016-04-08 12:10:15', 'yyyy-LL-dd hh:mm:ss')"
            },
        )

    def test_timestamp_seconds(self):
        # Test basic TIMESTAMP_SECONDS with integer literal
        self.validate_all(
            "FROM_UNIXTIME_WITHUNIT(1230219000, 'seconds')",
            read={
                "databricks": "TIMESTAMP_SECONDS(1230219000)",
            },
        )

        # Test TIMESTAMP_SECONDS with decimal literal (fractional seconds)
        self.validate_all(
            "FROM_UNIXTIME_WITHUNIT(1230219000.123, 'seconds')",
            read={
                "databricks": "TIMESTAMP_SECONDS(1230219000.123)",
            },
        )

        # Test TIMESTAMP_SECONDS with column reference
        self.validate_all(
            "FROM_UNIXTIME_WITHUNIT(epoch_timestamp, 'seconds')",
            read={
                "databricks": "TIMESTAMP_SECONDS(epoch_timestamp)",
            },
        )

        # Test TIMESTAMP_SECONDS with expression
        self.validate_all(
            "FROM_UNIXTIME_WITHUNIT(unix_time + 3600, 'seconds')",
            read={
                "databricks": "TIMESTAMP_SECONDS(unix_time + 3600)",
            },
        )

        # Test TIMESTAMP_SECONDS with NULL
        self.validate_all(
            "FROM_UNIXTIME_WITHUNIT(NULL, 'seconds')",
            read={
                "databricks": "TIMESTAMP_SECONDS(NULL)",
            },
        )

        # Test TIMESTAMP_SECONDS in SELECT statement
        self.validate_all(
            "SELECT FROM_UNIXTIME_WITHUNIT(1230219000, 'seconds') AS converted_timestamp",
            read={
                "databricks": "SELECT TIMESTAMP_SECONDS(1230219000) AS converted_timestamp",
            },
        )

        # Test TIMESTAMP_SECONDS in WHERE clause
        self.validate_all(
            "SELECT * FROM events WHERE created_at > FROM_UNIXTIME_WITHUNIT(1230219000, 'seconds')",
            read={
                "databricks": "SELECT * FROM events WHERE created_at > TIMESTAMP_SECONDS(1230219000)",
            },
        )

        # Test multiple TIMESTAMP_SECONDS calls
        self.validate_all(
            "SELECT FROM_UNIXTIME_WITHUNIT(start_time, 'seconds') AS start_ts, FROM_UNIXTIME_WITHUNIT(end_time, 'seconds') AS end_ts FROM events",
            read={
                "databricks": "SELECT TIMESTAMP_SECONDS(start_time) AS start_ts, TIMESTAMP_SECONDS(end_time) AS end_ts FROM events",
            },
        )

        # Test TIMESTAMP_SECONDS with CAST
        self.validate_all(
            "FROM_UNIXTIME_WITHUNIT(CAST(epoch_string AS BIGINT), 'seconds')",
            read={
                "databricks": "TIMESTAMP_SECONDS(CAST(epoch_string AS BIGINT))",
            },
        )

        # Test TIMESTAMP_SECONDS with subquery
        self.validate_all(
            "SELECT FROM_UNIXTIME_WITHUNIT((SELECT MAX(epoch_time) FROM historical_data), 'seconds') AS max_timestamp",
            read={
                "databricks": "SELECT TIMESTAMP_SECONDS((SELECT MAX(epoch_time) FROM historical_data)) AS max_timestamp",
            },
        )

    def test_timestamp_epoch(self):
        # Test timestamp 'epoch' literal
        self.validate_all(
            "CAST('1970-01-01T00:00:00.000' AS TIMESTAMP)",
            read={
                "databricks": "timestamp 'epoch'",
            },
        )

        # Test CAST('epoch' AS TIMESTAMP)
        self.validate_all(
            "CAST('1970-01-01T00:00:00.000' AS TIMESTAMP)",
            read={
                "databricks": "CAST('epoch' AS TIMESTAMP)",
            },
        )

    def test_array_agg(self):
        self.validate_all(
            "SELECT ARRAY_AGG(DISTINCT col) AS result FROM (VALUES (1), (2), (NULL), (1)) AS tab(col)",
            read={
                "databricks": "SELECT collect_list(DISTINCT col) AS result FROM VALUES (1), (2), (NULL), (1) AS tab(col)",
            },
            write={
                "databricks": "SELECT COLLECT_LIST(DISTINCT col) AS result FROM VALUES (1), (2), (NULL), (1) AS tab(col)"
            },
        )

        self.validate_all(
            "SELECT ARRAY_AGG(employee) FILTER(WHERE performance_rating > 3) OVER (PARTITION BY dept) AS top_performers FROM (VALUES ('Sales', 'Alice', 5), ('Sales', 'Bob', 2)) AS tab(dept, employee, performance_rating)",
            read={
                "databricks": "SELECT collect_list(employee) FILTER (WHERE performance_rating > 3) OVER (PARTITION BY dept) AS top_performers FROM (VALUES ('Sales', 'Alice', 5), ('Sales', 'Bob', 2)) AS tab(dept, employee, performance_rating)",
            },
            write={
                "databricks": "SELECT COLLECT_LIST(employee) FILTER(WHERE performance_rating > 3) OVER (PARTITION BY dept) AS top_performers FROM VALUES ('Sales', 'Alice', 5), ('Sales', 'Bob', 2) AS tab(dept, employee, performance_rating)",
            },
        )

        self.validate_all(
            "SELECT ARRAY_AGG(col) FROM (VALUES (1), (2), (NULL), (1)) AS tab(col)",
            read={
                "databricks": "SELECT collect_set(col) FROM VALUES (1), (2), (NULL), (1) AS tab(col)",
            },
        )

    def test_bitwise(self):
        self.validate_all(
            "BITWISE_NOT(1)",
            read={
                "snowflake": "BITNOT(1)",
            },
        )

        self.validate_all(
            "SHIFTLEFT(x, 1)",
            read={
                "trino": "bitwise_left_shift(x, 1)",
                "duckdb": "x << 1",
                "hive": "x << 1",
                "spark": "SHIFTLEFT(x, 1)",
                "databricks": "SHIFTLEFT(x, 1)",
                "snowflake": "BITSHIFTLEFT(x, 1)",
            },
            write={
                "snowflake": "BITSHIFTLEFT(x, 1)",
                "spark": "SHIFTLEFT(x, 1)",
                "databricks": "SHIFTLEFT(x, 1)",
                "trino": "BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)",
            },
        )

        self.validate_all(
            "SELECT CASE WHEN SHIFTLEFT(1, 4) > 10 THEN SHIFTRIGHT(128, 3) ELSE SHIFTLEFT(2, 2) END AS result",
            read={
                "databricks": "SELECT CASE WHEN SHIFTLEFT(1, 4) > 10 THEN SHIFTRIGHT(128, 3) ELSE SHIFTLEFT(2, 2) END AS result",
            },
        )

        self.validate_all(
            "SHIFTRIGHT(x, 1)",
            read={
                "trino": "bitwise_right_shift(x, 1)",
                "duckdb": "x >> 1",
                "hive": "x >> 1",
                "spark": "SHIFTRIGHT(x, 1)",
                "databricks": "SHIFTRIGHT(x, 1)",
                "snowflake": "BITSHIFTRIGHT(x, 1)",
            },
            write={
                "snowflake": "BITSHIFTRIGHT(x, 1)",
                "spark": "SHIFTRIGHT(x, 1)",
                "databricks": "SHIFTRIGHT(x, 1)",
                "trino": "BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)",
            },
        )

    def test_space(self):
        # Basic integer literal
        self.validate_all(
            "REPEAT(' ', 5)",
            read={"databricks": "SPACE(5)"},
        )

        # Column reference
        self.validate_all(
            "REPEAT(' ', n)",
            read={"databricks": "SPACE(n)"},
        )

        # Complex expression
        self.validate_all(
            "REPEAT(' ', column_count + 2)",
            read={"databricks": "SPACE(column_count + 2)"},
        )

        # Zero spaces
        self.validate_all(
            "REPEAT(' ', 0)",
            read={"databricks": "SPACE(0)"},
        )

        # In SELECT with alias
        self.validate_all(
            "SELECT REPEAT(' ', 10) AS spaces",
            read={"databricks": "SELECT SPACE(10) AS spaces"},
        )

        # With CONCAT
        self.validate_all(
            "SELECT CONCAT('Hello', REPEAT(' ', 5), 'World') AS greeting",
            read={"databricks": "SELECT CONCAT('Hello', SPACE(5), 'World') AS greeting"},
        )

    def test_databricks_to_e6data_pretty(self):
        sql = "SELECT CASE WHEN SHIFTLEFT(1, 4) > 10 THEN SHIFTRIGHT(128, 3) ELSE SHIFTLEFT(2, 2) END AS result"

        expected = """SELECT
  CASE WHEN SHIFTLEFT(1, 4) > 10 THEN SHIFTRIGHT(128, 3) ELSE SHIFTLEFT(2, 2) END AS result"""

        self.validate_all(expected, read={"databricks": sql}, pretty=True)

    def test_values_in_cte(self):
        self.validate_identity(
            "WITH cte1 AS (SELECT * FROM (VALUES ('foo_val')) AS data(c1)) SELECT foo1 FROM cte1"
        )

        self.validate_all(
            """WITH map AS (SELECT * FROM (VALUES ('allure', 'Allure', 'US')) AS map(app_id, brand, market)) SELECT app_id, brand, market FROM map""",
            read={
                "databricks": """WITH map AS (VALUES ('allure', 'Allure', 'US') AS map(app_id, brand, market)) select app_id, brand, market from map"""
            },
        )

    def test_random(self):
        self.validate_all(
            "RAND()",
            write={
                "bigquery": "RAND()",
                "clickhouse": "randCanonical()",
                "databricks": "RAND()",
                "doris": "RAND()",
                "drill": "RAND()",
                "duckdb": "RANDOM()",
                "hive": "RAND()",
                "mysql": "RAND()",
                "oracle": "DBMS_RANDOM.VALUE()",
                "postgres": "RANDOM()",
                "presto": "RAND()",
                "spark": "RAND()",
                "sqlite": "RANDOM()",
                "tsql": "RAND()",
            },
            read={
                "bigquery": "RAND()",
                "clickhouse": "randCanonical()",
                "databricks": "RAND()",
                "doris": "RAND()",
                "drill": "RAND()",
                "duckdb": "RANDOM()",
                "hive": "RAND()",
                "mysql": "RAND()",
                "oracle": "DBMS_RANDOM.VALUE()",
                "postgres": "RANDOM()",
                "presto": "RAND()",
                "spark": "RAND()",
                "sqlite": "RANDOM()",
                "tsql": "RAND()",
            },
        )

    def test_group_by_all(self):
        # Basic GROUP BY ALL test
        self.validate_all(
            "SELECT category, brand, AVG(price) AS average_price FROM products GROUP BY ALL",
            read={
                "databricks": "SELECT category, brand, AVG(price) AS average_price FROM products GROUP BY ALL"
            },
        )

        # GROUP BY ALL with CTE
        self.validate_all(
            """WITH products AS (SELECT 'Electronics' AS category, 'BrandA' AS brand, 100 AS price UNION ALL SELECT 'Electronics' AS category, 'BrandA' AS brand, 150 AS price) SELECT category, brand, AVG(price) AS average_price FROM products GROUP BY ALL""",
            read={
                "databricks": """WITH products AS (SELECT 'Electronics' AS category, 'BrandA' AS brand, 100 AS price UNION ALL SELECT 'Electronics' AS category, 'BrandA' AS brand, 150 AS price) SELECT category, brand, AVG(price) AS average_price FROM products GROUP BY ALL"""
            },
        )

        # GROUP BY ALL with ORDER BY
        self.validate_all(
            "SELECT department, COUNT(*) AS employee_count FROM employees GROUP BY ALL ORDER BY employee_count DESC",
            read={
                "databricks": "SELECT department, COUNT(*) AS employee_count FROM employees GROUP BY ALL ORDER BY employee_count DESC"
            },
        )

        # GROUP BY ALL with HAVING clause
        self.validate_all(
            "SELECT region, SUM(sales) AS total_sales FROM sales_data GROUP BY ALL HAVING SUM(sales) > 1000",
            read={
                "databricks": "SELECT region, SUM(sales) AS total_sales FROM sales_data GROUP BY ALL HAVING SUM(sales) > 1000"
            },
        )

        # GROUP BY ALL with multiple aggregations
        self.validate_all(
            "SELECT product_category, COUNT(*) AS item_count, AVG(price) AS avg_price, MAX(price) AS max_price FROM inventory GROUP BY ALL",
            read={
                "databricks": "SELECT product_category, COUNT(*) AS item_count, AVG(price) AS avg_price, MAX(price) AS max_price FROM inventory GROUP BY ALL"
            },
        )

    def test_keywords(self):
        self.validate_identity("""SELECT a."variant" FROM table AS a""")

    def test_odbc_datetime_literals(self):
        """Test ODBC datetime literal parsing for Databricks queries."""

        # Test basic ODBC date literal
        self.validate_all(
            "SELECT DATE('2025-05-31')",
            read={
                "databricks": "SELECT {d '2025-05-31'}",
            },
            write={
                "databricks": "SELECT DATE('2025-05-31')",
                "e6": "SELECT DATE('2025-05-31')",
                "spark": "SELECT DATE('2025-05-31')",
            },
        )

        # Test ODBC date literal in WHERE clause
        self.validate_all(
            "SELECT * FROM t WHERE d = DATE('2025-05-31')",
            read={
                "databricks": "SELECT * FROM t WHERE d = {d '2025-05-31'}",
            },
            write={
                "databricks": "SELECT * FROM t WHERE d = DATE('2025-05-31')",
                "e6": "SELECT * FROM t WHERE d = DATE('2025-05-31')",
            },
        )

        # Test ODBC date literal in IN clause (single value)
        self.validate_all(
            "SELECT * FROM t WHERE d IN (DATE('2025-05-31'))",
            read={
                "databricks": "SELECT * FROM t WHERE d IN ({d '2025-05-31'})",
            },
            write={
                "databricks": "SELECT * FROM t WHERE d IN (DATE('2025-05-31'))",
                "e6": "SELECT * FROM t WHERE d IN (DATE('2025-05-31'))",
            },
        )

        # Test ODBC date literals in IN clause (multiple values)
        self.validate_all(
            "SELECT * FROM t WHERE d IN (DATE('2025-05-31'), DATE('2025-06-01'), DATE('2025-06-02'))",
            read={
                "databricks": "SELECT * FROM t WHERE d IN ({d '2025-05-31'}, {d '2025-06-01'}, {d '2025-06-02'})",
            },
            write={
                "databricks": "SELECT * FROM t WHERE d IN (DATE('2025-05-31'), DATE('2025-06-01'), DATE('2025-06-02'))",
                "e6": "SELECT * FROM t WHERE d IN (DATE('2025-05-31'), DATE('2025-06-01'), DATE('2025-06-02'))",
            },
        )

        # Test ODBC time literal
        self.validate_all(
            "SELECT TIME('14:30:45')",
            read={
                "databricks": "SELECT {t '14:30:45'}",
            },
            write={
                "databricks": "SELECT TIME('14:30:45')",
                "e6": "SELECT TIME('14:30:45')",
            },
        )

        # Test ODBC timestamp literal
        self.validate_all(
            "SELECT TIMESTAMP('2025-05-31 14:30:45')",
            read={
                "databricks": "SELECT {ts '2025-05-31 14:30:45'}",
            },
            write={
                "databricks": "SELECT TIMESTAMP('2025-05-31 14:30:45')",
                "e6": "SELECT TIMESTAMP('2025-05-31 14:30:45')",
            },
        )

        # Test ODBC timestamp literals in IN clause
        self.validate_all(
            "SELECT * FROM t WHERE ts IN (TIMESTAMP('2025-05-31 14:30:45'), TIMESTAMP('2025-06-01 09:15:00'))",
            read={
                "databricks": "SELECT * FROM t WHERE ts IN ({ts '2025-05-31 14:30:45'}, {ts '2025-06-01 09:15:00'})",
            },
            write={
                "databricks": "SELECT * FROM t WHERE ts IN (TIMESTAMP('2025-05-31 14:30:45'), TIMESTAMP('2025-06-01 09:15:00'))",
                "e6": "SELECT * FROM t WHERE ts IN (TIMESTAMP('2025-05-31 14:30:45'), TIMESTAMP('2025-06-01 09:15:00'))",
            },
        )

        # Test complex query with many ODBC date literals (similar to the original failing query)
        self.validate_all(
            "SELECT inventory_source_name, SUM(CAST(bid AS DOUBLE)) AS C1 FROM trader_db WHERE event_time IN (DATE('2025-05-31'), DATE('2025-06-14'), DATE('2025-07-11'), DATE('2025-06-01'), DATE('2025-06-15')) GROUP BY inventory_source_name",
            read={
                "databricks": "SELECT inventory_source_name, SUM(CAST(bid AS DOUBLE)) AS C1 FROM trader_db WHERE event_time IN ({d '2025-05-31'}, {d '2025-06-14'}, {d '2025-07-11'}, {d '2025-06-01'}, {d '2025-06-15'}) GROUP BY inventory_source_name",
            },
            write={
                "e6": "SELECT inventory_source_name, SUM(CAST(bid AS DOUBLE)) AS C1 FROM trader_db WHERE event_time IN (DATE('2025-05-31'), DATE('2025-06-14'), DATE('2025-07-11'), DATE('2025-06-01'), DATE('2025-06-15')) GROUP BY inventory_source_name",
            },
        )

        # Test mixed date and time literals
        self.validate_all(
            "SELECT * FROM events WHERE event_date = DATE('2025-05-31') AND event_time = TIME('14:30:00')",
            read={
                "databricks": "SELECT * FROM events WHERE event_date = {d '2025-05-31'} AND event_time = {t '14:30:00'}",
            },
            write={
                "e6": "SELECT * FROM events WHERE event_date = DATE('2025-05-31') AND event_time = TIME('14:30:00')",
            },
        )

        # Test ODBC literals in BETWEEN clause
        self.validate_all(
            "SELECT * FROM orders WHERE order_date BETWEEN DATE('2025-01-01') AND DATE('2025-12-31')",
            read={
                "databricks": "SELECT * FROM orders WHERE order_date BETWEEN {d '2025-01-01'} AND {d '2025-12-31'}",
            },
            write={
                "e6": "SELECT * FROM orders WHERE order_date BETWEEN DATE('2025-01-01') AND DATE('2025-12-31')",
            },
        )

    def test_interval_cast_transformation(self):
        """Test interval cast transformations for ::INTERVAL casting"""

        # Test plural to singular conversion - hours to hour
        self.validate_all(
            "INTERVAL col1 'hour'",
            read={
                "databricks": "(col1 || ' hours')::INTERVAL",
            },
        )

        # Test plural to singular conversion - minutes to minute
        self.validate_all(
            "INTERVAL col1 'minute'",
            read={
                "databricks": "((col1) || ' minutes')::INTERVAL",
            },
        )

        # Test plural to singular conversion - days to day
        self.validate_all(
            "INTERVAL (col1 + col2) 'day'",
            read={
                "databricks": "((col1 + col2) || ' days')::INTERVAL",
            },
        )

        # Test plural to singular conversion - seconds to second
        self.validate_all(
            "INTERVAL (col1 * 2 + col2 / 3) 'second'",
            read={
                "databricks": "((col1 * 2 + col2 / 3) || ' seconds')::INTERVAL",
            },
        )

        # Test plural to singular conversion - weeks to week
        self.validate_all(
            "INTERVAL (ROUND(col1, 2)) 'week'",
            read={
                "databricks": "(ROUND(col1, 2) || ' weeks')::INTERVAL",
            },
        )

        # Test plural to singular conversion - months to month
        self.validate_all(
            "INTERVAL col1 'month'",
            read={
                "databricks": "(col1 || ' months')::INTERVAL",
            },
        )

        # Test plural to singular conversion - years to year
        self.validate_all(
            "INTERVAL col1 'year'",
            read={
                "databricks": "(col1 || ' years')::INTERVAL",
            },
        )

        # Test singular units remain singular - hour stays hour
        self.validate_all(
            "INTERVAL col1 'hour'",
            read={
                "databricks": "(col1 || ' hour')::INTERVAL",
            },
        )

        # Test singular units remain singular - day stays day
        self.validate_all(
            "INTERVAL col1 'day'",
            read={
                "databricks": "(col1 || ' day')::INTERVAL",
            },
        )

        # Test with invalid/non-standard time units (should still work)
        self.validate_all(
            "INTERVAL col1 'invalid_unit'",
            read={
                "databricks": "(col1 || ' invalid_unit')::INTERVAL",
            },
        )

        # Test multiple interval expressions with plural conversion
        self.validate_all(
            "SELECT INTERVAL col1 'hour', INTERVAL col2 'minute'",
            read={
                "databricks": "SELECT (col1 || ' hours')::INTERVAL, (col2 || ' minutes')::INTERVAL",
            },
        )

        # Test in WHERE clause with plural conversion
        self.validate_all(
            "SELECT * FROM events WHERE duration > INTERVAL col1 'hour'",
            read={
                "databricks": "SELECT * FROM events WHERE duration > (col1 || ' hours')::INTERVAL",
            },
        )

        # Test case insensitive plural conversion - HOURS to hour
        self.validate_all(
            "INTERVAL col1 'hour'",
            read={
                "databricks": "(col1 || ' HOURS')::INTERVAL",
            },
        )

        # Test microseconds to microsecond conversion
        self.validate_all(
            "INTERVAL col1 'microsecond'",
            read={
                "databricks": "(col1 || ' microseconds')::INTERVAL",
            },
        )

        self.validate_all(
            'SELECT ean "STRING" FROM silver_postgres_v2.thor_inbound.inbound_sku',
            read={
                "databricks": "SELECT ean STRING FROM silver_postgres_v2.thor_inbound.inbound_sku"
            },
        )

        self.validate_all(
            'SELECT ean "INT" FROM silver_postgres_v2.thor_inbound.inbound_sku',
            read={"databricks": "SELECT ean INT FROM silver_postgres_v2.thor_inbound.inbound_sku"},
        )
        self.validate_all(
            'SELECT ean "FLOAT" FROM silver_postgres_v2.thor_inbound.inbound_sku',
            read={
                "databricks": "SELECT ean FLOAT FROM silver_postgres_v2.thor_inbound.inbound_sku"
            },
        )
        self.validate_all(
            'SELECT ean "DECIMAL" FROM silver_postgres_v2.thor_inbound.inbound_sku',
            read={
                "databricks": "SELECT ean DECIMAL FROM silver_postgres_v2.thor_inbound.inbound_sku"
            },
        )

    def test_table_alias_qualification(self):
        """Test table alias qualification for LEFT JOIN with USING clause"""
        from sqlglot.dialects.e6 import E6

        # Enable the feature flag
        E6.ENABLE_TABLE_ALIAS_QUALIFICATION = True

        try:
            # Test 1: Simple LEFT JOIN with USING - columns should be qualified
            self.validate_all(
                "SELECT pv.start_tstamp_date, pv.vehicle_vin FROM vehicle_parked_view AS pv LEFT JOIN parked_well AS pw USING (vehicle_vin) WHERE pv.start_tstamp_date >= '2024-06-06'",
                read={
                    "databricks": "SELECT start_tstamp_date, vehicle_vin FROM vehicle_parked_view pv LEFT JOIN parked_well pw USING (vehicle_vin) WHERE start_tstamp_date >= '2024-06-06'",
                },
            )

            # Test 2: INNER JOIN with USING - columns should NOT be qualified
            self.validate_all(
                "SELECT user_id, session_id FROM sessions AS s INNER JOIN activities AS a USING (session_id)",
                read={
                    "databricks": "SELECT user_id, session_id FROM sessions s INNER JOIN activities a USING (session_id)",
                },
            )

            # Test 3: LEFT JOIN with ON - columns should NOT be qualified
            self.validate_all(
                "SELECT user_id, session_id FROM sessions AS s LEFT JOIN activities AS a ON s.session_id = a.session_id",
                read={
                    "databricks": "SELECT user_id, session_id FROM sessions s LEFT JOIN activities a ON s.session_id = a.session_id",
                },
            )

            # Test 4: Nested query with LEFT JOIN in inner query
            self.validate_all(
                "SELECT user_id, order_count FROM (SELECT o.user_id, o.order_id, o.order_count FROM orders AS o LEFT JOIN order_details AS od USING (order_id) WHERE o.order_date >= '2024-01-01') AS subquery WHERE order_count > 10",
                read={
                    "databricks": "SELECT user_id, order_count FROM (SELECT user_id, order_id, order_count FROM orders o LEFT JOIN order_details od USING (order_id) WHERE order_date >= '2024-01-01') AS subquery WHERE order_count > 10",
                },
            )

            # Test 5: Nested query with LEFT JOIN in outer query (subquery in FROM)
            self.validate_all(
                "SELECT pv.user_id, pv.session_count FROM (SELECT user_id, COUNT(*) AS session_count FROM sessions GROUP BY user_id) AS pv LEFT JOIN users AS u USING (user_id) WHERE pv.session_count > 5",
                read={
                    "databricks": "SELECT user_id, session_count FROM (SELECT user_id, COUNT(*) AS session_count FROM sessions GROUP BY user_id) pv LEFT JOIN users u USING (user_id) WHERE session_count > 5",
                },
            )

            # Test 6: Real-world CTE query with LEFT JOIN and aggregates
            self.validate_all(
                "WITH pw AS (SELECT dt AS start_tstamp_date, app_id, domain_userid, domain_sessionid, web_page.id AS page_view_id, CASE messaging_unit_event.is_exceeded WHEN TRUE THEN 'final barrier' ELSE 'dismissable growler' END AS meter_unit_type FROM silver_eu_prod.spruce.slv_core_events WHERE event_name = 'messaging_unit_event' AND messaging_unit_event.subject LIKE 'paywall%' AND dt = CAST('2025-10-01' AS DATE) AND messaging_unit_event.type = 'impression' AND app_id IN ('vogue-bz')) SELECT pv.start_tstamp_date, YEAR(TO_DATE(pv.start_tstamp_date)) AS Year, MONTH(TO_DATE(pv.start_tstamp_date)) AS Month, CAST(DATE_TRUNC('WEEK', pv.start_tstamp_date) AS DATE) AS Week, pv.app_id, COUNT(DISTINCT pv.domain_userid) AS uvs FROM gold_eu_prod.spruce.gld_web_page_views AS pv LEFT JOIN pw USING (app_id, start_tstamp_date, page_view_id) WHERE pv.start_tstamp_date = CAST('2025-10-01' AS DATE) AND pv.app_id IN ('vogue-bz') GROUP BY ALL",
                read={
                    "databricks": """
                    with pw as (
                        SELECT
                          dt AS start_tstamp_date
                          ,app_id
                          ,domain_userid
                          ,domain_sessionid
                          ,web_page.id as page_view_id
                          ,case messaging_unit_event.is_exceeded when true then 'final barrier' else 'dismissable growler' end as meter_unit_type
                        FROM
                        silver_eu_prod.spruce.slv_core_events
                        WHERE
                          event_name = 'messaging_unit_event'
                          and messaging_unit_event.subject like 'paywall%'
                          and dt = Date('2025-10-01')
                          and messaging_unit_event.type ='impression'
                          and app_id IN('vogue-bz'))
                    SELECT
                        start_tstamp_date,
                        year(start_tstamp_date) AS Year,
                        MONTH(start_tstamp_date) AS Month,
                        CAST(date_trunc('WEEK', start_tstamp_date) AS DATE) AS Week,
                        app_id
                        ,COUNT(distinct pv.domain_userid) as uvs
                    FROM gold_eu_prod.spruce.gld_web_page_views pv
                    left join pw using (app_id,start_tstamp_date,page_view_id)
                    WHERE
                      start_tstamp_date = Date('2025-10-01')
                      and app_id IN('vogue-bz')
                    GROUP BY  all
                    """,
                },
            )

        finally:
            # Always reset flag to default after tests
            E6.ENABLE_TABLE_ALIAS_QUALIFICATION = False

    def test_double_quotes(self):
        self.validate_all(
            "GREATEST(AVG(voluntary_cancellation_mrr.'CANCEL FROM PAID'), 0) * 0.15"
            if os.getenv("PRESERVE_DOUBLE_QUOTES_AROUND_IDENTIFIERS_DBR", "false").lower()
            == "false"
            else 'GREATEST(AVG(voluntary_cancellation_mrr."CANCEL FROM PAID"), 0) * 0.15',
            read={
                "databricks": """ GREATEST( AVG( voluntary_cancellation_mrr."CANCEL FROM PAID" ), 0 ) * 0.15 """
            },
        )
        self.validate_all(
            "INTERVAL ('time_col') 'hour'"
            if os.getenv("PRESERVE_DOUBLE_QUOTES_AROUND_IDENTIFIERS_DBR", "false").lower()
            == "false"
            else "INTERVAL \"time_col\" 'hour'",
            read={
                "databricks": "(\"time_col\" || ' hours')::INTERVAL",
            },
        )
