import sqlglot

sql_query = f"""
WITH link AS (
    SELECT 
        *
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
),
te_transformed AS (
    SELECT
        id,
        random_value,
        date_generated,
        CASE 
            WHEN MOD(id, 2) = 0 THEN 'even'
            ELSE 'odd'
        END AS id_parity,
        IFF(random_value > 50, 'high', 'low') AS random_category,
        TO_VARCHAR(date_generated, 'YYYY-MM-DD') AS formatted_date,
        DATE_PART('WEEK', date_generated) AS week_of_year,
        ARRAY_SIZE(array_example) AS array_size,
        ROW_NUMBER() OVER (ORDER BY random_value DESC) AS row_num,
        HASH(random_value::STRING) AS hash_value
    FROM cte_data
),
te_final AS (
    SELECT
        id,
        random_value,
        id_parity,
        random_category,
        formatted_date,
        week_of_year,
        array_size,
        row_num,
        hash_value,
        OBJECT_AGG(random_category, random_value) OVER () AS aggregated_object
    FROM cte_transformed
)
SELECT
    id,
    random_value,
    id_parity,
    random_category,
    formatted_date,
    week_of_year,
    array_size,
    row_num,
    hash_value,
    aggregated_object,
    JSON_PARSE(TO_JSON(OBJECT_CONSTRUCT(*))) AS json_representation
FROM te_final
WHERE random_value > 20
ORDER BY random_value DESC
LIMIT 10;

"""

thing = sqlglot.parse(sql_query,"snowflake", error_level=None)
print(thing)
