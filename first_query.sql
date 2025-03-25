/* enginenet:: edbb1c628c */
WITH date_item_store AS (
  SELECT
    COALESCE(
      Sum(
        CASE WHEN week_offset = -1 THEN pos_sales_this_year END
      ),
      0
    ) AS "value",
    Try_divide(
      COALESCE(
        Sum(
          CASE WHEN week_offset <= -2
          AND week_offset >= -5 THEN pos_sales_this_year END
        ),
        0
      ),
      4
    ) AS comp_value,
    Count(
      CASE WHEN week_offset = -1 THEN walmart_item_number END
    ) AS row_cnt,
    Try_divide(
      Count(
        CASE WHEN w.week_offset BETWEEN -5
        AND -2 THEN walmart_item_number END
      ),
      4
    ) AS l4w_row_cnt
  FROM
    onretail.onretail_anthem_snacks_luminate_basic_sales.date_item_store AS wis
    INNER JOIN common_walmart."default".view_weeks_dates AS w ON wis.business_date = w.cal_date
)
SELECT
  CASE WHEN Abs(
    (row_cnt - l4w_row_cnt) / NULLIF(l4w_row_cnt, 0)
  ) < 0.5
  /* sales within threshold of L4W Avg */
  AND row_cnt > 0 THEN true WHEN Abs(
    ('value' - comp_value) / NULLIF(comp_value, 0)
  ) < 0.5
  /* sales within threshold of L4W Avg */
  AND row_cnt > 0 THEN true WHEN 'value' > comp_value THEN true ELSE false END AS is_valid,
  'date_item_store' AS table_names,
  CASE WHEN l4w_row_cnt = 0 THEN 'L4W Row Count 0' ELSE 'value:' || 'value' || ', comp:' || comp_value END AS message
FROM
  date_item_store



------------------------------


/* enginenet:: edbb1c628c */
WITH date_item_store AS (
  SELECT
    COALESCE(
      SUM(
        CASE WHEN week_offset = -1 THEN pos_sales_this_year END
      ),
      0
    ) AS "value",
    TRY_DIVIDE(
      COALESCE(
        SUM(
          CASE WHEN week_offset <= -2
          AND week_offset >= -5 THEN pos_sales_this_year END
        ),
        0
      ),
      4
    ) AS comp_value,
    COUNT(
      CASE WHEN week_offset = -1 THEN walmart_item_number END
    ) AS row_cnt,
    TRY_DIVIDE(
      COUNT(
        CASE WHEN w.week_offset BETWEEN -5
        AND -2 THEN walmart_item_number END
      ),
      4
    ) AS l4w_row_cnt
  FROM
    onretail.onretail_anthem_snacks_luminate_basic_sales.date_item_store AS wis
    INNER JOIN common_walmart."default".view_weeks_dates AS w ON wis.business_date = w.cal_date
)
SELECT
  CASE WHEN ABS(
    (row_cnt - l4w_row_cnt) / NULLIF(l4w_row_cnt, 0)
  ) < 0.5
  AND
  /* sales within threshold of L4W Avg */
  row_cnt > 0 THEN TRUE WHEN ABS(
    ('value' - comp_value) / NULLIF(comp_value, 0)
  ) < 0.5
  AND
  /* sales within threshold of L4W Avg */
  row_cnt > 0 THEN TRUE WHEN 'value' > comp_value THEN TRUE ELSE FALSE END AS is_valid,
  'date_item_store' AS table_names,
  CASE WHEN l4w_row_cnt = 0 THEN 'L4W Row Count 0' ELSE 'value:' || 'value' || ', comp:' || comp_value END AS message
FROM
  date_item_store
