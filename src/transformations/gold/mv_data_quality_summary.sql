-- GOLD: Data quality monitoring table (refreshes every run).
CREATE OR REFRESH MATERIALIZED VIEW mv_data_quality_summary
COMMENT 'Daily data quality indicators for the cleaned sales table.'
AS SELECT
  business_date,
  COUNT(*)                                                   AS rows_loaded,
  COUNT(DISTINCT order_id)                                   AS orders,
  SUM(CASE WHEN order_hour IS NULL THEN 1 ELSE 0 END)        AS missing_hour,
  SUM(CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END)     AS missing_channel,
  SUM(CASE WHEN discount_amount > gross_amount THEN 1 ELSE 0 END) AS discount_above_price,
  COUNT(*) - COUNT(DISTINCT order_id, menu_item, quantity, line_revenue, discount_amount) AS possible_duplicate_lines,
  SUM(CASE WHEN store_id IS NULL THEN 1 ELSE 0 END)         AS missing_store,
  SUM(CASE WHEN discount_parse_failed THEN 1 ELSE 0 END)     AS discount_not_numeric,
  ROUND(SUM(line_revenue), 2)                                AS computed_revenue_usd,
  ROUND(SUM(check_total_reported), 2)                        AS raw_total_column_sum,
  MAX(_ingested_at)                                          AS last_ingested_at
FROM st_sales_cleaned
GROUP BY business_date;
