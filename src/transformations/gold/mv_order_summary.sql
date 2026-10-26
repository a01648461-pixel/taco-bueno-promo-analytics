-- GOLD: one row per order (check). Base for lift and basket analyses.
-- Uses order_id (store|date|check) so the same check number in two stores
-- is not merged into one giant "basket".
CREATE OR REFRESH MATERIALIZED VIEW mv_order_summary
CLUSTER BY (business_date)
COMMENT 'One row per order with basket size, discount and channel.'
AS SELECT
  order_id,
  store_id,
  business_date,
  MIN(order_hour)            AS order_hour,
  MIN(daypart)               AS daypart,
  MIN(sales_channel)         AS sales_channel,
  SUM(quantity)              AS units,
  COUNT(DISTINCT menu_item)  AS distinct_items,
  SUM(line_revenue)          AS basket_total_usd,
  MAX(check_total_reported)  AS check_total_reported,
  SUM(discount_amount)       AS discount_usd,
  MAX(CAST(has_discount AS INT)) = 1 AS has_promo
FROM st_sales_cleaned
GROUP BY order_id, store_id, business_date;
