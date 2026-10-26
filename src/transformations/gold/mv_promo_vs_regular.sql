-- GOLD (Analysis B): Discounted vs regular-price lines.
CREATE OR REFRESH MATERIALIZED VIEW mv_promo_vs_regular
COMMENT 'Volume, units per line and revenue for discounted vs regular-price lines.'
AS SELECT
  CASE WHEN has_discount THEN 'Discounted' ELSE 'Regular price' END AS pricing_type,
  COUNT(DISTINCT order_id)          AS orders,
  SUM(quantity)                     AS units,
  ROUND(AVG(quantity), 2)           AS avg_units_per_line,
  ROUND(SUM(discount_amount), 2)    AS discounts_given_usd,
  ROUND(SUM(line_revenue), 2)       AS net_revenue_usd
FROM st_sales_cleaned
GROUP BY has_discount;
