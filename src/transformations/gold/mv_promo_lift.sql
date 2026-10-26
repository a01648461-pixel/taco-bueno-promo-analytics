-- GOLD (Analysis D): Do promos increase basket size? Split by channel too.
CREATE OR REFRESH MATERIALIZED VIEW mv_promo_lift
COMMENT 'Average basket with vs without promo, overall and by channel.'
AS SELECT
  sales_channel,
  CASE WHEN has_promo THEN 'With promo' ELSE 'Without promo' END AS ticket_type,
  COUNT(*)                                  AS orders,
  ROUND(SUM(basket_total_usd), 2)           AS revenue_usd,
  ROUND(AVG(basket_total_usd), 2)           AS avg_basket_usd,
  ROUND(PERCENTILE_APPROX(basket_total_usd, 0.5), 2) AS median_basket_usd,
  ROUND(AVG(units), 2)                      AS avg_units
FROM mv_order_summary
GROUP BY sales_channel, has_promo;
