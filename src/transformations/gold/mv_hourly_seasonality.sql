-- GOLD (Analysis F/K): Orders and sales by day of week and 24h hour.
CREATE OR REFRESH MATERIALIZED VIEW mv_hourly_seasonality
COMMENT 'Orders, revenue and promo share by day of week, hour and daypart.'
AS SELECT
  DATE_FORMAT(business_date, 'EEEE') AS day_of_week,
  DAYOFWEEK(business_date)           AS day_num,
  order_hour,
  daypart,
  COUNT(*)                           AS orders,
  ROUND(SUM(basket_total_usd), 2)    AS revenue_usd,
  ROUND(AVG(basket_total_usd), 2)    AS avg_basket_usd,
  ROUND(100 * AVG(CAST(has_promo AS INT)), 2) AS pct_orders_with_promo
FROM mv_order_summary
GROUP BY DATE_FORMAT(business_date, 'EEEE'), DAYOFWEEK(business_date), order_hour, daypart;
