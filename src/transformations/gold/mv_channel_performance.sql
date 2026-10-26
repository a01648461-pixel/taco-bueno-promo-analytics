-- GOLD (Analysis C + G): Sales channel performance and promo usage by channel.
CREATE OR REFRESH MATERIALIZED VIEW mv_channel_performance
COMMENT 'Orders, revenue, average ticket and promo share per sales channel.'
AS SELECT
  sales_channel,
  COUNT(*)                                      AS orders,
  SUM(units)                                    AS units,
  ROUND(SUM(basket_total_usd), 2)               AS revenue_usd,
  ROUND(AVG(basket_total_usd), 2)               AS avg_ticket_usd,
  ROUND(SUM(discount_usd), 2)                   AS discount_usd,
  ROUND(100 * AVG(CAST(has_promo AS INT)), 2)   AS pct_orders_with_promo
FROM mv_order_summary
GROUP BY sales_channel;
