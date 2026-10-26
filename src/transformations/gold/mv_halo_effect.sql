-- GOLD (Analysis I): What do customers add when they buy a discounted item?
CREATE OR REFRESH MATERIALIZED VIEW mv_halo_effect
COMMENT 'Top companion items bought in the same order as each discounted item.'
AS WITH promo_lines AS (
  SELECT DISTINCT order_id, menu_item AS promo_item
  FROM st_sales_cleaned
  WHERE has_discount
),
pairs AS (
  SELECT p.promo_item, s.menu_item AS companion_item,
         COUNT(DISTINCT s.order_id)  AS orders_together,
         ROUND(SUM(s.line_revenue), 2) AS companion_revenue_usd
  FROM promo_lines p
  JOIN st_sales_cleaned s ON s.order_id = p.order_id
  WHERE s.menu_item <> p.promo_item
  GROUP BY p.promo_item, s.menu_item
)
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_item ORDER BY orders_together DESC) AS rank_num
  FROM pairs
) WHERE rank_num <= 5;
