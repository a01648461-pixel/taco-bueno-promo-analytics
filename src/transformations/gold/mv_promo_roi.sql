-- GOLD (Analysis H): Revenue generated per $1 of discount, with a Live/Die label.
CREATE OR REFRESH MATERIALIZED VIEW mv_promo_roi
COMMENT 'Discount efficiency per menu item and recommended action.'
AS SELECT
  menu_item,
  total_units,
  revenue_usd,
  discount_usd,
  discount_dependency_pct,
  ROUND(revenue_usd / NULLIF(discount_usd, 0), 2) AS revenue_per_discount_dollar,
  CASE
    WHEN discount_usd = 0                                             THEN 'No promo'
    WHEN revenue_usd / discount_usd < 6                               THEN 'DIE'
    WHEN discount_dependency_pct >= 70                                THEN 'DIE - trains customers to wait for discounts'
    WHEN revenue_usd / discount_usd >= 10 AND discount_dependency_pct < 40 THEN 'LIVE - scale up'
    ELSE 'REVIEW'
  END AS recommendation
FROM mv_discount_dependency
WHERE total_units >= 1000;
