-- GOLD (Analysis E): Cannibalization. Share of each item's units sold on discount.
CREATE OR REFRESH MATERIALIZED VIEW mv_discount_dependency
COMMENT 'Per menu item: units on discount vs full price and discount dependency %.'
AS SELECT
  menu_item,
  SUM(quantity)                                              AS total_units,
  SUM(CASE WHEN has_discount THEN quantity ELSE 0 END)       AS discounted_units,
  SUM(CASE WHEN NOT has_discount THEN quantity ELSE 0 END)   AS full_price_units,
  ROUND(100 * SUM(CASE WHEN has_discount THEN quantity ELSE 0 END) / SUM(quantity), 2)
                                                             AS discount_dependency_pct,
  ROUND(SUM(line_revenue), 2)                                  AS revenue_usd,
  ROUND(SUM(discount_amount), 2)                             AS discount_usd
FROM st_sales_cleaned
GROUP BY menu_item;
