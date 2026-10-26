-- GOLD (Analysis A): Top revenue & best-selling menu items.
CREATE OR REFRESH MATERIALIZED VIEW mv_top_menu_items
CLUSTER BY (menu_item)
COMMENT 'Units, gross and net revenue per menu item.'
AS SELECT
  menu_item,
  SUM(quantity)                    AS total_units,
  ROUND(SUM(gross_amount), 2)      AS gross_revenue_usd,
  ROUND(SUM(discount_amount), 2)   AS discount_usd,
  ROUND(SUM(line_revenue), 2)      AS net_revenue_usd,
  COUNT(DISTINCT order_id)         AS orders
FROM st_sales_cleaned
GROUP BY menu_item;
