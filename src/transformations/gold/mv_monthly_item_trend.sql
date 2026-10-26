-- GOLD (Analysis J): Monthly trend per item to spot promos that should COME BACK
-- (sales dropped after the discount was removed).
CREATE OR REFRESH MATERIALIZED VIEW mv_monthly_item_trend
CLUSTER BY (menu_item)
COMMENT 'Monthly units, promo units and revenue per menu item.'
AS SELECT
  menu_item,
  DATE_TRUNC('MONTH', business_date)                      AS month,
  SUM(quantity)                                           AS units,
  SUM(CASE WHEN has_discount THEN quantity ELSE 0 END)    AS promo_units,
  ROUND(SUM(line_revenue), 2)                               AS revenue_usd
FROM st_sales_cleaned
GROUP BY menu_item, DATE_TRUNC('MONTH', business_date);
