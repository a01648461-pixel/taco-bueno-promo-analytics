-- Databricks notebook source
-- Run ONCE on the Phase 1 table before the first pipeline run.
-- It answers 3 questions that change the numbers in the presentation.

-- COMMAND ----------
-- 1. Which columns exist? Look for the STORE and DATE columns.
DESCRIBE workspace.default.ventas_taco_bueno_clean;

-- COMMAND ----------
-- 2. Does Check_Number repeat across stores/days?
--    If checks_with_many_dates is large, grouping by Check_Number alone
--    merged different orders (this inflated Phase 1 basket sizes).
--    >>> Replace Business_Date with your date column name <<<
SELECT
  COUNT(DISTINCT Check_Number) AS distinct_check_numbers,
  COUNT(*) AS lines,
  SUM(CASE WHEN n_dates > 1 THEN 1 ELSE 0 END) AS checks_with_many_dates
FROM (
  SELECT Check_Number, COUNT(DISTINCT Business_Date) AS n_dates
  FROM workspace.default.ventas_taco_bueno_clean
  GROUP BY Check_Number
);

-- COMMAND ----------
-- 3. Is "Total" a LINE total or the CHECK total repeated on every line?
--    If same_total_every_line_pct is close to 100, Total is check-level and
--    SUM(Total) over lines overstated revenue in Analyses C, F and H.
WITH chk AS (
  SELECT Check_Number,
         COUNT(*) AS lines,
         COUNT(DISTINCT Total) AS distinct_totals,
         SUM(TRY_CAST(Quantity AS DOUBLE) * TRY_CAST(Menu_Price AS DOUBLE)
             - COALESCE(TRY_CAST(Item_Discount AS DOUBLE), 0)) AS computed_lines
  FROM workspace.default.ventas_taco_bueno_clean
  GROUP BY Check_Number
)
SELECT
  ROUND(100 * AVG(CASE WHEN distinct_totals = 1 THEN 1 ELSE 0 END), 1) AS same_total_every_line_pct
FROM chk
WHERE lines > 1;

-- COMMAND ----------
-- 4. Look at one multi-line check to confirm visually
SELECT Check_Number, Menu_Item, Quantity, Menu_Price, Item_Discount, Total
FROM workspace.default.ventas_taco_bueno_clean
WHERE Check_Number = (
  SELECT Check_Number FROM workspace.default.ventas_taco_bueno_clean
  GROUP BY Check_Number HAVING COUNT(*) BETWEEN 3 AND 6 LIMIT 1
);
