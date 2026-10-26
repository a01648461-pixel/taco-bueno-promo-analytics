-- Databricks notebook source
-- Post-pipeline validation. RAISE_ERROR fails the job task, which triggers
-- the failure email, so nobody presents broken numbers.
-- Run with the job's catalog/schema = workspace.taco_bueno

USE workspace.taco_bueno;

-- COMMAND ----------
-- 1. Gold tables are not empty
SELECT CASE WHEN COUNT(*) = 0 THEN RAISE_ERROR('mv_order_summary is empty') END
FROM mv_order_summary;

-- COMMAND ----------
-- 2. Order key is unique in the order summary
SELECT CASE WHEN COUNT(*) <> COUNT(DISTINCT order_id)
            THEN RAISE_ERROR('Duplicate order_id in mv_order_summary') END
FROM mv_order_summary;

-- COMMAND ----------
-- 3. Revenue reconciles between silver and gold (tolerance 0.01%)
SELECT CASE WHEN ABS(s.rev - g.rev) > 0.0001 * s.rev
            THEN RAISE_ERROR('Revenue mismatch between silver and gold') END
FROM (SELECT SUM(line_revenue) AS rev FROM st_sales_cleaned) s,
     (SELECT SUM(basket_total_usd) AS rev FROM mv_order_summary) g;

-- COMMAND ----------
-- 4. Hours are in range and not mostly missing
SELECT CASE WHEN SUM(CASE WHEN order_hour IS NULL THEN 1 ELSE 0 END) > 0.05 * COUNT(*)
            THEN RAISE_ERROR('More than 5% of orders have no hour') END
FROM mv_order_summary;

-- COMMAND ----------
-- 5. Summary for the run log
SELECT * FROM mv_data_quality_summary ORDER BY business_date DESC LIMIT 14;
