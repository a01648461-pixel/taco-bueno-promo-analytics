# Databricks notebook source
# SILVER: cleaned, typed and validated sales lines (streaming table)
# ------------------------------------------------------------
# Phase 1 equivalent: cell "Standarize columns name, data cleaning, create unified table"
# (void filters) + all the CAST(... AS DOUBLE) we had to repeat in every query.
#
# Improvements over Phase 1:
#   1. Voided orders/items are removed through EXPECTATIONS, so the pipeline UI
#      reports how many rows were dropped (evidence for data quality).
#   2. All numeric columns are typed once with try_cast -> no more
#      "'0.0' STRING cannot be cast to BIGINT" errors downstream.
#   3. Begin_Time ("9:31 AM") becomes a 0-23 hour. Phase 1 sorted by AM/PM text,
#      which placed 12 AM between 11 AM and 1 PM and 12 PM at the end of the chart.
#   4. A unique order_id = store + date + check. Check numbers restart per store
#      and per day, so grouping by Check_Number alone merges different orders.
#   5. line_revenue is computed from Quantity x Menu_Price - Item_Discount.
#      The raw "Total" column is kept as check_total_reported and reconciled
#      in mv_data_quality_summary (see README: it may be a check-level total).
#
# Store and date columns: auto-detected from common names. If yours is named
# differently, set the pipeline configuration keys "store_column" / "date_column".

import dlt
from pyspark.sql import functions as F

STORE_CANDIDATES = ["Store_Number", "Store_num", "Store", "Store_ID", "StoreNumber",
                    "Location", "Location_ID", "Location_num", "Unit", "Unit_num",
                    "Restaurant", "Site", "Store_Name"]
DATE_CANDIDATES = ["Business_Date", "Date", "Order_Date", "Check_Date", "DOB",
                   "Transaction_Date", "Open_Date", "Sales_Date", "Day"]


def pick(columns, candidates, override_key):
    override = spark.conf.get(override_key, "")
    if override:
        return override
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def num(c):
    return F.expr(f"try_cast(`{c}` AS DOUBLE)")


def flag_true(c):
    # Order_Voided / Item_Voided: 0 / 1 / True / False / null
    return F.coalesce(num(c), F.when(F.lower(F.col(c)).isin("true", "y", "yes"), 1.0),
                      F.lit(0.0)) > 0


# ---- Expectations -------------------------------------------------------
# expect_all_or_drop : bad rows are removed and counted in the pipeline UI
# expect_all         : rows are kept, violations are counted (monitoring)
DROP_RULES = {
    "order_not_voided": "order_voided = false",
    "item_not_voided": "item_voided = false",
    "valid_check_number": "check_number IS NOT NULL AND check_number <> ''",
    "valid_menu_item": "menu_item IS NOT NULL AND trim(menu_item) <> ''",
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_price": "menu_price IS NOT NULL AND menu_price >= 0",
}
WARN_RULES = {
    "has_business_date": "business_date IS NOT NULL",
    "has_store": "store_id IS NOT NULL",
    "valid_hour": "order_hour BETWEEN 0 AND 23",
    "has_channel": "sales_channel IS NOT NULL",
    "discount_not_above_price": "discount_amount <= gross_amount + 0.01",
    "discount_was_numeric": "discount_parse_failed = false",
}


@dlt.table(
    name="st_sales_cleaned",
    comment="Cleaned Taco Bueno sales lines: voids removed, numeric types, 24h hour, unique order_id.",
    table_properties={"quality": "silver"},
    cluster_by=["business_date", "menu_item"],   # liquid clustering on the most used filters
)
@dlt.expect_all_or_drop(DROP_RULES)
@dlt.expect_all(WARN_RULES)
def st_sales_cleaned():
    df = dlt.read_stream("st_sales_raw")
    store_col = pick(df.columns, STORE_CANDIDATES, "store_column")
    date_col = pick(df.columns, DATE_CANDIDATES, "date_column")

    store_expr = F.trim(F.col(store_col)) if store_col else F.lit(None).cast("string")
    date_expr = (
        F.coalesce(
            F.expr(f"try_to_timestamp(`{date_col}`, 'M/d/yyyy')"),
            F.expr(f"try_to_timestamp(`{date_col}`, 'M/d/yyyy h:mm:ss a')"),
            F.expr(f"try_to_timestamp(`{date_col}`, 'yyyy-MM-dd')"),
            F.expr(f"try_to_timestamp(`{date_col}`, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr(f"try_to_timestamp(`{date_col}`, 'M/d/yy')"),
            F.expr(f"try_to_timestamp(`{date_col}`, 'yyyyMMdd')"),
        ).cast("date")
        if date_col else F.lit(None).cast("date")
    )

    out = df.select(
        store_expr.alias("store_id"),
        date_expr.alias("business_date"),
        # Phase 1 cast Check_Number to string; keep that, and strip a trailing ".0"
        F.regexp_replace(F.trim(F.col("Check_Number")), r"\.0$", "").alias("check_number"),
        F.trim(F.col("Menu_Item")).alias("menu_item"),
        num("Quantity").alias("quantity"),
        num("Menu_Price").alias("menu_price"),
        F.abs(F.coalesce(num("Item_Discount"), F.lit(0.0))).alias("discount_amount"),
        (num("Item_Discount").isNull() & F.col("Item_Discount").isNotNull()
         & (F.trim(F.col("Item_Discount")) != "")).alias("discount_parse_failed"),
        num("Total").alias("check_total_reported"),
        F.trim(F.col("Pop")).alias("sales_channel"),
        F.hour(F.expr("try_to_timestamp(upper(trim(Begin_Time)), 'h:mm a')")).alias("order_hour"),
        flag_true("Order_Voided").alias("order_voided"),
        flag_true("Item_Voided").alias("item_voided"),
        "_source_file",
        "_ingested_at",
    )

    return (
        out
        .withColumn("order_id", F.concat_ws("|", "store_id", "business_date", "check_number"))
        .withColumn("gross_amount", F.col("quantity") * F.col("menu_price"))
        .withColumn("line_revenue", F.col("gross_amount") - F.col("discount_amount"))
        .withColumn("has_discount", F.col("discount_amount") > 0)
        .withColumn("day_of_week", F.date_format("business_date", "EEEE"))
        .withColumn(
            "daypart",
            F.when(F.col("order_hour").between(5, 10), "Breakfast")
             .when(F.col("order_hour").between(11, 13), "Lunch")
             .when(F.col("order_hour").between(14, 16), "Afternoon")
             .when(F.col("order_hour").between(17, 20), "Dinner")
             .otherwise("Late Night"),
        )
    )
    # Duplicates: Auto Loader never re-reads a file it already processed, so reruns
    # are safe. Identical lines are NOT dropped (two identical tacos on one check
    # are legitimate); they are measured in mv_data_quality_summary.
