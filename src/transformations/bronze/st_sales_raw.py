# Databricks notebook source
# BRONZE: raw sales ingestion (streaming table)
# ------------------------------------------------------------
# Phase 1 equivalent: cell "Read all CSV" (spark.read.csv on the volume).
#
# Difference: Auto Loader keeps a checkpoint, so each run only reads NEW files.
# The first run loads the full history (~85M rows); after that, when Taco Bueno
# drops tomorrow's CSV in the folder, only that file is processed.
#
# Bronze keeps the data as it arrived (all columns as strings, so a bad value
# never breaks ingestion) plus metadata to trace every row back to its file.
# Column names are standardized exactly like in Phase 1 (spaces -> _, # -> num),
# because Delta tables do not accept spaces or symbols in column names.

import re
import dlt
from pyspark.sql import functions as F

RAW_PATH = spark.conf.get(
    "raw_sales_path", "/Volumes/workspace/default/datos_tacobueno/Taco Bueno - Pricing/"
)


def clean_name(name: str) -> str:
    """Same rule used in Phase 1: strip, '#' -> 'num', spaces -> '_', drop symbols."""
    n = name.strip().replace("#", "num").replace(" ", "_")
    return re.sub(r"[^a-zA-Z0-9_]", "", n)


@dlt.table(
    name="st_sales_raw",
    comment="Raw Taco Bueno POS sales lines as received, with standardized column names.",
    table_properties={"quality": "bronze"},
)
def st_sales_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")      # everything as string in bronze
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unexpected columns -> _rescued_data
        .load(RAW_PATH)
    )
    renamed = [
        F.col(f"`{c}`").alias(clean_name(c)) for c in df.columns if c != "_rescued_data"
    ]
    return df.select(
        *renamed,
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
