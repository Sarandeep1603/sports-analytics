# Databricks notebook source
# Bronze Layer — Raw API Data Ingestion
# Lands raw JSON from API-Football & Cricket API into ADLS Gen2 Bronze layer
# This notebook is triggered by ADF pipeline after raw files are landed

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, input_file_name, col
)
import logging

# ── Parameters (passed from ADF / Airflow) ────────────────────────────────────
dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
dbutils.widgets.text("sport_type", "football", "Sport Type: football | cricket")
dbutils.widgets.text("bronze_base_path", "", "Bronze Base Path (ADLS Gen2)")

execution_date = dbutils.widgets.get("execution_date")
sport_type     = dbutils.widgets.get("sport_type")
bronze_base    = dbutils.widgets.get("bronze_base_path")

print(f"[Bronze] Execution Date : {execution_date}")
print(f"[Bronze] Sport Type     : {sport_type}")
print(f"[Bronze] Bronze Path    : {bronze_base}")

# ── Paths ─────────────────────────────────────────────────────────────────────
raw_json_path   = f"{bronze_base}{sport_type}/raw/{execution_date}/"
bronze_delta_path = f"{bronze_base}{sport_type}/delta/"

print(f"[Bronze] Reading raw JSON from : {raw_json_path}")
print(f"[Bronze] Writing Delta to      : {bronze_delta_path}")

# ── Read Raw JSON landed by ADF ───────────────────────────────────────────────
df_raw = (
    spark.read
    .option("multiLine", True)
    .option("mode", "PERMISSIVE")           # Don't fail on malformed records
    .json(raw_json_path)
)

print(f"[Bronze] Raw record count: {df_raw.count()}")
df_raw.printSchema()

# ── Add Audit Columns ─────────────────────────────────────────────────────────
df_bronze = (
    df_raw
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_ingest_date", lit(execution_date))
    .withColumn("_source_file", input_file_name())
    .withColumn("_sport_type", lit(sport_type))
)

# ── Write to Delta Lake (Bronze) ──────────────────────────────────────────────
# Append mode — Bronze is immutable, we never overwrite raw data
(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .partitionBy("_ingest_date", "_sport_type")
    .option("mergeSchema", "true")          # Handle schema evolution gracefully
    .save(bronze_delta_path)
)

print(f"[Bronze] ✅ Successfully written {df_bronze.count()} records to Bronze Delta layer")

# ── Register in Unity Catalog / Hive Metastore ───────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sports_analytics.bronze.raw_{sport_type}_matches
    USING DELTA
    LOCATION '{bronze_delta_path}'
""")

print(f"[Bronze] Table registered: sports_analytics.bronze.raw_{sport_type}_matches")

# ── Basic Validation ──────────────────────────────────────────────────────────
bronze_count = spark.read.format("delta").load(bronze_delta_path).count()
print(f"[Bronze] Total records in Bronze Delta: {bronze_count:,}")

dbutils.notebook.exit("Bronze ingestion completed successfully")
