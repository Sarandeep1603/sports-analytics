"""
utils.py — Shared utility functions for Sports Analytics Pipeline
Used across Bronze, Silver, and Gold notebooks
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "SportsAnalyticsPipeline") -> SparkSession:
    """
    Returns or creates a SparkSession with Delta Lake configs.
    On Databricks, spark is already available — this is for local testing.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def read_delta(spark: SparkSession, path: str, partition_filter: Optional[str] = None) -> DataFrame:
    """
    Reads a Delta table with optional partition filter.

    Args:
        spark: SparkSession
        path: ADLS Gen2 path to Delta table
        partition_filter: SQL-style filter string e.g. "_ingest_date = '2024-01-15'"

    Returns:
        DataFrame
    """
    df = spark.read.format("delta").load(path)
    if partition_filter:
        df = df.filter(partition_filter)
    logger.info(f"Read {df.count():,} records from Delta table: {path}")
    return df


def merge_into_delta(
    spark: SparkSession,
    df_source: DataFrame,
    target_path: str,
    merge_condition: str,
    partition_cols: Optional[List[str]] = None,
) -> None:
    """
    Generic MERGE (upsert) into Delta Lake.
    Creates the table on first run, then MERGEs on subsequent runs.

    Args:
        spark: SparkSession
        df_source: Source DataFrame to merge
        target_path: ADLS Gen2 path to Delta target
        merge_condition: SQL condition string e.g. "target.match_id = source.match_id"
        partition_cols: List of partition columns for initial write
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        logger.info(f"Merging into existing Delta table: {target_path}")
        delta_table = DeltaTable.forPath(spark, target_path)
        (
            delta_table.alias("target")
            .merge(source=df_source.alias("source"), condition=merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE completed successfully.")
    else:
        logger.info(f"Delta table not found — creating: {target_path}")
        writer = df_source.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        logger.info("Initial Delta table created.")


def optimize_delta_table(spark: SparkSession, path: str, zorder_cols: Optional[List[str]] = None) -> None:
    """
    Runs OPTIMIZE on a Delta table with optional Z-ORDER.
    Z-ORDER improves query performance for high-cardinality filter columns.

    Args:
        spark: SparkSession
        path: Delta table path
        zorder_cols: Columns to Z-ORDER by (max 4 recommended)
    """
    if zorder_cols:
        cols_str = ", ".join(zorder_cols)
        query = f"OPTIMIZE delta.`{path}` ZORDER BY ({cols_str})"
    else:
        query = f"OPTIMIZE delta.`{path}`"

    logger.info(f"Running: {query}")
    spark.sql(query)
    logger.info("OPTIMIZE completed.")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Runs VACUUM to remove old Delta files.
    Default retention: 168 hours (7 days) — Delta Lake minimum.

    Args:
        spark: SparkSession
        path: Delta table path
        retention_hours: Hours to retain old versions (default 168 = 7 days)
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"VACUUM completed for {path} with {retention_hours}h retention.")


def add_audit_columns(df: DataFrame, layer: str) -> DataFrame:
    """
    Adds standard audit columns to any DataFrame.

    Args:
        df: Input DataFrame
        layer: Pipeline layer name e.g. 'bronze', 'silver', 'gold'

    Returns:
        DataFrame with audit columns added
    """
    return (
        df
        .withColumn(f"_{layer}_processed_at", current_timestamp())
        .withColumn("_pipeline_version", lit("1.0.0"))
    )


def log_pipeline_stats(df: DataFrame, stage: str) -> None:
    """
    Logs record count and schema for a given pipeline stage.

    Args:
        df: DataFrame to inspect
        stage: Stage name for logging
    """
    count = df.count()
    logger.info(f"[{stage.upper()}] Record count: {count:,}")
    logger.info(f"[{stage.upper()}] Schema:")
    df.printSchema()
    return count
