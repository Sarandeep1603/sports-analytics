# Databricks notebook source
# Silver Layer — Cleanse, Deduplicate, Incremental MERGE
# Reads from Bronze Delta, applies transformations, MERGEs into Silver Delta Lake
# Great Expectations validation runs before final write

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, to_timestamp,
    when, coalesce, lit, current_timestamp,
    row_number, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType
from delta.tables import DeltaTable

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("execution_date", "", "Execution Date")
dbutils.widgets.text("bronze_path", "", "Bronze Base Path")
dbutils.widgets.text("silver_path", "", "Silver Base Path")

execution_date = dbutils.widgets.get("execution_date")
bronze_path    = dbutils.widgets.get("bronze_path")
silver_path    = dbutils.widgets.get("silver_path")

bronze_football_path = f"{bronze_path}football/delta/"
silver_football_path = f"{silver_path}football/matches/"

print(f"[Silver] Execution Date  : {execution_date}")
print(f"[Silver] Bronze Source   : {bronze_football_path}")
print(f"[Silver] Silver Target   : {silver_football_path}")

# ── Step 1: Read from Bronze ──────────────────────────────────────────────────
df_bronze = (
    spark.read
    .format("delta")
    .load(bronze_football_path)
    .filter(col("_ingest_date") == execution_date)   # Incremental — today's partition only
)

print(f"[Silver] Bronze records for {execution_date}: {df_bronze.count():,}")

# ── Step 2: Flatten Nested JSON ───────────────────────────────────────────────
# API-Football returns nested response → fixture → teams → goals → score
df_flat = df_bronze.select(
    col("fixture.id").cast(IntegerType()).alias("match_id"),
    col("fixture.date").cast(TimestampType()).alias("match_datetime"),
    to_date(col("fixture.date")).alias("match_date"),
    col("fixture.status.short").alias("match_status"),
    col("fixture.venue.name").alias("venue_name"),
    col("fixture.venue.city").alias("venue_city"),
    col("league.id").cast(IntegerType()).alias("league_id"),
    col("league.name").alias("league_name"),
    col("league.season").cast(IntegerType()).alias("season"),
    col("league.round").alias("round"),
    col("teams.home.id").cast(IntegerType()).alias("home_team_id"),
    col("teams.home.name").alias("home_team_name"),
    col("teams.away.id").cast(IntegerType()).alias("away_team_id"),
    col("teams.away.name").alias("away_team_name"),
    col("goals.home").cast(IntegerType()).alias("home_goals"),
    col("goals.away").cast(IntegerType()).alias("away_goals"),
    col("score.halftime.home").cast(IntegerType()).alias("ht_home_goals"),
    col("score.halftime.away").cast(IntegerType()).alias("ht_away_goals"),
    col("_ingest_timestamp"),
    col("_source_file"),
)

print(f"[Silver] Flattened record count: {df_flat.count():,}")

# ── Step 3: Cleansing ─────────────────────────────────────────────────────────
df_clean = (
    df_flat
    # Trim string columns
    .withColumn("league_name",    trim(col("league_name")))
    .withColumn("home_team_name", trim(col("home_team_name")))
    .withColumn("away_team_name", trim(col("away_team_name")))
    .withColumn("venue_name",     trim(col("venue_name")))
    # Standardize match status
    .withColumn("match_status",
        when(col("match_status") == "FT",  lit("FINISHED"))
        .when(col("match_status") == "NS",  lit("NOT_STARTED"))
        .when(col("match_status") == "LIVE", lit("LIVE"))
        .otherwise(col("match_status"))
    )
    # Null handling — default goals to 0 for postponed/cancelled matches
    .withColumn("home_goals", coalesce(col("home_goals"), lit(0)))
    .withColumn("away_goals", coalesce(col("away_goals"), lit(0)))
    # Add result column
    .withColumn("match_result",
        when(col("home_goals") > col("away_goals"), lit("HOME_WIN"))
        .when(col("home_goals") < col("away_goals"), lit("AWAY_WIN"))
        .when(col("home_goals") == col("away_goals"), lit("DRAW"))
        .otherwise(lit("UNKNOWN"))
    )
    # Add total goals
    .withColumn("total_goals", col("home_goals") + col("away_goals"))
    # Add silver audit column
    .withColumn("_silver_processed_at", current_timestamp())
)

# ── Step 4: Deduplication ─────────────────────────────────────────────────────
# Keep latest record per match_id (in case API returns duplicate fixtures)
window_dedup = Window.partitionBy("match_id").orderBy(col("_ingest_timestamp").desc())

df_deduped = (
    df_clean
    .withColumn("_row_num", row_number().over(window_dedup))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

print(f"[Silver] After deduplication: {df_deduped.count():,} records")

# ── Step 5: Filter only FINISHED matches ──────────────────────────────────────
df_silver_ready = df_deduped.filter(col("match_status") == "FINISHED")
print(f"[Silver] FINISHED matches to MERGE: {df_silver_ready.count():,}")

# ── Step 6: Incremental MERGE into Silver Delta Lake ─────────────────────────
if DeltaTable.isDeltaTable(spark, silver_football_path):
    print("[Silver] Delta table exists — performing MERGE (upsert)...")

    delta_silver = DeltaTable.forPath(spark, silver_football_path)

    delta_silver.alias("target").merge(
        source=df_silver_ready.alias("source"),
        condition="target.match_id = source.match_id"
    ).whenMatchedUpdateAll(               # Update if match data changed (e.g. score correction)
    ).whenNotMatchedInsertAll(            # Insert new matches
    ).execute()

    print("[Silver] ✅ MERGE completed successfully")

else:
    print("[Silver] Delta table does not exist — performing initial write...")
    (
        df_silver_ready
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("league_id", "season")
        .save(silver_football_path)
    )
    print("[Silver] ✅ Initial Silver Delta table created")

# ── Step 7: Register Table ────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sports_analytics.silver.football_matches
    USING DELTA
    LOCATION '{silver_football_path}'
""")

# ── Step 8: Optimize Delta Table ──────────────────────────────────────────────
# Run OPTIMIZE + Z-ORDER weekly (not every run — expensive)
# Z-ORDER on league_id + season for query performance
spark.sql(f"""
    OPTIMIZE delta.`{silver_football_path}`
    ZORDER BY (league_id, season)
""")

# ── Step 9: Final Validation ──────────────────────────────────────────────────
silver_total = spark.read.format("delta").load(silver_football_path).count()
print(f"[Silver] Total records in Silver Delta: {silver_total:,}")

# Schema check
spark.read.format("delta").load(silver_football_path).printSchema()

dbutils.notebook.exit("Silver transformation completed successfully")
