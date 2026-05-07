# Databricks notebook source
# Gold Layer — Star Schema Builder
# Builds: fact_match_stats, dim_team, dim_player, dim_date
# Optimized for Power BI DirectQuery with date-based partitioning

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, date_format,
    year, month, dayofmonth, dayofweek, weekofyear,
    quarter, when, monotonically_increasing_id,
    max as spark_max, min as spark_min, avg, sum as spark_sum, count
)
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# ── Parameters ────────────────────────────────────────────────────────────────
dbutils.widgets.text("execution_date", "", "Execution Date")
dbutils.widgets.text("silver_path", "", "Silver Base Path")
dbutils.widgets.text("gold_path",   "", "Gold Base Path")

execution_date = dbutils.widgets.get("execution_date")
silver_path    = dbutils.widgets.get("silver_path")
gold_path      = dbutils.widgets.get("gold_path")

silver_matches_path    = f"{silver_path}football/matches/"
gold_fact_path         = f"{gold_path}fact_match_stats/"
gold_dim_team_path     = f"{gold_path}dim_team/"
gold_dim_date_path     = f"{gold_path}dim_date/"

print(f"[Gold] Execution Date : {execution_date}")
print(f"[Gold] Silver Source  : {silver_matches_path}")
print(f"[Gold] Gold Target    : {gold_path}")

# ── Read Silver ───────────────────────────────────────────────────────────────
df_silver = spark.read.format("delta").load(silver_matches_path)
print(f"[Gold] Silver total records: {df_silver.count():,}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_TEAM — Team dimension table
# ══════════════════════════════════════════════════════════════════════════════
print("[Gold] Building dim_team...")

# Union home and away teams to get all unique teams
df_home_teams = df_silver.select(
    col("home_team_id").alias("team_id"),
    col("home_team_name").alias("team_name"),
    col("league_id"),
    col("league_name"),
    col("season"),
)
df_away_teams = df_silver.select(
    col("away_team_id").alias("team_id"),
    col("away_team_name").alias("team_name"),
    col("league_id"),
    col("league_name"),
    col("season"),
)

df_dim_team = (
    df_home_teams.union(df_away_teams)
    .distinct()
    .withColumn("_gold_processed_at", current_timestamp())
)

# MERGE dim_team
if DeltaTable.isDeltaTable(spark, gold_dim_team_path):
    DeltaTable.forPath(spark, gold_dim_team_path).alias("target").merge(
        source=df_dim_team.alias("source"),
        condition="target.team_id = source.team_id AND target.season = source.season"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_dim_team.write.format("delta").mode("overwrite").save(gold_dim_team_path)

print(f"[Gold] dim_team record count: {spark.read.format('delta').load(gold_dim_team_path).count():,}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_DATE — Date dimension table
# ══════════════════════════════════════════════════════════════════════════════
print("[Gold] Building dim_date...")

df_dim_date = (
    df_silver
    .select(col("match_date").alias("date"))
    .distinct()
    .withColumn("year",          year(col("date")))
    .withColumn("month",         month(col("date")))
    .withColumn("day",           dayofmonth(col("date")))
    .withColumn("quarter",       quarter(col("date")))
    .withColumn("week_of_year",  weekofyear(col("date")))
    .withColumn("day_of_week",   dayofweek(col("date")))
    .withColumn("month_name",    date_format(col("date"), "MMMM"))
    .withColumn("day_name",      date_format(col("date"), "EEEE"))
    .withColumn("is_weekend",
        when(dayofweek(col("date")).isin([1, 7]), lit(True)).otherwise(lit(False))
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

if DeltaTable.isDeltaTable(spark, gold_dim_date_path):
    DeltaTable.forPath(spark, gold_dim_date_path).alias("target").merge(
        source=df_dim_date.alias("source"),
        condition="target.date = source.date"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_dim_date.write.format("delta").mode("overwrite").save(gold_dim_date_path)

print(f"[Gold] dim_date record count: {spark.read.format('delta').load(gold_dim_date_path).count():,}")

# ══════════════════════════════════════════════════════════════════════════════
# FACT_MATCH_STATS — Core fact table
# ══════════════════════════════════════════════════════════════════════════════
print("[Gold] Building fact_match_stats...")

df_fact = (
    df_silver
    .select(
        col("match_id"),
        col("match_date"),
        col("match_datetime"),
        col("league_id"),
        col("season"),
        col("round"),
        col("home_team_id"),
        col("away_team_id"),
        col("home_goals"),
        col("away_goals"),
        col("ht_home_goals"),
        col("ht_away_goals"),
        col("total_goals"),
        col("match_result"),
        col("venue_name"),
        col("venue_city"),
        # Derived measures
        when(col("match_result") == "HOME_WIN", lit(3))
        .when(col("match_result") == "DRAW", lit(1))
        .otherwise(lit(0)).alias("home_team_points"),

        when(col("match_result") == "AWAY_WIN", lit(3))
        .when(col("match_result") == "DRAW", lit(1))
        .otherwise(lit(0)).alias("away_team_points"),

        (col("home_goals") - col("away_goals")).alias("goal_difference"),
    )
    .withColumn("_gold_processed_at", current_timestamp())
)

# MERGE fact table on match_id (matches can get score corrections)
if DeltaTable.isDeltaTable(spark, gold_fact_path):
    DeltaTable.forPath(spark, gold_fact_path).alias("target").merge(
        source=df_fact.alias("source"),
        condition="target.match_id = source.match_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    (
        df_fact
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("season", "league_id")    # Date-based partitioning for Power BI
        .save(gold_fact_path)
    )

# ── OPTIMIZE Gold Tables ──────────────────────────────────────────────────────
spark.sql(f"OPTIMIZE delta.`{gold_fact_path}` ZORDER BY (match_date, league_id)")

# ── Register Tables ───────────────────────────────────────────────────────────
for table_name, path in [
    ("fact_match_stats", gold_fact_path),
    ("dim_team",         gold_dim_team_path),
    ("dim_date",         gold_dim_date_path),
]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS sports_analytics.gold.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    count = spark.read.format("delta").load(path).count()
    print(f"[Gold] ✅ {table_name}: {count:,} records")

# ── Summary Stats ─────────────────────────────────────────────────────────────
print("\n[Gold] === Pipeline Summary ===")
df_summary = spark.read.format("delta").load(gold_fact_path)
df_summary.groupBy("league_id", "season").agg(
    count("match_id").alias("total_matches"),
    spark_sum("total_goals").alias("total_goals"),
    avg("total_goals").alias("avg_goals_per_match"),
).orderBy("league_id").show()

dbutils.notebook.exit("Gold layer built successfully")
