# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create the silver schema if it doesn't exist
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver
# MAGIC COMMENT 'Silver layer: cleansed, deduplicated, business-quality data ready for analytics';
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

"""
Silver layer transformation for NYC Taxi trip data.

This notebook reads from the Bronze table and produces two Silver tables:
  - silver.taxi_trips_clean       → rows that passed all quality rules
  - silver.taxi_trips_quarantine  → rows that failed quality rules (with reasons)

Following the principle: "In data engineering, deletion is failure. Quarantine is success."

Author: Kumari Shishubala
Project: NYC Taxi Medallion Pipeline
Layer: Silver
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, sha2, concat_ws,
    coalesce, unix_timestamp, expr, count, sum as spark_sum
)
from pyspark.sql.types import (
    IntegerType, DoubleType, TimestampType, StringType
)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SOURCE_FQN     = "workspace.bronze.taxi_trips_raw"
CLEAN_FQN      = "workspace.silver.taxi_trips_clean"
QUARANTINE_FQN = "workspace.silver.taxi_trips_quarantine"

# Business quality rules — keep them as named variables, easy to change & audit
MIN_FARE = 0.0
MAX_FARE = 1000.0
MIN_DISTANCE = 0.0
MAX_DISTANCE = 100.0
MIN_PASSENGERS = 1
MAX_PASSENGERS = 6

print(f"Bronze source:    {SOURCE_FQN}")
print(f"Silver clean:     {CLEAN_FQN}")
print(f"Silver quarantine: {QUARANTINE_FQN}")
print(f"Quality rules: fare ∈ [{MIN_FARE}, {MAX_FARE}], distance ∈ [{MIN_DISTANCE}, {MAX_DISTANCE}], passengers ∈ [{MIN_PASSENGERS}, {MAX_PASSENGERS}]")

# COMMAND ----------

df_bronze = spark.read.table(SOURCE_FQN)

print(f"Bronze row count: {df_bronze.count():,}")
print(f"Bronze columns:")
df_bronze.printSchema()

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 1: Standardisation
# ─────────────────────────────────────────────────────────────
# Source columns are technical (tpep_*). Silver should expose
# business-friendly names that downstream analysts and BI tools
# can use without confusion.
# ─────────────────────────────────────────────────────────────

df_renamed = (
    df_bronze
    .withColumnRenamed("tpep_pickup_datetime", "pickup_timestamp")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_timestamp")
)

print("Renamed columns successfully.")
df_renamed.printSchema()

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 2: Schema enforcement (strict types via cast)
# ─────────────────────────────────────────────────────────────
# Failed casts produce NULL rather than crashing the job.
# We catch the NULLs in the quality-check stage and quarantine
# those rows with a reason.
# ─────────────────────────────────────────────────────────────

df_typed = (
    df_renamed
    .withColumn("pickup_timestamp",   col("pickup_timestamp").cast(TimestampType()))
    .withColumn("dropoff_timestamp",  col("dropoff_timestamp").cast(TimestampType()))
    .withColumn("trip_distance",      col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount",        col("fare_amount").cast(DoubleType()))
    .withColumn("pickup_zip",         col("pickup_zip").cast(IntegerType()))
    .withColumn("dropoff_zip",        col("dropoff_zip").cast(IntegerType()))
)

print("Types enforced. Schema now:")
df_typed.printSchema()

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 3: Light enrichment — derive trip duration in minutes
# ─────────────────────────────────────────────────────────────
# Silver can include simple derived columns that make analytics
# easier downstream. Heavy aggregations belong in Gold.
# ─────────────────────────────────────────────────────────────

df_enriched = df_typed.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("dropoff_timestamp") - unix_timestamp("pickup_timestamp")) / 60.0
)

print("Sample of enriched data:")
df_enriched.select(
    "pickup_timestamp", "dropoff_timestamp", "trip_duration_minutes",
    "trip_distance", "fare_amount"
).show(5, truncate=False)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 4: Data quality rules — tag every row pass/fail
# ─────────────────────────────────────────────────────────────
# We don't filter yet. We TAG each row with whether it passed
# all rules, and if it failed, WHY. Then we split into clean +
# quarantine. This way nothing is silently dropped.
#
# Rules:
#   R1: pickup_timestamp must not be null
#   R2: dropoff_timestamp must not be null
#   R3: dropoff must be AFTER pickup (positive duration)
#   R4: fare_amount within sane range
#   R5: trip_distance within sane range
# ─────────────────────────────────────────────────────────────

df_tagged = df_enriched.withColumn(
    "_rejection_reason",
    when(col("pickup_timestamp").isNull(),  lit("R1: null pickup_timestamp"))
    .when(col("dropoff_timestamp").isNull(), lit("R2: null dropoff_timestamp"))
    .when(col("trip_duration_minutes") <= 0, lit("R3: non-positive trip duration"))
    .when((col("fare_amount") < MIN_FARE) | (col("fare_amount") > MAX_FARE),
        lit("R4: fare out of range"))
    .when((col("trip_distance") < MIN_DISTANCE) | (col("trip_distance") > MAX_DISTANCE),
        lit("R5: distance out of range"))
    .otherwise(lit(None))   # null reason = passed all rules
)

# Quick sanity check — how many rows fall into each bucket?
print("Quality rule breakdown:")
df_tagged.groupBy("_rejection_reason").count().orderBy("count", ascending=False).show(truncate=False)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 5: Deduplication using row_number() with a deterministic 
# tiebreaker
# ─────────────────────────────────────────────────────────────
# We use a Window function so duplicates are grouped, and within 
# each group we keep the row with the LATEST _ingestion_timestamp.
# This is more robust than dropDuplicates(), which would pick a 
# duplicate non-deterministically.
# 
# Why this matters in production:
#   - Reproducible: same input always yields same output
#   - Auditable: we can explain WHICH row we kept and WHY
#   - Resilient to multiple ingestion runs landing the same row
# ─────────────────────────────────────────────────────────────

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define what makes two rows "duplicates"
dedup_keys = [
    "pickup_timestamp", "dropoff_timestamp",
    "trip_distance", "fare_amount",
    "pickup_zip", "dropoff_zip"
]

# Within each duplicate group, keep the LATEST ingested row
window_spec = (
    Window
    .partitionBy(*dedup_keys)
    .orderBy(col("_ingestion_timestamp").desc())
)

df_ranked = df_tagged.withColumn("_dedup_rank", row_number().over(window_spec))

# Inspect: how many duplicate groups exist?
total_rows = df_ranked.count()
unique_rows = df_ranked.filter(col("_dedup_rank") == 1).count()
duplicate_rows = total_rows - unique_rows

print(f"Total rows:      {total_rows:,}")
print(f"Unique rows:     {unique_rows:,}")
print(f"Duplicate rows:  {duplicate_rows:,}")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 6: Drop duplicates (keep only rank=1) and add Silver audit
# ─────────────────────────────────────────────────────────────

df_silver_full = (
    df_ranked
    .filter(col("_dedup_rank") == 1)
    .drop("_dedup_rank")
    .withColumn("_silver_processed_timestamp", current_timestamp())
)

print(f"After dedup: {df_silver_full.count():,} rows")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 7: Split into Clean and Quarantine outputs
# ─────────────────────────────────────────────────────────────

df_clean = df_silver_full.filter(col("_rejection_reason").isNull()).drop("_rejection_reason")
df_quarantine = df_silver_full.filter(col("_rejection_reason").isNotNull())

print(f"Clean rows:       {df_clean.count():,}")
print(f"Quarantined rows: {df_quarantine.count():,}")
print(f"Total:            {df_clean.count() + df_quarantine.count():,}  (must equal post-dedup total)")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Step 8: Write to Delta Lake — both clean and quarantine tables
# ─────────────────────────────────────────────────────────────

# Clean table
(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CLEAN_FQN)
)
print(f"✅ Written: {CLEAN_FQN}  ({df_clean.count():,} rows)")

# Quarantine table
(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_FQN)
)
print(f"✅ Written: {QUARANTINE_FQN}  ({df_quarantine.count():,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.taxi_trips_clean IS
# MAGIC   'Silver layer: cleansed, typed, deduplicated NYC taxi trips that passed all quality rules. Ready for analytics consumption by Gold layer.';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.silver.taxi_trips_quarantine IS
# MAGIC   'Silver quarantine: rows from Bronze that failed one or more data quality rules. Each row has a _rejection_reason explaining why. Investigate periodically.';
# MAGIC
# MAGIC DESCRIBE EXTENDED workspace.silver.taxi_trips_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The kind of query that would now run reliably against Silver
# MAGIC -- because every row has been cleansed and typed
# MAGIC
# MAGIC SELECT
# MAGIC   date_trunc('day', pickup_timestamp) AS trip_day,
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   ROUND(AVG(fare_amount), 2) AS avg_fare,
# MAGIC   ROUND(AVG(trip_duration_minutes), 1) AS avg_duration_mins,
# MAGIC   ROUND(AVG(trip_distance), 2) AS avg_distance_miles
# MAGIC FROM workspace.silver.taxi_trips_clean
# MAGIC GROUP BY trip_day
# MAGIC ORDER BY trip_day
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What were the 6 rejected rows? Why were they rejected?
# MAGIC SELECT 
# MAGIC   _rejection_reason,
# MAGIC   COUNT(*) AS rejected_count,
# MAGIC   ROUND(AVG(fare_amount), 2) AS avg_rejected_fare,
# MAGIC   ROUND(AVG(trip_distance), 2) AS avg_rejected_distance
# MAGIC FROM workspace.silver.taxi_trips_quarantine
# MAGIC GROUP BY _rejection_reason
# MAGIC ORDER BY rejected_count DESC;