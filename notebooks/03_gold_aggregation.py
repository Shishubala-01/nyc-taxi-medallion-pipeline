# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold
# MAGIC COMMENT 'Gold layer: business-ready aggregated tables for analytics consumption';
# MAGIC
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

"""
Gold layer aggregation for NYC Taxi trip data.

This notebook reads from the cleaned Silver table and builds
a star schema for analytics:
  - gold.dim_zones                → zone dimension
  - gold.fact_trips_daily         → daily trip facts
  - gold.fact_trips_by_zone_day   → daily trips per pickup zone

Following the principle: "If Silver is what we trust, Gold is what answers business questions."

Author: Kumari Shishubala
Project: NYC Taxi Medallion Pipeline
Layer: Gold
"""

from pyspark.sql.functions import (
    col, current_timestamp, lit, count, sum as spark_sum, 
    avg, min as spark_min, max as spark_max,
    date_trunc, to_date, hour, dayofweek, month, year, 
    countDistinct, round as spark_round, when
)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SOURCE_FQN          = "workspace.silver.taxi_trips_clean"
DIM_ZONES_FQN       = "workspace.gold.dim_zones"
FACT_DAILY_FQN      = "workspace.gold.fact_trips_daily"
FACT_ZONE_DAILY_FQN = "workspace.gold.fact_trips_by_zone_day"

print(f"Silver source:           {SOURCE_FQN}")
print(f"Gold dim_zones:          {DIM_ZONES_FQN}")
print(f"Gold fact_trips_daily:   {FACT_DAILY_FQN}")
print(f"Gold fact_trips_by_zone: {FACT_ZONE_DAILY_FQN}")

# COMMAND ----------

df_silver = spark.read.table(SOURCE_FQN)

print(f"Silver row count: {df_silver.count():,}")
df_silver.printSchema()

# Quick sanity check
df_silver.select("pickup_timestamp", "pickup_zip", "fare_amount", "trip_distance").show(3)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Gold table 1: dim_zones — the zone dimension
# ─────────────────────────────────────────────────────────────
# A dimension table contains descriptive attributes about an 
# entity. Here it's pickup/dropoff zones. Even with limited 
# attributes, having a dim table:
#   - Standardises zone references across all fact tables
#   - Lets us add zone-level enrichment later (borough, neighborhood)
#   - Makes BI tools generate optimal join queries
# 
# We derive the zone list from observed pickup AND dropoff zones 
# in the data. In a real production warehouse, this would come 
# from a TLC reference table.
# ─────────────────────────────────────────────────────────────

# Get distinct pickup zones
df_pickup_zones = df_silver.select(col("pickup_zip").alias("zone_id")).distinct()

# Get distinct dropoff zones  
df_dropoff_zones = df_silver.select(col("dropoff_zip").alias("zone_id")).distinct()

# Union and dedupe
df_dim_zones = (
    df_pickup_zones.union(df_dropoff_zones)
    .filter(col("zone_id").isNotNull())
    .distinct()
    .withColumn("zone_type", lit("zip_code"))                      # placeholder for future enrichment
    .withColumn("_processed_timestamp", current_timestamp())
    .orderBy("zone_id")
)

print(f"Distinct zones: {df_dim_zones.count():,}")
df_dim_zones.show(10)

# COMMAND ----------

(
    df_dim_zones.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_ZONES_FQN)
)
print(f"✅ Written: {DIM_ZONES_FQN}  ({df_dim_zones.count():,} rows)")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Gold table 2: fact_trips_daily — daily trip metrics
# ─────────────────────────────────────────────────────────────
# This answers questions like:
#   - "What was our revenue yesterday?"
#   - "Did Tuesday's trip volume drop?"
#   - "What's our average trip distance trending toward?"
# 
# Design choice: pre-compute these aggregates rather than 
# making BI tools recompute them on every dashboard load. 
# This is core to Gold layer design.
# ─────────────────────────────────────────────────────────────

df_fact_daily = (
    df_silver
    .withColumn("trip_date", to_date("pickup_timestamp"))
    .groupBy("trip_date")
    .agg(
        count("*").alias("trip_count"),
        spark_round(spark_sum("fare_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
        spark_round(avg("trip_duration_minutes"), 1).alias("avg_duration_minutes"),
        spark_round(spark_sum("trip_distance"), 2).alias("total_distance_miles"),
        countDistinct("pickup_zip").alias("unique_pickup_zones"),
    )
    .withColumn("_processed_timestamp", current_timestamp())
    .orderBy("trip_date")
)

print(f"Daily fact rows: {df_fact_daily.count()}")
df_fact_daily.show(10, truncate=False)

# COMMAND ----------

(
    df_fact_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FACT_DAILY_FQN)
)
print(f"✅ Written: {FACT_DAILY_FQN}  ({df_fact_daily.count():,} rows)")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Gold table 3: fact_trips_by_zone_day — trips per zone per day
# ─────────────────────────────────────────────────────────────
# A "two-dimensional" fact table: aggregated by both date AND zone.
# This is the kind of table that powers heatmaps, geographic 
# dashboards, and zone performance reports.
# 
# Note: in production we'd often partition large fact tables by 
# date for query performance. We'll add partitioning here as a 
# best-practice demonstration.
# ─────────────────────────────────────────────────────────────

df_fact_zone_daily = (
    df_silver
    .filter(col("pickup_zip").isNotNull())
    .withColumn("trip_date", to_date("pickup_timestamp"))
    .groupBy("trip_date", "pickup_zip")
    .agg(
        count("*").alias("trip_count"),
        spark_round(spark_sum("fare_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
    )
    .withColumn("_processed_timestamp", current_timestamp())
)

print(f"Zone-daily fact rows: {df_fact_zone_daily.count():,}")
df_fact_zone_daily.orderBy(col("total_revenue").desc()).show(10, truncate=False)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Write with partitioning by trip_date
# ─────────────────────────────────────────────────────────────
# Partitioning by date means queries like 
#   "give me data for last week"
# only read the relevant date partitions, not the whole table.
# This is a real production optimisation that interviewers love.
# ─────────────────────────────────────────────────────────────

(
    df_fact_zone_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("trip_date")
    .saveAsTable(FACT_ZONE_DAILY_FQN)
)
print(f"✅ Written: {FACT_ZONE_DAILY_FQN}  ({df_fact_zone_daily.count():,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.dim_zones IS
# MAGIC   'Gold dimension: pickup and dropoff zones. Standardises zone references across fact tables. Currently zip-code based; can be enriched with borough/neighborhood from TLC reference data.';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.gold.fact_trips_daily IS
# MAGIC   'Gold fact: daily trip metrics including revenue, volume, and averages. Pre-aggregated for direct BI consumption. One row per date.';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.gold.fact_trips_by_zone_day IS
# MAGIC   'Gold fact: trip metrics by pickup zone per day. Partitioned by trip_date for query performance. Powers geographic dashboards and zone-performance reports.';
# MAGIC
# MAGIC -- Show all gold tables and their row counts
# MAGIC SELECT 'dim_zones' AS table_name, COUNT(*) AS row_count FROM workspace.gold.dim_zones
# MAGIC UNION ALL
# MAGIC SELECT 'fact_trips_daily', COUNT(*) FROM workspace.gold.fact_trips_daily
# MAGIC UNION ALL
# MAGIC SELECT 'fact_trips_by_zone_day', COUNT(*) FROM workspace.gold.fact_trips_by_zone_day;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The kind of question a stakeholder would actually ask:
# MAGIC -- "What were our top 5 highest-revenue pickup zones, and what 
# MAGIC --  percentage of total revenue did they generate?"
# MAGIC
# MAGIC WITH zone_totals AS (
# MAGIC   SELECT 
# MAGIC     pickup_zip,
# MAGIC     SUM(total_revenue) AS zone_revenue,
# MAGIC     SUM(trip_count) AS zone_trips
# MAGIC   FROM workspace.gold.fact_trips_by_zone_day
# MAGIC   GROUP BY pickup_zip
# MAGIC ),
# MAGIC grand_total AS (
# MAGIC   SELECT SUM(zone_revenue) AS total_rev FROM zone_totals
# MAGIC )
# MAGIC SELECT 
# MAGIC   z.pickup_zip,
# MAGIC   z.zone_trips,
# MAGIC   z.zone_revenue,
# MAGIC   ROUND(100.0 * z.zone_revenue / g.total_rev, 2) AS pct_of_total_revenue
# MAGIC FROM zone_totals z, grand_total g
# MAGIC ORDER BY z.zone_revenue DESC
# MAGIC LIMIT 5;