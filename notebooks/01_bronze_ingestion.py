# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create the bronze schema if it doesn't exist
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze
# MAGIC COMMENT 'Bronze layer: raw, immutable copies of source data';
# MAGIC
# MAGIC -- Verify it was created
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

"""
Bronze layer ingestion for NYC Taxi trip data.

This notebook reads from the Databricks-provided sample dataset 
(samples.nyctaxi.trips) and lands it in our Bronze table with 
the three standard metadata columns required for audit and lineage.

Author: Kumari Shishubala
Project: NYC Taxi Medallion Pipeline
Layer: Bronze
"""

from pyspark.sql.functions import current_timestamp, lit, input_file_name

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

# Source: Databricks-provided sample dataset
SOURCE_TABLE = "samples.nyctaxi.trips"

# Target: our Bronze table in Unity Catalog (catalog.schema.table)
TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "bronze"
TARGET_TABLE = "taxi_trips_raw"
TARGET_FQN = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"  # fully-qualified name

# Source system identifier — important for lineage in multi-source warehouses
SOURCE_SYSTEM = "nyc_tlc_sample"

print(f"Source table:     {SOURCE_TABLE}")
print(f"Target Bronze:    {TARGET_FQN}")
print(f"Source system:    {SOURCE_SYSTEM}")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Read from source
# ─────────────────────────────────────────────────────────────

df_source = spark.read.table(SOURCE_TABLE)

print(f"Source row count: {df_source.count():,}")
print(f"Source schema:")
df_source.printSchema()

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Add the three Bronze metadata columns
# ─────────────────────────────────────────────────────────────
# These columns are the audit trail. They tell us:
#   - WHEN the data landed in Bronze
#   - WHERE the data originated (source file or system)
#   - WHICH upstream system produced it
# 
# We add them BEFORE writing to Bronze so every row is traceable.
# ─────────────────────────────────────────────────────────────

df_bronze = (
    df_source
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", lit(SOURCE_TABLE))   # using the source table name as our 'source' identifier
    .withColumn("_source_system", lit(SOURCE_SYSTEM))
)

print(f"Bronze DataFrame schema (with metadata):")
df_bronze.printSchema()

print("\nFirst 3 rows preview:")
df_bronze.select("_ingestion_timestamp", "_source_file", "_source_system", "fare_amount", "trip_distance").show(3, truncate=False)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Write to Bronze Delta table
# ─────────────────────────────────────────────────────────────
# We use 'overwrite' here ONLY because this is the initial load.
# In production, Bronze is append-only — we'd use .mode("append") 
# with a merge key, OR Auto Loader with checkpointing.
# 
# Why Delta format?
#   - ACID transactions (no half-written data)
#   - Time travel (we can query "Bronze as of 5 minutes ago")
#   - Schema evolution
#   - Performance (Z-Order, OPTIMIZE)
# ─────────────────────────────────────────────────────────────

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_FQN)
)

print(f"✅ Bronze table written: {TARGET_FQN}")
print(f"   Row count: {df_bronze.count():,}")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# Verification
# ─────────────────────────────────────────────────────────────

# Read the table back from Unity Catalog
df_verify = spark.read.table(TARGET_FQN)

print(f"Bronze table {TARGET_FQN}")
print(f"  Row count: {df_verify.count():,}")
print(f"  Column count: {len(df_verify.columns)}")
print(f"  Columns: {df_verify.columns}")
print("Sample rows:")
df_verify.select("tpep_pickup_datetime", "fare_amount", "_ingestion_timestamp", "_source_system").show(5, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a table comment for governance
# MAGIC COMMENT ON TABLE workspace.bronze.taxi_trips_raw IS 
# MAGIC   'Bronze layer: raw NYC Yellow Taxi trip records from samples.nyctaxi.trips. Append-only, immutable. Source of truth for downstream Silver/Gold layers.';
# MAGIC
# MAGIC -- View the table properties
# MAGIC DESCRIBE EXTENDED workspace.bronze.taxi_trips_raw;