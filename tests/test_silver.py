"""
Unit tests for the pure transformation functions in ``nyc_taxi.silver``.

Each test builds a tiny in-memory DataFrame with ``spark.createDataFrame``
and asserts on the output, so the whole suite runs against a local
SparkSession in seconds without touching Delta or the Databricks
workspace.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

from nyc_taxi.silver import (
    MAX_FARE,
    MIN_FARE,
    add_trip_duration,
    dedupe_with_window,
    enforce_schema,
    split_clean_and_quarantine,
    standardise_columns,
    tag_data_quality,
)


def test_standardise_columns_renames_pickup_and_dropoff(spark: SparkSession) -> None:
    """The two ``tpep_*`` columns are renamed to their Silver-friendly names."""
    df = spark.createDataFrame(
        [(datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 30, 0), 12.5)],
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount"],
    )

    result = standardise_columns(df)

    assert "pickup_timestamp" in result.columns
    assert "dropoff_timestamp" in result.columns
    assert "tpep_pickup_datetime" not in result.columns


def test_enforce_schema_casts_numeric_columns_to_double(spark: SparkSession) -> None:
    """String-typed fare/distance columns are cast to DoubleType."""
    df = spark.createDataFrame(
        [("2024-01-01 10:00:00", "2024-01-01 10:30:00", "1.5", "10.0", "12345", "67890")],
        [
            "pickup_timestamp",
            "dropoff_timestamp",
            "trip_distance",
            "fare_amount",
            "pickup_zip",
            "dropoff_zip",
        ],
    )

    result = enforce_schema(df)

    assert result.schema["fare_amount"].dataType.typeName() == "double"
    assert result.schema["trip_distance"].dataType.typeName() == "double"


def test_add_trip_duration_computes_minutes_correctly(spark: SparkSession) -> None:
    """A 30-minute trip yields trip_duration_minutes == 30.0."""
    df = spark.createDataFrame(
        [(datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 30, 0))],
        ["pickup_timestamp", "dropoff_timestamp"],
    )

    result = add_trip_duration(df)

    assert result.collect()[0]["trip_duration_minutes"] == 30.0


def test_tag_data_quality_passes_clean_rows(spark: SparkSession) -> None:
    """A row that satisfies every rule is tagged with a null reason."""
    df = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 10, 0, 0),   # pickup_timestamp
                datetime(2024, 1, 1, 10, 30, 0),  # dropoff_timestamp
                2.5,                              # trip_distance
                15.0,                             # fare_amount
                30.0,                             # trip_duration_minutes
            )
        ],
        [
            "pickup_timestamp",
            "dropoff_timestamp",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
        ],
    )

    result = tag_data_quality(df)

    assert result.collect()[0]["_rejection_reason"] is None


def test_tag_data_quality_rejects_negative_fare(spark: SparkSession) -> None:
    """A negative fare violates R4 and is tagged accordingly."""
    df = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 30, 0),
                2.5,
                -5.0,   # negative fare → R4
                30.0,
            )
        ],
        [
            "pickup_timestamp",
            "dropoff_timestamp",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
        ],
    )

    reason = tag_data_quality(df).collect()[0]["_rejection_reason"]

    assert reason is not None
    assert reason.startswith("R4") or "fare out of range" in reason
    # Sanity-check the rule constants we're testing against.
    assert -5.0 < MIN_FARE <= MAX_FARE


def test_dedupe_with_window_keeps_one_per_group(spark: SparkSession) -> None:
    """Two rows with identical dedup keys collapse to one — the latest wins."""
    earlier = datetime(2024, 1, 1, 9, 0, 0)
    later = datetime(2024, 1, 2, 9, 0, 0)

    df = spark.createDataFrame(
        [
            ("trip-A", earlier),
            ("trip-A", later),
        ],
        ["trip_key", "_ingestion_timestamp"],
    )

    result = dedupe_with_window(
        df,
        dedup_keys=["trip_key"],
        order_col="_ingestion_timestamp",
    )

    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["_ingestion_timestamp"] == later


def test_split_clean_and_quarantine_preserves_total_count(spark: SparkSession) -> None:
    """Splitting on _rejection_reason partitions the input with no row loss."""
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("_rejection_reason", StringType(), nullable=True),
        ]
    )
    df = spark.createDataFrame(
        [
            (1, None),
            (2, None),
            (3, None),
            (4, "R1: null pickup_timestamp"),
            (5, "R4: fare out of range"),
        ],
        schema=schema,
    )

    df_clean, df_quarantine = split_clean_and_quarantine(df)

    assert df_clean.count() == 3
    assert df_quarantine.count() == 2
    assert df_clean.count() + df_quarantine.count() == 5
    # Clean output must not leak the rejection-reason column.
    assert "_rejection_reason" not in df_clean.columns
