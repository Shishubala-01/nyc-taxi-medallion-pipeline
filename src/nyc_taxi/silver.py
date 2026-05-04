"""
Silver layer transformations for NYC Taxi trip data.

Pure DataFrame-in / DataFrame-out functions extracted from
notebooks/02_silver_transformation.py so they can be unit-tested
against a local SparkSession without touching Delta tables.

The orchestration notebook is responsible for I/O (spark.read /
spark.write); this module is responsible only for the transformation
logic.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, row_number, unix_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────
# Business data-quality rules
# ─────────────────────────────────────────────────────────────
MIN_FARE: float = 0.0
MAX_FARE: float = 1000.0
MIN_DISTANCE: float = 0.0
MAX_DISTANCE: float = 100.0
MIN_PASSENGERS: int = 1
MAX_PASSENGERS: int = 6


def standardise_columns(df: DataFrame) -> DataFrame:
    """Rename Bronze technical column names to Silver business names.

    The raw NYC TLC feed exposes ``tpep_pickup_datetime`` and
    ``tpep_dropoff_datetime``; Silver consumers (analysts, BI tools)
    expect the friendlier ``pickup_timestamp`` / ``dropoff_timestamp``.

    Parameters
    ----------
    df:
        Input DataFrame containing ``tpep_pickup_datetime`` and
        ``tpep_dropoff_datetime`` columns.

    Returns
    -------
    DataFrame
        The same DataFrame with the two columns renamed.
    """
    return (
        df
        .withColumnRenamed("tpep_pickup_datetime", "pickup_timestamp")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_timestamp")
    )


def enforce_schema(df: DataFrame) -> DataFrame:
    """Cast Silver columns to their canonical types.

    Failed casts produce ``NULL`` rather than raising, which lets
    :func:`tag_data_quality` capture and quarantine bad rows downstream
    instead of failing the whole job.

    Parameters
    ----------
    df:
        Input DataFrame with ``pickup_timestamp``, ``dropoff_timestamp``,
        ``trip_distance``, ``fare_amount``, ``pickup_zip``, ``dropoff_zip``.

    Returns
    -------
    DataFrame
        DataFrame with those six columns cast to their target types.
    """
    return (
        df
        .withColumn("pickup_timestamp",  col("pickup_timestamp").cast(TimestampType()))
        .withColumn("dropoff_timestamp", col("dropoff_timestamp").cast(TimestampType()))
        .withColumn("trip_distance",     col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount",       col("fare_amount").cast(DoubleType()))
        .withColumn("pickup_zip",        col("pickup_zip").cast(IntegerType()))
        .withColumn("dropoff_zip",       col("dropoff_zip").cast(IntegerType()))
    )


def add_trip_duration(df: DataFrame) -> DataFrame:
    """Add ``trip_duration_minutes`` derived from pickup/dropoff timestamps.

    Light enrichment belongs in Silver; heavier aggregations belong in
    Gold. A non-positive duration is a quality signal — see rule R3 in
    :func:`tag_data_quality`.

    Parameters
    ----------
    df:
        DataFrame with timestamp columns ``pickup_timestamp`` and
        ``dropoff_timestamp``.

    Returns
    -------
    DataFrame
        Input DataFrame plus a ``trip_duration_minutes`` (double) column.
    """
    return df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp("dropoff_timestamp") - unix_timestamp("pickup_timestamp")) / 60.0,
    )


def tag_data_quality(
    df: DataFrame,
    min_fare: float = MIN_FARE,
    max_fare: float = MAX_FARE,
    min_distance: float = MIN_DISTANCE,
    max_distance: float = MAX_DISTANCE,
) -> DataFrame:
    """Tag each row with a ``_rejection_reason`` (or ``NULL`` if it passed).

    Rules are evaluated in order; the first failure wins:

    * **R1** – ``pickup_timestamp`` is null
    * **R2** – ``dropoff_timestamp`` is null
    * **R3** – ``trip_duration_minutes`` is non-positive
    * **R4** – ``fare_amount`` outside ``[min_fare, max_fare]``
    * **R5** – ``trip_distance`` outside ``[min_distance, max_distance]``

    Nothing is filtered here — failing rows are *tagged* so the caller
    can split them into a quarantine table for investigation.

    Parameters
    ----------
    df:
        DataFrame produced by :func:`add_trip_duration`.
    min_fare, max_fare:
        Inclusive fare-amount bounds for rule R4.
    min_distance, max_distance:
        Inclusive trip-distance bounds for rule R5.

    Returns
    -------
    DataFrame
        Input DataFrame with an extra ``_rejection_reason`` (string) column.
    """
    return df.withColumn(
        "_rejection_reason",
        when(col("pickup_timestamp").isNull(),  lit("R1: null pickup_timestamp"))
        .when(col("dropoff_timestamp").isNull(), lit("R2: null dropoff_timestamp"))
        .when(col("trip_duration_minutes") <= 0, lit("R3: non-positive trip duration"))
        .when((col("fare_amount") < min_fare) | (col("fare_amount") > max_fare),
              lit("R4: fare out of range"))
        .when((col("trip_distance") < min_distance) | (col("trip_distance") > max_distance),
              lit("R5: distance out of range"))
        .otherwise(lit(None)),
    )


def dedupe_with_window(
    df: DataFrame,
    dedup_keys: list[str],
    order_col: str,
) -> DataFrame:
    """Deduplicate rows deterministically using a row-number window.

    Rows that share the same values across ``dedup_keys`` are grouped;
    within each group the row with the highest value of ``order_col``
    is kept (descending sort, ``row_number == 1``). This is more robust
    than ``dropDuplicates``, which picks a duplicate non-deterministically.

    Parameters
    ----------
    df:
        DataFrame to deduplicate.
    dedup_keys:
        Columns whose combined values define a duplicate group.
    order_col:
        Column used to break ties within a duplicate group; the row
        with the largest value wins (e.g. latest ``_ingestion_timestamp``).

    Returns
    -------
    DataFrame
        Deduplicated DataFrame with the helper ``_dedup_rank`` column
        added then dropped.
    """
    window_spec = Window.partitionBy(*dedup_keys).orderBy(col(order_col).desc())
    return (
        df
        .withColumn("_dedup_rank", row_number().over(window_spec))
        .filter(col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def split_clean_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a tagged DataFrame into clean and quarantine outputs.

    The clean DataFrame contains rows that passed every quality rule
    (``_rejection_reason IS NULL``); the quarantine DataFrame contains
    every other row, with the rejection reason preserved for auditing.

    "In data engineering, deletion is failure. Quarantine is success."

    Parameters
    ----------
    df:
        DataFrame produced by :func:`tag_data_quality` (must contain a
        ``_rejection_reason`` column).

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(df_clean, df_quarantine)``. ``df_clean`` has
        ``_rejection_reason`` dropped; ``df_quarantine`` retains it.
    """
    df_clean = (
        df
        .filter(col("_rejection_reason").isNull())
        .drop("_rejection_reason")
    )
    df_quarantine = df.filter(col("_rejection_reason").isNotNull())
    return df_clean, df_quarantine
