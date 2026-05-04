"""
Shared pytest fixtures for the NYC Taxi test suite.

Provides a session-scoped local SparkSession that every test in this
directory can request by name. Creating a SparkSession is expensive
(JVM start-up, ~5-10 s), so we build it once per test session and let
pytest hand the same instance to every test that asks for it.
"""

from __future__ import annotations

from typing import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Yield a local SparkSession shared across the whole test session.

    Configured for fast, hermetic unit tests:

    * ``local[2]`` — two driver cores, no cluster needed.
    * ``spark.sql.shuffle.partitions = 2`` — keeps shuffles tiny so
      DataFrame operations on a handful of rows finish in milliseconds.
    * ``spark.driver.bindAddress = 127.0.0.1`` — avoids hostname
      resolution issues on macOS where the default bind address can
      hang on a flaky DNS lookup.

    The session is stopped after all tests have finished.
    """
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("nyc-taxi-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()
