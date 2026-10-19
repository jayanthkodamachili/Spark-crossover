"""Set up each engine, then run one workload end to end, repeatedly.

Two decisions shape every number this file produces.

Setup is timed separately from the workload. Starting a JVM costs seconds and starting a
pandas import costs milliseconds, and burying that difference inside each repeat would
mean every Spark measurement is mostly a measurement of JVM startup. It is reported on its
own instead, because it is a real cost that a reader has to add back, and because a cost
you can see is one you can amortise deliberately.

The read is not timed separately. Every repeat loads the Parquet file and then computes,
because that is what a batch job actually pays. An engine that keeps the data in memory
between repeats would be answering a different question, and the engines differ in whether
they can do that at all.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import contextmanager

from spark_crossover import workloads
from spark_crossover.config import BenchmarkConfig
from spark_crossover.data import ensure_dataset


@contextmanager
def pandas_engine(config: BenchmarkConfig):
    import pandas as pd

    left_path, right_path = ensure_dataset(config)

    def run(workload: str) -> dict:
        left = pd.read_parquet(left_path)
        right = pd.read_parquet(right_path)
        return workloads.PANDAS[workload](left, right)

    yield run


@contextmanager
def polars_engine(config: BenchmarkConfig):
    import polars as pl

    left_path, right_path = ensure_dataset(config)

    def run(workload: str) -> dict:
        left = pl.read_parquet(left_path)
        right = pl.read_parquet(right_path)
        return workloads.POLARS[workload](left, right)

    yield run


@contextmanager
def duckdb_engine(config: BenchmarkConfig):
    import duckdb

    left_path, right_path = ensure_dataset(config)
    connection = duckdb.connect()
    connection.execute(f"PRAGMA threads={config.spark_cores}")
    try:
        yield lambda workload: workloads.DUCKDB[workload](
            connection, left_path, right_path
        )
    finally:
        connection.close()


@contextmanager
def spark_engine(config: BenchmarkConfig):
    import os

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    left_path, right_path = ensure_dataset(config)
    spark = (
        SparkSession.builder.master(f"local[{config.spark_cores}]")
        .appName("spark-crossover")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions))
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    def run(workload: str) -> dict:
        left = spark.read.parquet(str(left_path))
        right = spark.read.parquet(str(right_path))
        return workloads.SPARK[workload](spark, left, right)

    try:
        yield run
    finally:
        spark.stop()


ENGINE_CONTEXTS: dict[str, Callable] = {
    "pandas": pandas_engine,
    "polars": polars_engine,
    "duckdb": duckdb_engine,
    "spark": spark_engine,
}


def measure(config: BenchmarkConfig) -> dict:
    """Run one engine on one workload and return setup cost, timings and the answer.

    The answer comes from the final repeat rather than being recomputed, so the thing
    compared across engines is the thing that was timed.
    """
    context = ENGINE_CONTEXTS[config.engine]

    setup_start = time.perf_counter()
    with context(config) as run:
        setup_seconds = time.perf_counter() - setup_start

        for _ in range(config.warmups):
            run(config.workload)

        timings, answer = [], None
        for _ in range(config.repeats):
            started = time.perf_counter()
            answer = run(config.workload)
            timings.append(time.perf_counter() - started)

    return {
        "config": config.as_dict(),
        "setup_seconds": setup_seconds,
        "timings": timings,
        "answer": answer,
    }
