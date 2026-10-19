"""The four workloads, implemented once per engine, returning a comparable answer.

Two rules hold this file together.

First, every implementation must return the same answer. A benchmark where the engines
compute slightly different things is not a benchmark, and the difference is easy to
introduce by accident: a default that drops nulls, an implicit sort, an index that one
library keeps and another does not. The answers are compared in agreement.py and a
mismatch fails the run.

Second, every implementation must fully materialise its result. Polars has a lazy API,
Spark is lazy throughout, and DuckDB builds a relation that does nothing until it is
consumed. Timing a query that was never executed is the single most common way to publish
a benchmark showing a lazy engine winning by a factor of a thousand. Each function here
ends by pulling concrete Python values out of the engine.
"""

from __future__ import annotations

from typing import Any

Answer = dict[str, Any]


# --------------------------------------------------------------------------- pandas


def pandas_groupby(left, right) -> Answer:
    grouped = left.groupby("key", sort=True).agg(
        mean_value=("value", "mean"), sum_weight=("weight", "sum")
    )
    return {
        "n_groups": int(len(grouped)),
        "sum_mean_value": float(grouped["mean_value"].sum()),
        "sum_weight_total": float(grouped["sum_weight"].sum()),
    }


def pandas_join(left, right) -> Answer:
    merged = left.merge(right, on="key", how="inner")
    merged["scaled"] = merged["value"] * merged["factor"]
    grouped = merged.groupby("key", sort=True)["scaled"].sum()
    return {
        "n_rows": int(len(merged)),
        "n_groups": int(len(grouped)),
        "sum_scaled": float(grouped.sum()),
    }


def pandas_sort(left, right) -> Answer:
    ordered = left.sort_values(["value", "id"], kind="stable")
    middle = ordered.iloc[len(ordered) // 2 : len(ordered) // 2 + 5]
    return {
        "middle_ids": [int(v) for v in middle["id"]],
        "first_id": int(ordered["id"].iloc[0]),
        "last_id": int(ordered["id"].iloc[-1]),
    }


def pandas_distinct(left, right) -> Answer:
    import numpy as np

    bucket = np.floor(left["value"] * 10).astype("int64")
    pairs = left.assign(bucket=bucket)[["key", "bucket"]].drop_duplicates()
    return {"n_distinct": int(len(pairs))}


# --------------------------------------------------------------------------- polars


def polars_groupby(left, right) -> Answer:
    import polars as pl

    grouped = (
        left.lazy()
        .group_by("key")
        .agg(pl.col("value").mean().alias("mean_value"),
             pl.col("weight").sum().alias("sum_weight"))
        .collect()
    )
    return {
        "n_groups": int(grouped.height),
        "sum_mean_value": float(grouped["mean_value"].sum()),
        "sum_weight_total": float(grouped["sum_weight"].sum()),
    }


def polars_join(left, right) -> Answer:
    import polars as pl

    merged = (
        left.lazy()
        .join(right.lazy(), on="key", how="inner")
        .with_columns((pl.col("value") * pl.col("factor")).alias("scaled"))
    )
    grouped = merged.group_by("key").agg(pl.col("scaled").sum()).collect()
    n_rows = merged.select(pl.len()).collect().item()
    return {
        "n_rows": int(n_rows),
        "n_groups": int(grouped.height),
        "sum_scaled": float(grouped["scaled"].sum()),
    }


def polars_sort(left, right) -> Answer:
    ordered = left.lazy().sort(["value", "id"]).collect()
    half = ordered.height // 2
    return {
        "middle_ids": [int(v) for v in ordered["id"][half : half + 5]],
        "first_id": int(ordered["id"][0]),
        "last_id": int(ordered["id"][ordered.height - 1]),
    }


def polars_distinct(left, right) -> Answer:
    import polars as pl

    count = (
        left.lazy()
        .with_columns((pl.col("value") * 10).floor().cast(pl.Int64).alias("bucket"))
        .select(["key", "bucket"])
        .unique()
        .select(pl.len())
        .collect()
        .item()
    )
    return {"n_distinct": int(count)}


# --------------------------------------------------------------------------- duckdb


def duckdb_groupby(connection, left_path, right_path) -> Answer:
    row = connection.execute(
        f"""
        WITH g AS (
            SELECT key, avg(value) AS mean_value, sum(weight) AS sum_weight
            FROM read_parquet('{left_path}') GROUP BY key
        )
        SELECT count(*), sum(mean_value), sum(sum_weight) FROM g
        """
    ).fetchone()
    return {
        "n_groups": int(row[0]),
        "sum_mean_value": float(row[1]),
        "sum_weight_total": float(row[2]),
    }


def duckdb_join(connection, left_path, right_path) -> Answer:
    row = connection.execute(
        f"""
        WITH j AS (
            SELECT l.key, l.value * r.factor AS scaled
            FROM read_parquet('{left_path}') l
            JOIN read_parquet('{right_path}') r USING (key)
        ), g AS (
            SELECT key, sum(scaled) AS s FROM j GROUP BY key
        )
        SELECT (SELECT count(*) FROM j), count(*), sum(s) FROM g
        """
    ).fetchone()
    return {"n_rows": int(row[0]), "n_groups": int(row[1]), "sum_scaled": float(row[2])}


def duckdb_sort(connection, left_path, right_path) -> Answer:
    total = connection.execute(
        f"SELECT count(*) FROM read_parquet('{left_path}')"
    ).fetchone()[0]
    middle = connection.execute(
        f"""SELECT id FROM read_parquet('{left_path}')
            ORDER BY value, id LIMIT 5 OFFSET {total // 2}"""
    ).fetchall()
    ends = connection.execute(
        f"""SELECT first(id ORDER BY value, id), last(id ORDER BY value, id)
            FROM read_parquet('{left_path}')"""
    ).fetchone()
    return {
        "middle_ids": [int(r[0]) for r in middle],
        "first_id": int(ends[0]),
        "last_id": int(ends[1]),
    }


def duckdb_distinct(connection, left_path, right_path) -> Answer:
    row = connection.execute(
        f"""SELECT count(*) FROM (
                SELECT DISTINCT key, CAST(floor(value * 10) AS BIGINT) AS bucket
                FROM read_parquet('{left_path}')
            )"""
    ).fetchone()
    return {"n_distinct": int(row[0])}


# --------------------------------------------------------------------------- spark


def spark_groupby(spark, left, right) -> Answer:
    from pyspark.sql import functions as F

    grouped = left.groupBy("key").agg(
        F.mean("value").alias("mean_value"), F.sum("weight").alias("sum_weight")
    )
    row = grouped.agg(
        F.count("*").alias("n"),
        F.sum("mean_value").alias("a"),
        F.sum("sum_weight").alias("b"),
    ).collect()[0]
    return {
        "n_groups": int(row["n"]),
        "sum_mean_value": float(row["a"]),
        "sum_weight_total": float(row["b"]),
    }


def spark_join(spark, left, right) -> Answer:
    from pyspark.sql import functions as F

    merged = left.join(right, on="key", how="inner").withColumn(
        "scaled", F.col("value") * F.col("factor")
    )
    merged.cache()
    n_rows = merged.count()
    grouped = merged.groupBy("key").agg(F.sum("scaled").alias("s"))
    row = grouped.agg(F.count("*").alias("n"), F.sum("s").alias("t")).collect()[0]
    merged.unpersist()
    return {"n_rows": int(n_rows), "n_groups": int(row["n"]), "sum_scaled": float(row["t"])}


def spark_sort(spark, left, right) -> Answer:
    total = left.count()
    ordered = left.orderBy("value", "id")
    middle = ordered.select("id").limit(total // 2 + 5).tail(5)
    first = ordered.select("id").first()["id"]
    last = ordered.select("id").tail(1)[0]["id"]
    return {
        "middle_ids": [int(r["id"]) for r in middle],
        "first_id": int(first),
        "last_id": int(last),
    }


def spark_distinct(spark, left, right) -> Answer:
    from pyspark.sql import functions as F

    count = (
        left.withColumn("bucket", F.floor(F.col("value") * 10).cast("long"))
        .select("key", "bucket")
        .distinct()
        .count()
    )
    return {"n_distinct": int(count)}


PANDAS = {"groupby": pandas_groupby, "join": pandas_join,
          "sort": pandas_sort, "distinct": pandas_distinct}
POLARS = {"groupby": polars_groupby, "join": polars_join,
          "sort": polars_sort, "distinct": polars_distinct}
DUCKDB = {"groupby": duckdb_groupby, "join": duckdb_join,
          "sort": duckdb_sort, "distinct": duckdb_distinct}
SPARK = {"groupby": spark_groupby, "join": spark_join,
         "sort": spark_sort, "distinct": spark_distinct}
