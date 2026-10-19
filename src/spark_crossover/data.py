"""Generate the benchmark dataset once, to Parquet, and let every engine read the same bytes.

Every engine reads the identical file. An engine handed data in its own preferred
in-memory format is being measured on a different task from one that has to parse a file,
and the difference is large enough to reverse a ranking.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from spark_crossover.config import BenchmarkConfig

RIGHT_ROWS_PER_GROUP = 1


def build_table(n_rows: int, n_groups: int, seed: int) -> pa.Table:
    """Deterministic columns, fixed dtypes, no nulls.

    int64 and float64 throughout rather than the narrowest type that fits. A benchmark
    where one engine happens to choose int32 is measuring a dtype decision, and the
    engines differ in what they infer from Parquet otherwise.
    """
    rng = np.random.default_rng(seed)
    return pa.table(
        {
            "id": pa.array(np.arange(n_rows, dtype=np.int64)),
            "key": pa.array(rng.integers(0, n_groups, size=n_rows, dtype=np.int64)),
            "value": pa.array(rng.normal(0.0, 1.0, size=n_rows).astype(np.float64)),
            "weight": pa.array(rng.uniform(0.5, 1.5, size=n_rows).astype(np.float64)),
        }
    )


def build_right_table(n_groups: int, seed: int) -> pa.Table:
    """A small dimension table: one row per key, which is the shape most joins actually are."""
    rng = np.random.default_rng(seed + 1)
    return pa.table(
        {
            "key": pa.array(np.arange(n_groups, dtype=np.int64)),
            "factor": pa.array(rng.uniform(1.0, 2.0, size=n_groups).astype(np.float64)),
        }
    )


def ensure_dataset(config: BenchmarkConfig) -> tuple[Path, Path]:
    """Write the dataset if it is not already present, and return both paths.

    Written once and reused across engines and repeats. Regenerating per run would put
    several seconds of numpy into a measurement that is supposed to be about the engine,
    and would make the first engine measured look slow for no reason connected to it.
    """
    left, right = config.dataset_path, config.right_path
    left.parent.mkdir(parents=True, exist_ok=True)

    if not left.exists():
        table = build_table(config.n_rows, config.n_groups, config.seed)
        pq.write_table(table, left, compression="snappy")
    if not right.exists():
        pq.write_table(
            build_right_table(config.n_groups, config.seed), right, compression="snappy"
        )
    return left, right


def dataset_bytes(config: BenchmarkConfig) -> int:
    left, right = ensure_dataset(config)
    return left.stat().st_size + right.stat().st_size
