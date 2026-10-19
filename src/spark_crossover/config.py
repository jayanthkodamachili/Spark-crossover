"""Benchmark configuration, validated on construction.

A benchmark with a silently wrong setting produces numbers that look fine and mean
something else, which is worse than a crash. Every constraint that would invalidate a
measurement is checked here rather than discovered in the results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

ENGINES = ("pandas", "polars", "duckdb", "spark")
WORKLOADS = ("groupby", "join", "sort", "distinct")

MIN_REPEATS = 3


class ConfigError(ValueError):
    """Raised when a configuration would produce a measurement that cannot be trusted."""


@dataclass(frozen=True)
class BenchmarkConfig:
    """One benchmark: an engine, a workload, a data size, and how to time it."""

    engine: str = "pandas"
    workload: str = "groupby"
    n_rows: int = 1_000_000
    n_groups: int = 1_000
    repeats: int = 5
    warmups: int = 1
    seed: int = 20260219
    data_dir: Path = field(default=Path("data"))
    memory_limit_mb: int | None = None
    spark_cores: int = 2
    spark_shuffle_partitions: int = 4

    def __post_init__(self) -> None:
        if self.engine not in ENGINES:
            raise ConfigError(f"engine must be one of {ENGINES}, got {self.engine!r}")
        if self.workload not in WORKLOADS:
            raise ConfigError(f"workload must be one of {WORKLOADS}, got {self.workload!r}")
        if self.n_rows < 1000:
            raise ConfigError(
                f"n_rows={self.n_rows} is below 1000. At that size every engine is measuring "
                "its own startup and the comparison says nothing about data processing."
            )
        if not 1 <= self.n_groups <= self.n_rows:
            raise ConfigError(
                f"n_groups={self.n_groups} must be between 1 and n_rows={self.n_rows}"
            )
        if self.repeats < MIN_REPEATS:
            raise ConfigError(
                f"repeats={self.repeats} is below {MIN_REPEATS}. A median over fewer than "
                f"{MIN_REPEATS} runs is not a median, it is one run with extra steps, and "
                "timing noise on a shared machine is large enough to invert a comparison."
            )
        if self.warmups < 0:
            raise ConfigError(f"warmups={self.warmups} cannot be negative")
        if self.memory_limit_mb is not None and self.memory_limit_mb < 128:
            raise ConfigError(
                f"memory_limit_mb={self.memory_limit_mb} is below 128, where a Python "
                "interpreter cannot reliably start and the failure would be attributed to "
                "the engine rather than to the cap."
            )
        if self.spark_cores < 1:
            raise ConfigError(f"spark_cores={self.spark_cores} must be at least 1")

    @property
    def dataset_name(self) -> str:
        """Datasets are identified by shape and seed, so two configs that need the same
        bytes share them rather than racing to write the same path."""
        return f"rows{self.n_rows}_groups{self.n_groups}_seed{self.seed}"

    @property
    def dataset_path(self) -> Path:
        return Path(self.data_dir) / f"{self.dataset_name}.parquet"

    @property
    def right_path(self) -> Path:
        """The right hand side of the join, generated alongside the main table."""
        return Path(self.data_dir) / f"{self.dataset_name}_right.parquet"

    def as_dict(self) -> dict:
        return {
            "engine": self.engine,
            "workload": self.workload,
            "n_rows": self.n_rows,
            "n_groups": self.n_groups,
            "repeats": self.repeats,
            "warmups": self.warmups,
            "seed": self.seed,
            "memory_limit_mb": self.memory_limit_mb,
            "spark_cores": self.spark_cores,
            "spark_shuffle_partitions": self.spark_shuffle_partitions,
        }
