"""Measure where a distributed engine starts beating a single node.

The advice is always "use Spark when the data is big". That sentence contains no number,
and the number is the only part that matters. This package measures it.
"""

from spark_crossover.config import BenchmarkConfig, ConfigError

__all__ = ["BenchmarkConfig", "ConfigError"]
__version__ = "0.1.0"
