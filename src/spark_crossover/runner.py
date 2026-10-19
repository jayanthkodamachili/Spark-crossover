"""Run exactly one measurement in this process, print JSON, exit.

Invoked as a subprocess by timing.py so that each measurement starts from a clean process.
Running several engines in one interpreter contaminates the comparison in ways that are
hard to see and easy to publish: a JVM already warm from a previous workload, an engine
holding memory a later engine then cannot get, import side effects, and a filesystem cache
already full of the file the next engine is about to read.

It also means a measurement that dies, by memory cap or by segfault, is reported as a
failed measurement rather than taking the whole experiment with it.
"""

from __future__ import annotations

import argparse
import json
import resource
import sys

from spark_crossover.config import ENGINES, WORKLOADS, BenchmarkConfig, ConfigError
from spark_crossover.engines import measure


def apply_memory_cap(megabytes: int) -> None:
    """Cap this process's address space.

    RLIMIT_AS rather than a cgroup, because it needs no privileges and fails the process
    deterministically at a known number.

    The cap is on virtual address space, not resident memory, and the two are not the same
    number. An engine that reserves address space generously fails a cap it would never
    have needed, which is not a hypothetical: Polars aborts under a 2500 MB cap on a sort
    where its peak resident memory is a fraction of that, while pandas completes. Peak
    resident memory is therefore reported next to cap survival, and ADR-004 says which
    question each one answers.
    """
    limit = megabytes * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="spark-crossover-runner")
    parser.add_argument("--engine", choices=ENGINES, required=True)
    parser.add_argument("--workload", choices=WORKLOADS, required=True)
    parser.add_argument("--n-rows", type=int, required=True)
    parser.add_argument("--n-groups", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20260219)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--memory-limit-mb", type=int, default=None)
    parser.add_argument("--spark-cores", type=int, default=2)
    args = parser.parse_args(argv)

    try:
        config = BenchmarkConfig(
            engine=args.engine, workload=args.workload, n_rows=args.n_rows,
            n_groups=args.n_groups, repeats=args.repeats, warmups=args.warmups,
            seed=args.seed, data_dir=args.data_dir,
            memory_limit_mb=args.memory_limit_mb, spark_cores=args.spark_cores,
        )
    except ConfigError as exc:
        print(json.dumps({"ok": False, "error": f"config: {exc}"}))
        return 2

    # The dataset is written before the cap is applied. Generating it is the harness's
    # job, not the engine's, and letting the cap kill data generation would report a
    # memory failure for an engine that never ran.
    from spark_crossover.data import ensure_dataset

    ensure_dataset(config)

    if config.memory_limit_mb is not None:
        apply_memory_cap(config.memory_limit_mb)

    try:
        result = measure(config)
    except MemoryError as exc:
        print(json.dumps({"ok": False, "error": "MemoryError", "detail": str(exc)[:200]}))
        return 3
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": type(exc).__name__,
                          "detail": str(exc)[:400]}))
        return 4

    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(json.dumps({"ok": True, "peak_rss_mb": round(peak_rss_mb, 1), **result}))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
