"""E3: is there one crossover, or one per operation?

"Switch to Spark above N rows" assumes a single number exists. Different operations put
different pressure on an engine: a grouped aggregate is mostly a hash table, a sort is
mostly comparisons and memory traffic, a join against a small table may not shuffle at
all, and a distinct over a wide key is a shuffle with nowhere to hide.

I expected the engine rankings to change between operations, and planned to argue from
that. They did not. At this size one engine wins all four, and the single node engines
finish in the same order every time. The guess was wrong and the experiment is written up
as it ran rather than rephrased around what it found.

The conclusion survives anyway, by a different route. Spark's deficit varies several fold
across the four operations at one fixed size, so the row count at which it would catch up
is a different number for each of them. Advice shaped like "above N rows" is still
unanswerable as asked, not because the ranking is unstable but because N is not one number.
"""

from __future__ import annotations

from _common import DATA_DIR, table, write

from spark_crossover.agreement import check_all
from spark_crossover.config import WORKLOADS
from spark_crossover.timing import run_measurement

ENGINES = ("pandas", "polars", "duckdb", "spark")
N_ROWS = 1_600_000


def main() -> int:
    print(__doc__)
    print(f"All four workloads at {N_ROWS:,} rows.\n")

    cells, grid = [], {}
    for workload in WORKLOADS:
        answers, row = {}, {}
        for engine in ENGINES:
            m = run_measurement(engine, workload, N_ROWS, repeats=5, warmups=1,
                                data_dir=DATA_DIR, timeout_seconds=2400)
            if not m.ok:
                print(f"  {workload}/{engine} FAILED: {m.error}")
                return 1
            row[engine] = m
            answers[engine] = m.answer
            cells.append({
                "workload": workload, "engine": engine,
                "work_seconds": round(m.median_seconds, 4),
                "total_seconds": round(m.total_seconds, 4),
                "peak_rss_mb": m.peak_rss_mb,
            })
        check_all(answers)
        grid[workload] = row
        print(f"  {workload:9s} done, engines agree")

    print("\nmedian work time by operation, seconds\n")
    table(["workload"] + list(ENGINES),
          [[w] + [f"{grid[w][e].median_seconds:.3f}" for e in ENGINES] for w in WORKLOADS],
          [12] + [10] * len(ENGINES))

    winners = {w: min(ENGINES, key=lambda e: grid[w][e].median_seconds) for w in WORKLOADS}
    spark_ratio = {
        w: grid[w]["spark"].median_seconds
        / min(grid[w][e].median_seconds for e in ("pandas", "polars", "duckdb"))
        for w in WORKLOADS
    }

    print("\nfastest engine per operation, and how far behind Spark is\n")
    table(["workload", "fastest", "spark is"],
          [[w, winners[w], f"{spark_ratio[w]:.1f}x slower"] for w in WORKLOADS],
          [12, 12, 16])

    best, worst = min(spark_ratio, key=spark_ratio.get), max(spark_ratio, key=spark_ratio.get)
    spread = spark_ratio[worst] / spark_ratio[best]

    # Does the ranking of the single node engines change between operations?
    orders = {
        w: tuple(sorted(("pandas", "polars", "duckdb"),
                        key=lambda e: grid[w][e].median_seconds))
        for w in WORKLOADS
    }
    distinct_orders = len(set(orders.values()))

    print(f"\nSpark's deficit ranges from {spark_ratio[best]:.1f}x on {best} to "
          f"{spark_ratio[worst]:.1f}x on {worst}, a spread of {spread:.1f}x across "
          "operations at one fixed size.")
    print(f"The single node engines finish in {distinct_orders} distinct ordering(s) "
          f"across the {len(WORKLOADS)} operations.")
    if distinct_orders == 1:
        print("That refutes the guess this experiment was built on. The ranking is "
              "stable: one engine wins every operation, in the same order each time.")
        print("The question survives the refutation. Spark would need a different row "
              f"count to catch up on each operation, spread over {spread:.1f}x here, so "
              "there is still no single N behind the advice 'switch above N rows'.")
    else:
        print("So the ranking depends on the operation, and no single row count answers "
              "the question for all of them.")

    write("e3_by_operation", {
        "experiment": "e3_by_operation",
        "question": "Is there one crossover, or one per operation?",
        "hypothesis": "engine rankings would change between operations",
        "hypothesis_held": False,
        "n_rows": N_ROWS, "engines": list(ENGINES), "workloads": list(WORKLOADS),
        "cells": cells,
        "spark_ratio_by_workload": {w: round(v, 1) for w, v in spark_ratio.items()},
        "fastest_by_workload": winners,
        "best_workload_for_spark": best,
        "worst_workload_for_spark": worst,
        "spark_ratio_spread": round(spread, 1),
        "distinct_single_node_orderings": distinct_orders,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
