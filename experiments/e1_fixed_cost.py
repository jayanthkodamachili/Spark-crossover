"""E1: what does each engine cost before it has done anything at all?

The question behind this one is the simplest in the repository and the least often asked.
Every comparison of "Spark versus pandas" assumes the comparison starts at zero. It does
not. A JVM has to come up, and until the workload is large enough to out-earn that, no
amount of distributed throughput can put Spark ahead.

The output is a floor: the smallest total runtime an engine can possibly report.
"""

from __future__ import annotations

import statistics

from _common import DATA_DIR, table, write

from spark_crossover.timing import run_measurement

ENGINES = ("pandas", "polars", "duckdb", "spark")
LAUNCHES = 5
TINY_ROWS = 1000


def main() -> int:
    print(__doc__)
    print(f"Launching each engine {LAUNCHES} times on a {TINY_ROWS} row groupby, which is "
          "small enough that the workload is not what is being measured.\n")

    cells, setups = [], {}
    for engine in ENGINES:
        observed = []
        for _ in range(LAUNCHES):
            m = run_measurement(engine, "groupby", TINY_ROWS, repeats=3, warmups=0,
                                n_groups=10, data_dir=DATA_DIR, timeout_seconds=900)
            if not m.ok:
                print(f"{engine} failed: {m.error}")
                return 1
            observed.append({"setup": m.setup_seconds, "work": m.median_seconds,
                             "total": m.total_seconds})

        setup = statistics.median(o["setup"] for o in observed)
        work = statistics.median(o["work"] for o in observed)
        totals = [o["total"] for o in observed]
        total = statistics.median(totals)
        setups[engine] = setup
        cells.append({
            "engine": engine, "setup_seconds": round(setup, 4),
            "work_seconds": round(work, 4), "floor_seconds": round(total, 4),
            # Spread across separate process launches, which is the variability a reader
            # actually faces when they rerun this. Within one process the repeats agree
            # far more closely than this, and quoting that tighter number would overstate
            # how reproducible any of these timings are.
            "launch_spread_ratio": round(max(totals) / min(totals), 3),
        })

    fastest = min(c["floor_seconds"] for c in cells)
    for cell in cells:
        cell["floor_multiple"] = round(cell["floor_seconds"] / fastest, 1)

    table(["engine", "setup (s)", "work (s)", "floor (s)", "x fastest", "launch spread"],
          [[c["engine"], f"{c['setup_seconds']:.3f}", f"{c['work_seconds']:.4f}",
            f"{c['floor_seconds']:.3f}", f"{c['floor_multiple']:.1f}",
            f"x{c['launch_spread_ratio']:.2f}"] for c in cells],
          [10, 11, 10, 11, 11, 14])

    spark_floor = next(c for c in cells if c["engine"] == "spark")["floor_seconds"]
    duck_floor = next(c for c in cells if c["engine"] == "duckdb")["floor_seconds"]

    print(f"\nSpark cannot finish anything in under {spark_floor:.2f}s on this machine, "
          f"which is {spark_floor / duck_floor:.0f} times DuckDB's floor.")
    print("Any workload DuckDB finishes faster than that is a workload Spark cannot win, "
          "whatever happens inside the engine.")

    worst_spread = max(c["launch_spread_ratio"] for c in cells)
    print(f"\nAcross separate launches the same measurement varies by up to "
          f"x{worst_spread:.2f} on this machine. Every timing in this repository carries "
          "that much noise, which is why the conclusions are drawn from gaps of tens of "
          "times rather than from small differences, and why rerunning an experiment "
          "will not reproduce its numbers exactly.")

    write("e1_fixed_cost", {
        "experiment": "e1_fixed_cost",
        "question": "What does each engine cost before doing any work?",
        "launches": LAUNCHES, "tiny_rows": TINY_ROWS,
        "cells": cells,
        "spark_floor_seconds": spark_floor,
        "duckdb_floor_seconds": duck_floor,
        "spark_floor_multiple": round(spark_floor / duck_floor, 1),
        "spark_setup_seconds": round(setups["spark"], 3),
        "worst_launch_spread_ratio": worst_spread,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
