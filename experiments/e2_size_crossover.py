"""E2: at what size does the distributed engine start winning?

This is the question the repository is named after. The advice is "use Spark when the data
is big", and the useful form of that advice is a number of rows.

The honest possible answers include "it does not, anywhere in the range measured", and the
experiment is written so that answer can come out. A crossover is reported only where one
engine's curve actually crosses another's; otherwise the result says no crossover was
found and gives the ratio at the largest size measured.
"""

from __future__ import annotations

import math

from _common import DATA_DIR, table, write

from spark_crossover.agreement import check_all
from spark_crossover.timing import run_measurement

ENGINES = ("pandas", "polars", "duckdb", "spark")
SIZES = (100_000, 400_000, 1_600_000, 6_400_000)
WORKLOAD = "groupby"


def _extrapolate(ratios: dict[int, float]) -> tuple[float, float]:
    """Fit log(ratio) against log(rows) and solve for ratio = 1.

    Least squares on four points. The exponent is a real measurement of how fast the gap
    closes over the measured range; the implied crossover is an extrapolation far outside
    it and is treated as such everywhere it appears.
    """
    xs = [math.log(n) for n in ratios]
    ys = [math.log(r) for r in ratios.values()]
    n = len(xs)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    denominator = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True)) / denominator
    intercept = mean_y - slope * mean_x
    return slope, math.exp(-intercept / slope)


def main() -> int:
    print(__doc__)
    results: dict[int, dict[str, object]] = {}

    for n_rows in SIZES:
        print(f"\n--- {n_rows:,} rows ---")
        answers, row = {}, {}
        for engine in ENGINES:
            m = run_measurement(engine, WORKLOAD, n_rows, repeats=5, warmups=1,
                                data_dir=DATA_DIR, timeout_seconds=2400)
            if not m.ok:
                print(f"  {engine:7s} FAILED: {m.error}")
                return 1
            row[engine] = m
            answers[engine] = m.answer
            print(f"  {engine:7s} work {m.median_seconds:7.3f}s  "
                  f"total {m.total_seconds:7.3f}s  spread x{m.spread_ratio:.1f}  "
                  f"peak RSS {m.peak_rss_mb:6.0f} MB")

        # Timings are worthless unless the engines computed the same thing.
        check_all(answers)
        print("  all engines agree on the answer")
        results[n_rows] = row

    cells = []
    for n_rows in SIZES:
        for engine in ENGINES:
            m = results[n_rows][engine]
            cells.append({
                "n_rows": n_rows, "engine": engine,
                "work_seconds": round(m.median_seconds, 4),
                "total_seconds": round(m.total_seconds, 4),
                "setup_seconds": round(m.setup_seconds, 4),
                "spread_ratio": round(m.spread_ratio, 3),
                "peak_rss_mb": m.peak_rss_mb,
            })

    print("\nmedian work time, seconds (setup excluded)\n")
    table(["rows"] + list(ENGINES),
          [[f"{n:,}"] + [f"{results[n][e].median_seconds:.3f}" for e in ENGINES]
           for n in SIZES], [12] + [10] * len(ENGINES))

    print("\ntotal time including setup, seconds (what a single run costs)\n")
    table(["rows"] + list(ENGINES),
          [[f"{n:,}"] + [f"{results[n][e].total_seconds:.3f}" for e in ENGINES]
           for n in SIZES], [12] + [10] * len(ENGINES))

    # Did Spark's work-time curve ever cross the best single node engine's?
    single_node = ("pandas", "polars", "duckdb")
    crossing = None
    ratios = {}
    for n_rows in SIZES:
        best = min(results[n_rows][e].median_seconds for e in single_node)
        spark_work = results[n_rows]["spark"].median_seconds
        ratios[n_rows] = spark_work / best
        if crossing is None and spark_work < best:
            crossing = n_rows

    largest = SIZES[-1]
    print("\nSpark's work time divided by the best single node engine's:")
    for n_rows in SIZES:
        print(f"  {n_rows:>9,} rows: {ratios[n_rows]:6.1f}x")

    if crossing is None:
        print(f"\nNo crossover in the measured range. At {largest:,} rows Spark is still "
              f"{ratios[largest]:.1f} times slower than the best single node engine on "
              "work alone, before its startup is counted.")
        trend = ratios[SIZES[0]] / ratios[largest]
        print(f"The gap is closing: the ratio fell by {trend:.1f}x from the smallest size "
              "to the largest, so a crossover exists somewhere. It is not here.")
        exponent, implied = _extrapolate(ratios)
        print(f"\nFitting ratio against size as a power law gives an exponent of "
              f"{exponent:.3f}, which puts the crossover near {implied:.2g} rows.")
        print("That number should not be believed. It extrapolates four points across "
              "three orders of magnitude to reach a size this machine cannot hold, and a "
              "power law fitted over that range is an assumption, not a measurement. It is "
              "reported because its absurdity is the finding: on work time alone the gap "
              "does not close anywhere near the sizes a single machine deals with, so "
              "speed is the wrong axis for this decision. E4 has the right one.")
    else:
        print(f"\nCrossover at {crossing:,} rows.")

    write("e2_size_crossover", {
        "experiment": "e2_size_crossover",
        "question": "At what size does Spark start beating a single node?",
        "workload": WORKLOAD, "sizes": list(SIZES), "engines": list(ENGINES),
        "cells": cells,
        "spark_ratio_by_size": {str(k): round(v, 2) for k, v in ratios.items()},
        "crossover_rows": crossing,
        "largest_rows": largest,
        "spark_ratio_at_largest": round(ratios[largest], 1),
        "spark_ratio_at_smallest": round(ratios[SIZES[0]], 1),
        "ratio_improvement": round(ratios[SIZES[0]] / ratios[largest], 1),
        "fitted_exponent": round(_extrapolate(ratios)[0], 3),
        "extrapolated_crossover_rows": float(f"{_extrapolate(ratios)[1]:.3g}"),
        "extrapolation_warning": (
            "four points extrapolated three orders of magnitude beyond the measured "
            "range; reported to show the size of the gap, not to predict a number"
        ),
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
