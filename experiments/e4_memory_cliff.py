"""E4: the axis that actually matters, which is not speed.

E2 found no speed crossover anywhere a single machine can reach. But the usual reason
teams reach for a cluster is not that pandas is slow. It is that pandas died.

This experiment caps the address space available to a process and asks which engines still
finish. That is a fair model of a container with a hard memory limit, which is where most
of this code really runs, and it is the question "is the data too big for one machine"
asked precisely enough to have an answer.

Two things are measured, because they disagree. Peak resident memory is what an engine
actually uses. Cap survival is whether it completes under a hard address space limit, and
an engine that reserves address space generously can fail a cap it never needed.
"""

from __future__ import annotations

from _common import DATA_DIR, table, write

from spark_crossover.timing import run_measurement

ENGINES = ("pandas", "polars", "duckdb", "spark")
CAPS_MB = (750, 1500, 3000, 6000)
CAPPED_TIMEOUT_SECONDS = 900
N_ROWS = 6_400_000
WORKLOAD = "sort"


def main() -> int:
    print(__doc__)

    print(f"First, peak resident memory with no cap, {WORKLOAD} on {N_ROWS:,} rows.\n")
    uncapped = {}
    for engine in ENGINES:
        m = run_measurement(engine, WORKLOAD, N_ROWS, repeats=3, warmups=1,
                            data_dir=DATA_DIR, timeout_seconds=2400)
        uncapped[engine] = m
        status = f"{m.median_seconds:7.3f}s" if m.ok else f"FAILED {m.failure_kind}"
        print(f"  {engine:7s} peak RSS {m.peak_rss_mb:7.0f} MB   {status}")

    print(f"\nNow under address space caps.\n")
    survival: dict[int, dict[str, object]] = {}
    cells = []
    for cap in CAPS_MB:
        survival[cap] = {}
        line = []
        for engine in ENGINES:
            # A process thrashing just under a cap can run for a very long time before
            # the allocation that finally fails. The cut off is generous relative to the
            # uncapped run, which finishes in seconds, and is recorded with the result.
            m = run_measurement(engine, WORKLOAD, N_ROWS, repeats=3, warmups=0,
                                data_dir=DATA_DIR, memory_limit_mb=cap,
                                timeout_seconds=CAPPED_TIMEOUT_SECONDS)
            survival[cap][engine] = m
            cells.append({
                "cap_mb": cap, "engine": engine, "ok": m.ok,
                "failure_kind": m.failure_kind,
                "work_seconds": round(m.median_seconds, 4) if m.ok else None,
                "peak_rss_mb": m.peak_rss_mb if m.ok else None,
                "error": None if m.ok else (m.error or "")[:200],
            })
            line.append(f"{engine}={'ok' if m.ok else (m.failure_kind or 'failed')}")
        print(f"  cap {cap:5d} MB   " + "   ".join(line))

    print(f"\npeak resident memory, uncapped, {WORKLOAD} on {N_ROWS:,} rows\n")
    table(["engine", "peak RSS (MB)", "work (s)"],
          [[e, f"{uncapped[e].peak_rss_mb:.0f}",
            f"{uncapped[e].median_seconds:.3f}" if uncapped[e].ok else "failed"]
           for e in ENGINES], [10, 16, 12])

    print("\nsurvival under an address space cap\n")
    table(["cap (MB)"] + list(ENGINES),
          [[str(c)] + ["ok" if survival[c][e].ok else (survival[c][e].failure_kind or "failed")
                       for e in ENGINES] for c in CAPS_MB],
          [10] + [20] * len(ENGINES))

    # Spark under RLIMIT_AS is not a measurement. The JVM reserves address space far
    # beyond what it uses, so the gateway dies before any work starts, at caps where
    # Spark would have run. Scoring that as a loss would be publishing a false result.
    not_measurable = {
        e for e in ENGINES
        if any(survival[c][e].failure_kind == "jvm_not_measurable" for c in CAPS_MB)
    }

    lowest_survived = {}
    for engine in ENGINES:
        if engine in not_measurable:
            lowest_survived[engine] = None
            continue
        passed = [c for c in CAPS_MB if survival[c][engine].ok]
        lowest_survived[engine] = min(passed) if passed else None

    print("\nlowest cap survived\n")
    for engine in ENGINES:
        if engine in not_measurable:
            print(f"  {engine:7s} not measurable with this instrument")
            continue
        value = lowest_survived[engine]
        print(f"  {engine:7s} {str(value) + ' MB' if value else 'none of the caps tested'}")

    if not_measurable:
        print(f"\n{', '.join(sorted(not_measurable))} cannot be capped this way: the JVM "
              "reserves address space far beyond what it uses, so it fails to start at "
              "caps it would have run under. That is the instrument not fitting the "
              "engine, not the engine losing, and it is recorded as such.")

    kinds = {c: {e: survival[c][e].failure_kind
                 for e in ENGINES
                 if not survival[c][e].ok and e not in not_measurable}
             for c in CAPS_MB}
    distinct_kinds = sorted({k for row in kinds.values() for k in row.values() if k})

    print(f"\nThe engines do not fail the same way. Observed failure modes: "
          f"{', '.join(distinct_kinds)}.")
    print("Only a MemoryError is something your code can catch. The others end the "
          "process, so a retry has to live outside it.")
    print("Which allocation fails first is not deterministic, so the same cell can "
          "report a MemoryError on one run and a different allocation failure on the "
          "next. The raw error text is recorded per cell for exactly that reason: an "
          "engine that fails differently each time is an engine you cannot write a "
          "reliable handler for, and that is worth knowing before you try.")

    write("e4_memory_cliff", {
        "experiment": "e4_memory_cliff",
        "question": "Which engines survive a hard memory cap, and how do the rest fail?",
        "n_rows": N_ROWS, "workload": WORKLOAD, "caps_mb": list(CAPS_MB),
        "engines": list(ENGINES),
        "uncapped": {e: {"peak_rss_mb": uncapped[e].peak_rss_mb,
                         "work_seconds": round(uncapped[e].median_seconds, 4),
                         "ok": uncapped[e].ok} for e in ENGINES},
        "cells": cells,
        "lowest_cap_survived_mb": lowest_survived,
        "not_measurable_under_cap": sorted(not_measurable),
        "failure_kinds_observed": distinct_kinds,
        "lowest_duckdb_cap_mb": lowest_survived.get("duckdb"),
        "lowest_pandas_cap_mb": lowest_survived.get("pandas"),
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
