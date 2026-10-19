"""Command line entry point.

Exit code 5 is the one worth having: no single node engine finished this workload under
the memory you said you had. That is the only honest machine readable case for reaching
for a cluster, and it is a different question from "is pandas slow".

Exit code 4 is the one that protects the rest. If the engines disagree about the answer,
every timing in the run is meaningless, and the tool refuses rather than printing a
ranking nobody should act on.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from spark_crossover.agreement import AgreementError, check_all
from spark_crossover.config import ENGINES, WORKLOADS
from spark_crossover.timing import run_measurement

EXIT_FITS_ON_ONE_NODE = 0
EXIT_TIGHT = 1
EXIT_BAD_CONFIG = 2
EXIT_INTERNAL_ERROR = 3
EXIT_ENGINES_DISAGREE = 4
EXIT_NEEDS_MORE_THAN_ONE_NODE = 5

SINGLE_NODE = ("pandas", "polars", "duckdb")
TIGHT_FRACTION = 0.8


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crossover",
        description="Measure whether this workload needs more than one machine.",
    )
    parser.add_argument("--workload", choices=WORKLOADS, default="groupby")
    parser.add_argument("--n-rows", type=int, default=1_000_000)
    parser.add_argument("--n-groups", type=int, default=1000)
    parser.add_argument("--engines", nargs="+", choices=ENGINES, default=list(SINGLE_NODE))
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--memory-budget-mb", type=int, default=None,
        help="the memory this workload is allowed on the target machine",
    )
    parser.add_argument("--json", action="store_true")
    return parser


def classify(results: list, budget_mb: int | None) -> int:
    survivors = [r for r in results if r.ok]
    single_node_survivors = [r for r in survivors if r.engine in SINGLE_NODE]

    if not single_node_survivors:
        return EXIT_NEEDS_MORE_THAN_ONE_NODE
    if budget_mb is not None:
        headroom = min(r.peak_rss_mb for r in single_node_survivors)
        if headroom > budget_mb * TIGHT_FRACTION:
            return EXIT_TIGHT
    return EXIT_FITS_ON_ONE_NODE


def render(results: list, budget_mb: int | None) -> str:
    lines = [f"{'engine':<10}{'work (s)':>11}{'total (s)':>11}{'peak RSS':>11}  status",
             "-" * 56]
    for r in results:
        if r.ok:
            lines.append(f"{r.engine:<10}{r.median_seconds:>11.3f}{r.total_seconds:>11.3f}"
                         f"{r.peak_rss_mb:>9.0f} MB  ok")
        else:
            lines.append(f"{r.engine:<10}{'':>11}{'':>11}{'':>11}  "
                         f"{r.failure_kind or 'failed'}")

    survivors = [r for r in results if r.ok and r.engine in SINGLE_NODE]
    if survivors:
        best = min(survivors, key=lambda r: r.total_seconds)
        lines.append("")
        lines.append(f"fastest single node engine: {best.engine} at "
                     f"{best.total_seconds:.3f}s, peak RSS {best.peak_rss_mb:.0f} MB")
        if budget_mb is not None:
            lightest = min(survivors, key=lambda r: r.peak_rss_mb)
            lines.append(f"lightest single node engine: {lightest.engine} at "
                         f"{lightest.peak_rss_mb:.0f} MB against a budget of {budget_mb} MB")
    else:
        lines.append("")
        lines.append("no single node engine completed this workload")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    results = []
    try:
        for engine in args.engines:
            results.append(run_measurement(
                engine, args.workload, args.n_rows, repeats=args.repeats,
                warmups=args.warmups, n_groups=args.n_groups, data_dir=args.data_dir,
                memory_limit_mb=args.memory_budget_mb,
            ))
    except Exception as exc:  # noqa: BLE001
        print(f"benchmark failed to complete: {exc}", file=sys.stderr)
        return EXIT_INTERNAL_ERROR

    # Config errors arrive here as a failed measurement rather than an exception, because
    # the configuration is validated inside the subprocess that does the work. An earlier
    # version also caught ConfigError around the loop above. That handler could never run:
    # the exception is raised in a different process and arrives as JSON on stdout. It was
    # deleted rather than tested, because a test for it would have had to fake the thing
    # it was claiming to protect against.
    if any(r.failure_kind == "other" and "config" in (r.error or "") for r in results):
        print("invalid benchmark: " + next(r.error for r in results if r.error),
              file=sys.stderr)
        return EXIT_BAD_CONFIG

    answers = {r.engine: r.answer for r in results if r.ok and r.answer is not None}
    if len(answers) > 1:
        try:
            check_all(answers)
        except AgreementError as exc:
            print(str(exc), file=sys.stderr)
            return EXIT_ENGINES_DISAGREE

    if args.json:
        print(json.dumps({"results": [r.as_dict() for r in results]}, indent=2))
    else:
        print(render(results, args.memory_budget_mb))
    return classify(results, args.memory_budget_mb)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
