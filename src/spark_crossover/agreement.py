"""Check that the engines computed the same thing before comparing how fast they did it.

This is the load bearing check in the repository. A benchmark whose engines quietly
disagree is worse than no benchmark, because it looks like evidence.
"""

from __future__ import annotations

from typing import Any

# Floating point addition is not associative, and these engines sum in different orders:
# pandas accumulates in one pass, DuckDB in vectorised chunks, Spark per partition and then
# across partitions. Summing several million normal draws, the results agree to roughly
# twelve significant figures and not beyond. An exact comparison would fail on correct
# engines, so the tolerance is relative and stated rather than tuned until green.
RELATIVE_TOLERANCE = 1e-9


class AgreementError(AssertionError):
    """Raised when two engines produced different answers for the same workload."""


def _close(left: float, right: float) -> bool:
    scale = max(abs(left), abs(right), 1.0)
    return abs(left - right) <= RELATIVE_TOLERANCE * scale


def compare(reference: dict[str, Any], other: dict[str, Any]) -> list[str]:
    """Return a list of human readable differences, empty when the answers agree."""
    problems = []
    for key in sorted(set(reference) | set(other)):
        if key not in reference or key not in other:
            problems.append(f"{key}: present in only one answer")
            continue
        a, b = reference[key], other[key]
        if isinstance(a, list) or isinstance(b, list):
            if list(a) != list(b):
                problems.append(f"{key}: {a} != {b}")
        elif isinstance(a, int) and isinstance(b, int):
            if a != b:
                problems.append(f"{key}: {a} != {b} (exact comparison, these are counts)")
        elif not _close(float(a), float(b)):
            problems.append(f"{key}: {a} != {b} beyond relative tolerance")
    return problems


def check_all(answers: dict[str, dict], reference_engine: str = "pandas") -> None:
    """Raise unless every engine agrees with the reference."""
    if reference_engine not in answers:
        reference_engine = sorted(answers)[0]
    reference = answers[reference_engine]

    failures = {}
    for engine, answer in answers.items():
        if engine == reference_engine:
            continue
        problems = compare(reference, answer)
        if problems:
            failures[engine] = problems

    if failures:
        lines = [f"engines disagree with {reference_engine}:"]
        for engine, problems in sorted(failures.items()):
            for problem in problems:
                lines.append(f"  {engine}: {problem}")
        lines.append(
            "Timings from this run are meaningless: the engines did not compute the "
            "same thing. Fix the workload implementation before reading any number."
        )
        raise AgreementError("\n".join(lines))
