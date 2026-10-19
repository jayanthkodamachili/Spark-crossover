"""Spawn measurements as subprocesses and summarise them.

The summary is a median rather than a mean, and the spread is published alongside it. On a
shared machine the slow tail is caused by something other than the engine, and a mean
quietly imports that something into the result.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

RUNNER = "spark_crossover.runner"


@dataclass(frozen=True)
class Measurement:
    """One engine, one workload, one size: what it cost and whether it worked."""

    engine: str
    workload: str
    n_rows: int
    ok: bool
    setup_seconds: float = 0.0
    median_seconds: float = 0.0
    min_seconds: float = 0.0
    max_seconds: float = 0.0
    spread_ratio: float = 0.0
    answer: dict | None = None
    error: str | None = None
    failure_kind: str | None = None
    peak_rss_mb: float = 0.0

    @property
    def total_seconds(self) -> float:
        """Median workload time plus the one time setup cost.

        The number a person actually waits for when running this once, which is the
        common case and the one where Spark's startup is not amortised over anything.
        """
        return self.median_seconds + self.setup_seconds

    def as_dict(self) -> dict:
        return {
            "engine": self.engine, "workload": self.workload, "n_rows": self.n_rows,
            "ok": self.ok, "setup_seconds": round(self.setup_seconds, 4),
            "median_seconds": round(self.median_seconds, 4),
            "min_seconds": round(self.min_seconds, 4),
            "max_seconds": round(self.max_seconds, 4),
            "spread_ratio": round(self.spread_ratio, 4),
            "total_seconds": round(self.total_seconds, 4),
            "error": self.error, "failure_kind": self.failure_kind,
            "peak_rss_mb": round(self.peak_rss_mb, 1),
        }


def run_measurement(
    engine: str, workload: str, n_rows: int, *, repeats: int = 5, warmups: int = 1,
    n_groups: int = 1000, data_dir: str | Path = "data",
    memory_limit_mb: int | None = None, spark_cores: int = 2,
    timeout_seconds: int = 1800, python: str | None = None,
) -> Measurement:
    command = [
        python or sys.executable, "-m", RUNNER,
        "--engine", engine, "--workload", workload, "--n-rows", str(n_rows),
        "--n-groups", str(n_groups), "--repeats", str(repeats),
        "--warmups", str(warmups), "--data-dir", str(data_dir),
        "--spark-cores", str(spark_cores),
    ]
    if memory_limit_mb is not None:
        command += ["--memory-limit-mb", str(memory_limit_mb)]

    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout_seconds, check=False
        )
    except subprocess.TimeoutExpired:
        return Measurement(engine, workload, n_rows, ok=False,
                           error=f"timeout after {timeout_seconds}s",
                           failure_kind="timeout")
    except OSError as exc:
        # A missing or unexecutable interpreter is a harness problem, not an engine
        # result. It is reported the same way as any other failed measurement so that a
        # sweep records which cell failed and why instead of ending at the first one.
        return Measurement(engine, workload, n_rows, ok=False,
                           error=f"could not launch {command[0]}: {exc}",
                           failure_kind="launch_failure")

    payload = _last_json_line(completed.stdout)
    if payload is None:
        tail = (completed.stderr or completed.stdout or "").strip()[-300:]
        if completed.returncode < 0:
            return Measurement(
                engine, workload, n_rows, ok=False, failure_kind="aborted",
                error=f"killed by signal {-completed.returncode}: {tail}",
            )
        return Measurement(engine, workload, n_rows, ok=False, failure_kind="no_result",
                           error=f"no result (exit {completed.returncode}): {tail}")
    if not payload.get("ok"):
        detail = payload.get("detail", "")
        return Measurement(
            engine, workload, n_rows, ok=False,
            error=f"{payload.get('error', 'unknown')}: {detail}".strip(": "),
            failure_kind=classify_failure(payload.get("error", ""), detail),
        )

    timings = payload["timings"]
    low, high = min(timings), max(timings)
    return Measurement(
        engine=engine, workload=workload, n_rows=n_rows, ok=True,
        setup_seconds=payload["setup_seconds"],
        median_seconds=statistics.median(timings),
        min_seconds=low, max_seconds=high,
        spread_ratio=high / low if low > 0 else 0.0,
        answer=payload["answer"], peak_rss_mb=payload.get("peak_rss_mb", 0.0),
    )


NATIVE_LOAD_MARKERS = ("binary is missing", "is not defined", "undefined symbol",
                       "failed to map")

# Which allocation fails first under a cap is not deterministic. The same pandas sort
# reported MemoryError on one run and an Arrow allocation failure on the next, and the
# first version of this classifier called the second one "other", which reads like an
# unexplained crash rather than the memory limit it is.
MEMORY_MARKERS = ("memoryerror", "out of memory", "bad_alloc", "malloc",
                  "allocation failed", "cannot allocate", "arrowmemoryerror",
                  "outofmemoryerror")

# The JVM reserves address space far beyond what it will use, so RLIMIT_AS stops it
# starting at caps it would have run under comfortably. The gateway dying before it sends
# its port is that failure, and recording it as a memory failure would be a false result:
# Spark is not being beaten here, it is not being measured.
JVM_NOT_MEASURABLE_MARKERS = ("java_gateway_exited", "java gateway process exited")


def classify_failure(error: str, detail: str) -> str:
    """Name the mechanism, because these engines do not fail the same way.

    Under an address space cap, pandas raises MemoryError, which Python code can catch.
    DuckDB and Polars abort the process. Polars can also fail earlier and stranger: its
    native library fails to map and the first symptom is a NameError about a missing
    symbol, which reads like a broken install rather than a memory limit. Flattening all
    of these into "failed" would hide the most useful thing in the experiment, which is
    that only one of the three gives you something to catch.

    jvm_not_measurable is deliberately not a failure. It means the instrument did not fit
    the engine, and reporting it as a loss would be publishing a result that is not true.
    """
    blob = f"{error} {detail}".lower()
    if any(marker in blob for marker in JVM_NOT_MEASURABLE_MARKERS):
        return "jvm_not_measurable"
    if error == "MemoryError" or any(marker in blob for marker in MEMORY_MARKERS):
        return "memory_error"
    if any(marker in blob for marker in NATIVE_LOAD_MARKERS):
        return "native_load_failure"
    return "other"


def _last_json_line(text: str) -> dict | None:
    """Engines print to stdout uninvited.

    Spark in particular writes progress bars and log lines there, so the result is the
    last line that parses as a JSON object rather than the whole of stdout. Parsing all of
    stdout worked locally and failed the first time a Spark run emitted a stage line.
    """
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return None
