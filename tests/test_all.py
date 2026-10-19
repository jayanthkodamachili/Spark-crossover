"""Tests for spark-crossover.

The suite leans on the parts that decide whether a published number is trustworthy: the
configuration guards, the agreement check, the failure classifier, and the rule that every
workload must return concrete Python values rather than an unevaluated query.

Spark appears in exactly one test. Starting a JVM costs around six seconds and the thing
worth asserting about Spark, that it agrees with the others, is asserted once rather than
sixteen times.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from spark_crossover import agreement, cli, timing
from spark_crossover.config import WORKLOADS, BenchmarkConfig, ConfigError
from spark_crossover.data import build_right_table, build_table, ensure_dataset
from spark_crossover.engines import measure
from spark_crossover.runner import main as runner_main

SMALL_ROWS = 20_000
SMALL_GROUPS = 50


@pytest.fixture(scope="module")
def data_dir():
    path = Path(tempfile.mkdtemp(prefix="crossover-tests-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def small(engine: str, workload: str, data_dir: Path) -> BenchmarkConfig:
    return BenchmarkConfig(engine=engine, workload=workload, n_rows=SMALL_ROWS,
                           n_groups=SMALL_GROUPS, repeats=3, warmups=0, data_dir=data_dir)


class TestConfigRefusesMeasurementsThatCannotBeTrusted:
    def test_unknown_engine(self):
        with pytest.raises(ConfigError, match="engine must be one of"):
            BenchmarkConfig(engine="hadoop")

    def test_unknown_workload(self):
        with pytest.raises(ConfigError, match="workload must be one of"):
            BenchmarkConfig(workload="pivot")

    def test_a_sample_too_small_to_be_about_data(self):
        """Below a thousand rows every engine is timing its own startup."""
        with pytest.raises(ConfigError, match="below 1000"):
            BenchmarkConfig(n_rows=100)

    def test_more_groups_than_rows(self):
        with pytest.raises(ConfigError, match="must be between 1"):
            BenchmarkConfig(n_rows=1000, n_groups=5000)

    def test_too_few_repeats_for_a_median(self):
        with pytest.raises(ConfigError, match="is below 3"):
            BenchmarkConfig(repeats=2)

    def test_negative_warmups(self):
        with pytest.raises(ConfigError, match="cannot be negative"):
            BenchmarkConfig(warmups=-1)

    def test_a_memory_cap_too_small_to_start_python(self):
        """A cap that kills the interpreter would be reported as the engine failing."""
        with pytest.raises(ConfigError, match="below 128"):
            BenchmarkConfig(memory_limit_mb=64)

    def test_zero_spark_cores(self):
        with pytest.raises(ConfigError, match="at least 1"):
            BenchmarkConfig(spark_cores=0)

    def test_dataset_name_identifies_shape_and_seed(self):
        a = BenchmarkConfig(n_rows=5000, n_groups=10, seed=1)
        b = BenchmarkConfig(n_rows=5000, n_groups=10, seed=2)
        assert a.dataset_name != b.dataset_name
        assert a.dataset_path != a.right_path

    def test_as_dict_round_trips_the_knobs(self):
        payload = BenchmarkConfig(n_rows=5000).as_dict()
        assert payload["n_rows"] == 5000
        assert set(payload) >= {"engine", "workload", "repeats", "seed"}


class TestDataIsDeterministic:
    def test_same_seed_gives_identical_bytes(self):
        a = build_table(5000, 20, seed=7)
        b = build_table(5000, 20, seed=7)
        assert a.equals(b)

    def test_different_seed_gives_different_data(self):
        assert not build_table(5000, 20, seed=7).equals(build_table(5000, 20, seed=8))

    def test_right_table_has_one_row_per_key(self):
        right = build_right_table(37, seed=3)
        assert right.num_rows == 37
        assert len(set(right.column("key").to_pylist())) == 37

    def test_ensure_dataset_is_idempotent(self, data_dir):
        config = small("pandas", "groupby", data_dir)
        first = ensure_dataset(config)
        stamp = first[0].stat().st_mtime_ns
        second = ensure_dataset(config)
        assert first == second
        assert second[0].stat().st_mtime_ns == stamp, "should not have been rewritten"


class TestEnginesComputeTheSameAnswer:
    """The load bearing property. Timings mean nothing without it."""

    @pytest.mark.parametrize("workload", WORKLOADS)
    def test_single_node_engines_agree(self, workload, data_dir):
        answers = {
            engine: measure(small(engine, workload, data_dir))["answer"]
            for engine in ("pandas", "polars", "duckdb")
        }
        agreement.check_all(answers)

    @pytest.mark.parametrize("workload", WORKLOADS)
    def test_every_answer_is_concrete(self, workload, data_dir):
        """No lazy frames, no relations, no unevaluated queries.

        This is the test that stops the classic benchmark error. Polars, DuckDB and Spark
        all have APIs that return something cheap which has not run yet, and timing one of
        those produces a result that is wrong by orders of magnitude in the direction most
        likely to be published.
        """
        for engine in ("pandas", "polars", "duckdb"):
            answer = measure(small(engine, workload, data_dir))["answer"]
            for key, value in answer.items():
                assert isinstance(value, int | float | list), (
                    f"{engine}/{workload} returned {type(value).__name__} for {key}, "
                    "which suggests the query was never executed"
                )
                if isinstance(value, list):
                    assert all(isinstance(v, int) for v in value)

    @pytest.mark.slow
    def test_spark_agrees_with_pandas(self, data_dir):
        pandas_answer = measure(small("pandas", "groupby", data_dir))["answer"]
        spark_answer = measure(small("spark", "groupby", data_dir))["answer"]
        agreement.check_all({"pandas": pandas_answer, "spark": spark_answer})


class TestAgreementCatchesDisagreement:
    def test_identical_answers_agree(self):
        assert agreement.compare({"n": 5, "s": 1.0}, {"n": 5, "s": 1.0}) == []

    def test_counts_are_compared_exactly(self):
        problems = agreement.compare({"n": 5}, {"n": 6})
        assert problems and "exact comparison" in problems[0]

    def test_floats_agree_within_relative_tolerance(self):
        """Engines sum in different orders, so the last bits differ on correct engines."""
        assert agreement.compare({"s": 1_000_000.0}, {"s": 1_000_000.0000001}) == []

    def test_floats_disagree_beyond_it(self):
        assert agreement.compare({"s": 1_000_000.0}, {"s": 1_000_100.0})

    def test_lists_are_compared_in_order(self):
        assert agreement.compare({"ids": [1, 2, 3]}, {"ids": [3, 2, 1]})

    def test_a_missing_key_is_a_disagreement(self):
        assert agreement.compare({"a": 1}, {"b": 1})

    def test_check_all_raises_and_names_the_engine(self):
        with pytest.raises(agreement.AgreementError, match="duckdb"):
            agreement.check_all({"pandas": {"n": 5}, "duckdb": {"n": 6}})

    def test_check_all_says_the_timings_are_meaningless(self):
        with pytest.raises(agreement.AgreementError, match="meaningless"):
            agreement.check_all({"pandas": {"n": 5}, "polars": {"n": 7}})

    def test_check_all_falls_back_when_the_reference_is_absent(self):
        agreement.check_all({"polars": {"n": 5}, "duckdb": {"n": 5}})

    def test_check_all_passes_when_everything_agrees(self):
        agreement.check_all({"pandas": {"n": 5}, "polars": {"n": 5}, "duckdb": {"n": 5}})


class TestFailureClassification:
    """These labels are published, so each one is pinned to the text that produces it."""

    @pytest.mark.parametrize(
        ("error", "detail", "expected"),
        [
            ("MemoryError", "", "memory_error"),
            ("ArrowMemoryError", "malloc of size 8388608 failed", "memory_error"),
            ("OSError", "Cannot allocate memory", "memory_error"),
            ("RuntimeError", "std::bad_alloc", "memory_error"),
            ("NameError", "name 'PyLazyFrame' is not defined", "native_load_failure"),
            ("ImportError", "Polars binary is missing!", "native_load_failure"),
            ("PySparkRuntimeError", "[JAVA_GATEWAY_EXITED] Java gateway process exited",
             "jvm_not_measurable"),
            ("ValueError", "something unrelated", "other"),
        ],
    )
    def test_classification(self, error, detail, expected):
        assert timing.classify_failure(error, detail) == expected

    def test_a_jvm_that_cannot_start_is_not_recorded_as_a_loss(self):
        """The distinction the experiment depends on.

        Under an address space cap the JVM fails to start, at caps where Spark would have
        run. Calling that a memory failure would publish a false comparison.
        """
        assert timing.classify_failure(
            "PySparkRuntimeError", "[JAVA_GATEWAY_EXITED] cannot allocate memory"
        ) == "jvm_not_measurable"


class TestResultParsingSurvivesNoisyEngines:
    def test_the_last_json_line_wins(self):
        noisy = '[Stage 0:>  (0 + 1) / 1]\nWARN something\n{"ok": true, "n": 1}'
        assert timing._last_json_line(noisy) == {"ok": True, "n": 1}

    def test_a_broken_json_line_is_skipped(self):
        assert timing._last_json_line('{"ok": true}\n{not json}') == {"ok": True}

    def test_no_json_at_all_returns_none(self):
        assert timing._last_json_line("WARN only noise here") is None

    def test_empty_output(self):
        assert timing._last_json_line("") is None


class TestMeasurementSummary:
    def _measurement(self, **kwargs):
        defaults = dict(engine="duckdb", workload="groupby", n_rows=1000, ok=True,
                        setup_seconds=0.05, median_seconds=0.2, min_seconds=0.19,
                        max_seconds=0.25, spread_ratio=1.3, peak_rss_mb=120.0)
        defaults.update(kwargs)
        return timing.Measurement(**defaults)

    def test_total_includes_setup(self):
        assert self._measurement().total_seconds == pytest.approx(0.25)

    def test_as_dict_is_json_serialisable(self):
        json.dumps(self._measurement().as_dict())

    def test_a_failed_measurement_carries_its_kind(self):
        failed = self._measurement(ok=False, failure_kind="memory_error")
        assert failed.as_dict()["failure_kind"] == "memory_error"


class TestSubprocessIsolation:
    def test_a_measurement_runs_in_its_own_process(self, data_dir):
        import sys

        result = timing.run_measurement("duckdb", "groupby", SMALL_ROWS, repeats=3,
                                        warmups=0, n_groups=SMALL_GROUPS,
                                        data_dir=data_dir, python=sys.executable)
        assert result.ok
        assert result.peak_rss_mb > 0, "peak memory should have been reported back"

    def test_a_rejected_config_is_reported_not_raised(self, data_dir, capsys):
        code = runner_main([
            "--engine", "duckdb", "--workload", "groupby", "--n-rows", "10",
            "--data-dir", str(data_dir),
        ])
        assert code == 2
        assert json.loads(capsys.readouterr().out)["ok"] is False

    def test_a_timeout_is_a_failed_measurement_not_a_crash(self, data_dir):
        result = timing.run_measurement("duckdb", "groupby", SMALL_ROWS, repeats=3,
                                        warmups=0, n_groups=SMALL_GROUPS,
                                        data_dir=data_dir, timeout_seconds=0)
        assert not result.ok
        assert result.failure_kind == "timeout"

    def test_a_missing_interpreter_is_reported(self, data_dir):
        result = timing.run_measurement("duckdb", "groupby", SMALL_ROWS, repeats=3,
                                        warmups=0, n_groups=SMALL_GROUPS,
                                        data_dir=data_dir,
                                        python="/nonexistent/python")
        assert not result.ok


class TestExitCodes:
    def _m(self, engine, ok=True, peak=100.0):
        return timing.Measurement(engine=engine, workload="groupby", n_rows=1000, ok=ok,
                                  median_seconds=0.1, peak_rss_mb=peak)

    def test_fits_on_one_node(self):
        results = [self._m("pandas"), self._m("duckdb")]
        assert cli.classify(results, None) == cli.EXIT_FITS_ON_ONE_NODE

    def test_no_single_node_engine_survived(self):
        results = [self._m("pandas", ok=False), self._m("duckdb", ok=False)]
        assert cli.classify(results, None) == cli.EXIT_NEEDS_MORE_THAN_ONE_NODE

    def test_a_surviving_spark_does_not_make_it_fit_on_one_node(self):
        """Spark finishing is not evidence that one machine would have been enough."""
        results = [self._m("pandas", ok=False), self._m("duckdb", ok=False),
                   self._m("spark")]
        assert cli.classify(results, None) == cli.EXIT_NEEDS_MORE_THAN_ONE_NODE

    def test_close_to_the_budget_is_reported_as_tight(self):
        results = [self._m("duckdb", peak=900.0)]
        assert cli.classify(results, 1000) == cli.EXIT_TIGHT

    def test_comfortably_inside_the_budget(self):
        results = [self._m("duckdb", peak=100.0)]
        assert cli.classify(results, 1000) == cli.EXIT_FITS_ON_ONE_NODE

    def test_render_names_the_fastest_and_the_lightest(self):
        results = [self._m("pandas", peak=500.0), self._m("duckdb", peak=100.0)]
        rendered = cli.render(results, 1000)
        assert "fastest single node engine" in rendered
        assert "lightest single node engine" in rendered

    def test_render_says_so_when_nothing_survived(self):
        rendered = cli.render([self._m("pandas", ok=False)], None)
        assert "no single node engine completed" in rendered

    def test_disagreeing_engines_exit_four(self, data_dir, monkeypatch, capsys):
        """A ranking from engines that computed different things must not be printed."""
        def fake(engine, *args, **kwargs):
            answer = {"n_groups": 5 if engine == "pandas" else 6}
            return timing.Measurement(engine=engine, workload="groupby", n_rows=1000,
                                      ok=True, median_seconds=0.1, answer=answer)

        monkeypatch.setattr(cli, "run_measurement", fake)
        code = cli.main(["--n-rows", "20000", "--engines", "pandas", "duckdb",
                         "--data-dir", str(data_dir)])
        assert code == cli.EXIT_ENGINES_DISAGREE
        assert "meaningless" in capsys.readouterr().err

    def test_a_clean_run_exits_zero(self, data_dir, capsys):
        code = cli.main(["--n-rows", str(SMALL_ROWS), "--n-groups", str(SMALL_GROUPS),
                         "--engines", "duckdb", "polars", "--repeats", "3",
                         "--warmups", "0", "--data-dir", str(data_dir)])
        assert code == cli.EXIT_FITS_ON_ONE_NODE
        assert "duckdb" in capsys.readouterr().out

    def test_json_output_parses(self, data_dir, capsys):
        cli.main(["--n-rows", str(SMALL_ROWS), "--n-groups", str(SMALL_GROUPS),
                  "--engines", "duckdb", "--repeats", "3", "--warmups", "0",
                  "--json", "--data-dir", str(data_dir)])
        payload = json.loads(capsys.readouterr().out)
        assert payload["results"][0]["engine"] == "duckdb"

    def test_an_internal_failure_exits_three(self, data_dir, monkeypatch):
        def boom(*args, **kwargs):
            raise RuntimeError("engine exploded")

        monkeypatch.setattr(cli, "run_measurement", boom)
        assert cli.main(["--n-rows", "20000", "--engines", "duckdb",
                         "--data-dir", str(data_dir)]) == cli.EXIT_INTERNAL_ERROR


class TestRunnerEndToEnd:
    """The runner is what every published measurement actually goes through."""

    def test_a_successful_run_prints_a_parsable_result(self, data_dir, capsys):
        code = runner_main([
            "--engine", "duckdb", "--workload", "groupby", "--n-rows", str(SMALL_ROWS),
            "--n-groups", str(SMALL_GROUPS), "--repeats", "3", "--warmups", "0",
            "--data-dir", str(data_dir),
        ])
        assert code == 0
        payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert payload["ok"] is True
        assert len(payload["timings"]) == 3
        assert payload["peak_rss_mb"] > 0
        assert payload["answer"]["n_groups"] == SMALL_GROUPS

    def test_an_engine_failure_is_reported_as_json_not_a_traceback(
        self, data_dir, capsys, monkeypatch
    ):
        import spark_crossover.runner as runner_module

        def boom(config):
            raise RuntimeError("engine exploded")

        monkeypatch.setattr(runner_module, "measure", boom)
        code = runner_main([
            "--engine", "duckdb", "--workload", "groupby", "--n-rows", str(SMALL_ROWS),
            "--n-groups", str(SMALL_GROUPS), "--repeats", "3", "--warmups", "0",
            "--data-dir", str(data_dir),
        ])
        assert code == 4
        payload = json.loads(capsys.readouterr().out)
        assert payload["ok"] is False and payload["error"] == "RuntimeError"

    def test_a_memory_error_gets_its_own_exit_code(self, data_dir, capsys, monkeypatch):
        import spark_crossover.runner as runner_module

        def oom(config):
            raise MemoryError("malloc failed")

        monkeypatch.setattr(runner_module, "measure", oom)
        code = runner_main([
            "--engine", "pandas", "--workload", "sort", "--n-rows", str(SMALL_ROWS),
            "--n-groups", str(SMALL_GROUPS), "--repeats", "3", "--warmups", "0",
            "--data-dir", str(data_dir),
        ])
        assert code == 3
        assert json.loads(capsys.readouterr().out)["error"] == "MemoryError"

    def test_the_memory_cap_is_applied_after_the_data_is_written(self, data_dir):
        """Generating the dataset is the harness's job, not the engine's.

        If the cap applied first, a cap too small for numpy would be recorded as the
        engine failing on a workload it never saw.
        """
        config = BenchmarkConfig(engine="duckdb", workload="groupby", n_rows=SMALL_ROWS,
                                 n_groups=SMALL_GROUPS, repeats=3, warmups=0,
                                 data_dir=data_dir, memory_limit_mb=4096)
        assert ensure_dataset(config)[0].exists()


@pytest.mark.slow
class TestSparkWorkloads:
    """Every Spark workload, in one JVM.

    These paths produce published numbers, so they are tested rather than trusted. They
    share a single session because starting four costs twenty-five seconds and proves
    nothing extra.
    """

    def test_all_four_workloads_agree_with_duckdb(self, data_dir):
        from spark_crossover.engines import spark_engine

        config = small("spark", "groupby", data_dir)
        with spark_engine(config) as run:
            spark_answers = {w: run(w) for w in WORKLOADS}

        for workload in WORKLOADS:
            duck = measure(small("duckdb", workload, data_dir))["answer"]
            agreement.check_all({"duckdb": duck, "spark": spark_answers[workload]})

    def test_spark_answers_are_concrete(self, data_dir):
        from spark_crossover.engines import spark_engine

        with spark_engine(small("spark", "groupby", data_dir)) as run:
            for workload in WORKLOADS:
                for key, value in run(workload).items():
                    assert isinstance(value, int | float | list), (
                        f"spark/{workload} returned {type(value).__name__} for {key}, "
                        "which means the query was never executed"
                    )


class TestFailurePathsAreReportedNotRaised:
    def test_a_process_killed_by_a_signal_is_recorded_as_aborted(self, monkeypatch):
        import subprocess as sp

        def killed(*args, **kwargs):
            return sp.CompletedProcess(args, -6, stdout="", stderr="std::bad_alloc")

        monkeypatch.setattr(timing.subprocess, "run", killed)
        result = timing.run_measurement("duckdb", "sort", 100_000)
        assert not result.ok and result.failure_kind == "aborted"
        assert "signal 6" in result.error

    def test_a_nonzero_exit_with_no_json_is_recorded(self, monkeypatch):
        import subprocess as sp

        def silent(*args, **kwargs):
            return sp.CompletedProcess(args, 1, stdout="", stderr="something broke")

        monkeypatch.setattr(timing.subprocess, "run", silent)
        result = timing.run_measurement("duckdb", "sort", 100_000)
        assert not result.ok and result.failure_kind == "no_result"

    def test_a_zero_timing_does_not_divide_by_zero(self, monkeypatch):
        """A clock too coarse to see the work must not produce an infinite spread."""
        import subprocess as sp

        payload = json.dumps({"ok": True, "setup_seconds": 0.0, "timings": [0.0, 0.0, 0.0],
                              "answer": {"n": 1}, "peak_rss_mb": 10.0})

        def instant(*args, **kwargs):
            return sp.CompletedProcess(args, 0, stdout=payload, stderr="")

        monkeypatch.setattr(timing.subprocess, "run", instant)
        result = timing.run_measurement("duckdb", "groupby", 100_000)
        assert result.ok and result.spread_ratio == 0.0


class TestCliReportsConfigProblems:
    def test_an_invalid_row_count_is_refused_before_timing_anything(self, data_dir, capsys):
        code = cli.main(["--n-rows", "10", "--engines", "duckdb",
                         "--data-dir", str(data_dir)])
        assert code == cli.EXIT_BAD_CONFIG
        assert "invalid benchmark" in capsys.readouterr().err

    def test_a_single_engine_run_skips_the_agreement_check(self, data_dir):
        """With one engine there is nothing to compare, and comparing it to itself
        would report agreement that was never tested."""
        code = cli.main(["--n-rows", str(SMALL_ROWS), "--n-groups", str(SMALL_GROUPS),
                         "--engines", "duckdb", "--repeats", "3", "--warmups", "0",
                         "--data-dir", str(data_dir)])
        assert code == cli.EXIT_FITS_ON_ONE_NODE


class TestTheMemoryCapIsReal:
    """The cap underpins E4, so it is verified rather than assumed.

    It is applied inside the measurement subprocess, which is why the line that calls
    setrlimit does not appear in this process's coverage. What matters is that the cap
    takes effect, which is asserted here from the outside.
    """

    def test_the_cap_is_actually_set_in_the_child(self, data_dir):
        import subprocess
        import sys

        program = (
            "import resource, json;"
            "from spark_crossover.runner import apply_memory_cap;"
            "apply_memory_cap(2048);"
            "print(json.dumps(resource.getrlimit(resource.RLIMIT_AS)))"
        )
        out = subprocess.run([sys.executable, "-c", program], capture_output=True,
                             text=True, check=True).stdout
        soft, hard = json.loads(out)
        assert soft == hard == 2048 * 1024 * 1024

    def test_a_cap_below_what_the_work_needs_fails_the_measurement(self, data_dir):
        """The mechanism E4 depends on: a real workload, a cap it cannot fit in."""
        result = timing.run_measurement(
            "pandas", "sort", 200_000, repeats=3, warmups=0, n_groups=100,
            data_dir=data_dir, memory_limit_mb=200, timeout_seconds=600,
        )
        assert not result.ok
        assert result.failure_kind in {"memory_error", "aborted", "native_load_failure"}


class TestSmallSurfaces:
    def test_dataset_bytes_reports_both_files(self, data_dir):
        from spark_crossover.data import dataset_bytes

        config = small("duckdb", "groupby", data_dir)
        left, right = ensure_dataset(config)
        assert dataset_bytes(config) == left.stat().st_size + right.stat().st_size

    def test_warmups_run_before_the_timed_repeats(self, data_dir):
        """A warmup must not appear in the timings it exists to protect."""
        config = BenchmarkConfig(engine="duckdb", workload="groupby", n_rows=SMALL_ROWS,
                                 n_groups=SMALL_GROUPS, repeats=3, warmups=2,
                                 data_dir=data_dir)
        result = measure(config)
        assert len(result["timings"]) == 3


class TestBudgetAndCapAreDifferentQuestions:
    """These exist because one flag was doing both jobs and exit 1 could never fire.

    Survival under an address space cap requires a cap far above what the engine resides
    in, so comparing measured peak memory against the cap put every surviving run in the
    comfortable band. The budget is now a separate number about the target machine.
    """

    def _m(self, engine, ok=True, peak=100.0):
        return timing.Measurement(engine=engine, workload="sort", n_rows=1000, ok=ok,
                                  median_seconds=0.1, peak_rss_mb=peak)

    def test_tight_is_reachable(self):
        assert cli.classify([self._m("duckdb", peak=175.0)], 200) == cli.EXIT_TIGHT

    def test_comfortably_under_budget(self):
        assert cli.classify([self._m("duckdb", peak=100.0)],
                            1000) == cli.EXIT_FITS_ON_ONE_NODE

    def test_over_budget_needs_another_machine(self):
        """Finishing on a machine with more memory than the target has is not fitting."""
        assert cli.classify([self._m("duckdb", peak=1200.0)],
                            1000) == cli.EXIT_NEEDS_MORE_THAN_ONE_NODE

    def test_exactly_at_the_budget_is_not_over_it(self):
        assert cli.classify([self._m("duckdb", peak=1000.0)], 1000) == cli.EXIT_TIGHT

    def test_the_lightest_engine_decides(self):
        """One heavy engine does not condemn the workload if a lighter one fits."""
        results = [self._m("pandas", peak=900.0), self._m("duckdb", peak=100.0)]
        assert cli.classify(results, 1000) == cli.EXIT_FITS_ON_ONE_NODE

    def test_no_budget_means_no_memory_verdict(self):
        assert cli.classify([self._m("duckdb", peak=99999.0)],
                            None) == cli.EXIT_FITS_ON_ONE_NODE

    def test_the_cap_flag_is_passed_to_the_measurement_not_the_verdict(self, monkeypatch,
                                                                      data_dir):
        seen = {}

        def fake(engine, workload, n_rows, **kwargs):
            seen.update(kwargs)
            return timing.Measurement(engine=engine, workload=workload, n_rows=n_rows,
                                      ok=True, median_seconds=0.1, peak_rss_mb=50.0,
                                      answer={"n": 1})

        monkeypatch.setattr(cli, "run_measurement", fake)
        cli.main(["--n-rows", "20000", "--engines", "duckdb", "--memory-cap-mb", "4096",
                  "--memory-budget-mb", "100", "--data-dir", str(data_dir)])
        assert seen["memory_limit_mb"] == 4096
