.PHONY: install lint test bench figures receipts verify clean

# Every target runs through one interpreter. A bare `pytest` picks whichever console script
# is first on PATH, which can belong to a different Python than the one holding the
# editable install, and the resulting error looks like a missing package rather than a
# wrong interpreter. Benchmarks make this worse, not better: measurements are spawned with
# sys.executable, so the interpreter running make decides what is being measured.
PYTHON ?= python3

install:
	$(PYTHON) -m pip install --upgrade "setuptools<81" wheel
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest --cov=spark_crossover --cov-report=term-missing --cov-fail-under=99

# Fast suite for iteration: skips the tests that start a JVM.
test-fast:
	$(PYTHON) -m pytest -m "not slow"

# Rerun the measurements and rewrite docs/experiments. Not part of verify: timings are not
# reproducible, and a check that demands they match fails at random. See ADR-003.
bench:
	$(PYTHON) experiments/e1_fixed_cost.py
	$(PYTHON) experiments/e2_size_crossover.py
	$(PYTHON) experiments/e3_by_operation.py
	$(PYTHON) experiments/e4_memory_cliff.py

figures:
	$(PYTHON) tools/render_diagram.py
	$(PYTHON) tools/capture_cli.py

receipts:
	$(PYTHON) tools/collect_metrics.py
	$(PYTHON) tools/check_numbers.py

verify: lint test receipts

clean:
	rm -rf .pytest_cache .ruff_cache htmlcov coverage.json junit.xml .coverage \
	       spark-warehouse metastore_db derby.log
