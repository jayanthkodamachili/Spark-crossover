# ADR-001: Pin setuptools below 81 and build in a clean virtual environment

Status: accepted, after an afternoon

## Context

Installing PySpark failed with:

```
AttributeError: install_layout. Did you mean: 'install_platlib'?
ERROR: Failed building wheel for pyspark
```

The error names neither Spark nor setuptools, and the 450 MB source download between each
attempt makes guessing expensive.

## What was actually wrong

Two things at once, which is why single fixes kept half working.

PySpark still ships a legacy `setup.py`. `install_layout` is an attribute that Debian and
Ubuntu patch into their distribution copy of setuptools, and that recent upstream
setuptools removed. So the build needs a setuptools old enough to have the attribute and
unpatched enough to behave predictably.

Attempts that failed and what they taught:

- `pip install pyspark`: the original failure, under build isolation with a modern
  setuptools.
- `pip install "setuptools<81"` then `--no-build-isolation`: still failed. The system
  interpreter's setuptools was already 68, old enough, and still the Debian patched copy.
  Version was never the whole problem.
- `SETUPTOOLS_USE_DISTUTILS=stdlib`: a different failure, `setuptools is not available in
  the build environment`. Swapping the distutils implementation made setuptools invisible
  to the build.

## Decision

Build in a virtual environment, with setuptools pinned below 81 from PyPI rather than from
the distribution:

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade "setuptools<81" wheel
.venv/bin/pip install -e ".[dev]"
```

`pyproject.toml` pins `setuptools>=68,<81` in its build requirements so the constraint
travels with the repository.

## Why this is in version control

Because the error message is useless and the fix is not guessable from it. Somebody, very
possibly me in a year, will hit this and search for `install_layout`. This document is what
I want them to find: not just the command that works, but the three that do not and the
reason each one fails short of the real cause.

## Consequence

A reader must create a virtual environment rather than install into the system
interpreter. For a benchmark harness that spawns subprocesses with `sys.executable` this is
the right default anyway, since every measurement then runs under the same interpreter that
holds the dependencies.
