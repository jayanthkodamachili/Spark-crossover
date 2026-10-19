# ADR-003: `make verify` does not rerun the benchmarks

Status: accepted

## Context

Every other check in this repository is deterministic. The test suite either passes or does
not. The number checker compares prose against recorded figures and fails on a mismatch.

Timings are not like that. Across separate launches of the same measurement, the figures in
this repository move by up to a quarter, and a ratio built from two noisy quantities moves
considerably more. Rerunning one experiment changed a published derived ratio from 4.4 to
2.3 while nothing about the code had changed.

## Decision

Two targets, and `verify` is not the one that measures.

- `make verify` runs lint, the test suite, and the number check against the recorded run.
- `make bench` reruns the four experiments and rewrites `docs/experiments/`.

The timings in `docs/metrics.json` are read from the recorded run. The suite figures in the
same file are re-measured on every run, because they are deterministic and a drift in them
is a real regression.

## Why

A check that re-measures a non-deterministic quantity and demands it match the published
figure fails at random. A check that fails at random is a check everybody learns to ignore,
and then it is not protecting the deterministic figures either. The failure mode is not a
noisy build; it is a disbelieved one.

Recording the run also makes the benchmark reviewable. Because `docs/experiments/` is in
version control, rerunning the benchmarks produces a diff showing exactly which published
figures moved and by how much, which is more informative than a green tick.

## Consequence

The published timings belong to one machine on one day, and the README says so in the first
paragraph rather than the last. A reader who wants numbers for their own hardware runs
`make bench`, which is the intended use: the repository ships a harness, not a leaderboard.

The risk accepted is that a change to the engine code could alter the timings without
anything failing. The agreement check is what covers the serious version of that: if a
change makes an engine compute something different, the run fails regardless of timing.
