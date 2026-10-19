# ADR-004: Cap address space with RLIMIT_AS, and publish what that distorts

Status: accepted

## Context

E4 needs to ask which engines survive a hard memory limit. The options were a cgroup, a
container memory limit, or `resource.setrlimit(RLIMIT_AS)`.

## Decision

`RLIMIT_AS`, applied in the measurement subprocess after the dataset is written.

## Why

It needs no privileges, works identically on any Linux, fails at a number chosen in advance,
and applies to exactly one measurement. A cgroup would need root or delegation; a container
limit would apply to the whole run rather than one cell of a grid.

The cap is applied after the data is generated, because generating it is the harness's job.
A cap too small for numpy would otherwise record a memory failure for an engine that never
ran.

## What it distorts, measured rather than asserted

`RLIMIT_AS` limits virtual address space, not resident memory, and the two are different
numbers. An engine that reserves address space generously fails a cap it would never have
needed.

This is not hypothetical here:

- Polars aborts under a 3000 MB cap on a sort whose peak resident memory is around 1068 MB,
  while pandas completes the same work under the same cap using a comparable amount.
- Spark cannot be measured this way at all. The JVM reserves far more address space than it
  uses, so the gateway dies before any work starts, at caps Spark would have run under
  comfortably.

## Consequence

Peak resident memory is reported beside cap survival, because the two answer different
questions and disagree. The failure classifier has a dedicated `jvm_not_measurable` label,
and E4 excludes those cells from the survival comparison rather than scoring them as
losses. Recording "Spark failed" there would have been a false result produced by a true
measurement, which is the most convincing kind of wrong.

A reader who wants the resident memory question answered properly should rerun under a
cgroup limit. The harness would need one new function and nothing else.
