# ADR-005: What this repository cannot measure

Status: accepted

## Context

Every timing here comes from one machine with two cores, running a Spark session in local
mode. A reader is entitled to know exactly how far that generalises, and the honest answer
is: less far than the repository's name suggests.

## Decision

State the limits in the README, not in a footnote, and keep the full list here.

## The limits

**A local session is not a cluster.** `local[2]` runs the whole engine in one JVM on one
machine. There is no network shuffle, no distributed storage, no straggler, no executor
loss and no recovery from one. Those are the costs a cluster adds and the capabilities it
buys, and none of them appear in any number here. A real cluster would make Spark slower
per row at these sizes and able to reach sizes no single machine can.

**Two cores.** Spark's design assumes parallelism it does not have here, and so does
DuckDB's, and the comparison is between two engines that are both constrained. A machine
with thirty-two cores would move every number in this repository and could move the
ranking. The harness is shipped so that question can be answered on the machine it matters
on, which is the reader's, not mine.

**One data distribution.** Uniform integer keys, normal values, no nulls, no skew, no
strings and no high cardinality join. Skew is precisely where distributed engines earn
their keep and where a naive single node hash aggregate degrades, so its absence favours
the single node engines in a way that is worth naming.

**Four workloads, chosen before the results.** Grouped aggregation, a join against a small
dimension, a full sort and a distinct over a derived key. They were picked to span
different pressure on an engine and were not changed once the numbers came in. They are not
a representative sample of anything.

**The memory cap measures address space, not use.** `RLIMIT_AS` was chosen because it needs
no privileges and fails deterministically at a known number. It counts reserved address
space, which is why polars fails a cap it does not need and why Spark cannot be measured
this way at all. A cgroup limit would measure resident memory and would give different, and
for Polars and Spark fairer, answers. Peak resident memory is reported alongside precisely
because the two disagree.

**The extrapolated crossover is not a measurement.** Fitting four points and solving for
break even produces a number in the billions of rows. It is reported to show the size of
the gap, with that warning attached in the experiment and in the README, and it should not
be quoted as a finding.

**Benchmarks on a shared machine are noisy.** Repeated launches of the same measurement
vary by up to a quarter, and a ratio built from two noisy quantities varies considerably
more. Nothing here should be read to more precision than that.

## Consequence

The claims this repository actually supports are narrow: a distributed engine carries a
fixed startup cost that is large and measurable; on one machine that cost is not repaid at
any size the machine can hold; the memory wall usually cited as the reason to move is real
and can be cleared by a better single node engine; and the engines fail under memory
pressure in different ways, only one of which your code can catch.

Anything broader than that needs a cluster and a workload from the reader's own system,
which is what the harness is for.
