# ADR-002: One process per measurement

Status: accepted

## Context

A benchmark that runs four engines in one interpreter is cheap to write and quietly wrong.

## Decision

Every measurement runs in a fresh subprocess, launched by `timing.run_measurement` and
executed by `spark_crossover.runner`, which prints one JSON line and exits.

## Why

**Contamination.** A JVM already warm from an earlier workload, an engine still holding
memory the next one then cannot get, import side effects, a page cache already full of the
file the next engine is about to read. Each of these moves a timing in a direction nobody
can see from the results.

**Containment.** A measurement that dies under a memory cap, or aborts in native code,
ends one process rather than the experiment. E4 depends on this entirely: it is a grid of
deliberate failures, and in a single process the first one would end the run.

**Honest attribution.** Peak resident memory per engine only means something when the
process contains one engine.

## Costs, accepted

Process startup is included in every measurement, which is why setup time is measured and
reported separately rather than hidden inside the repeats.

Coverage cannot follow into the child, so the three lines that apply the memory cap are
uncovered in the parent's report. They are verified from outside instead: one test asserts
the limit is really set, another runs a real workload under a cap too small for it. A
pragma would have produced a rounder number and a weaker guarantee.

Engines write to stdout uninvited, so the result is the last line that parses as a JSON
object rather than the whole of stdout. Parsing all of stdout worked locally and failed the
first time Spark emitted a stage progress line.
