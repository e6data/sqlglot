# Large-query performance follow-up

Requirement: profile large queries and known regressions, optimize measured CPU
work while preserving existing SUBQUERY_TREE outcomes, and compare the full corpus.
Baseline: da8a92d74, Python tokenizer, serial converter calls, no HTTP/log I/O.
Workflow: host-native Vibe; demonstrate an allocation/work regression before code edits.

Plan:
1. Measure discovery, tokenization, parser attempts, cleanup, transforms and generation.
2. Implement the strongest bounded optimization supported by the measurements.
3. Run regression tests and full-corpus before/after timing and outcome comparison.

Initial cProfile: 11 evenly spaced queries from the >=100k bucket (3,184,255
characters). 27.096 seconds with profiler overhead; structural tree discovery
0.837 seconds, tokenization 6.567 seconds, region parse including tokenization
12.391 seconds. Mask/splice negligible. These overlapping cumulative times are
diagnostic, not production latency or additive stage totals.

## Findings and experiment decisions

- The >=100k bucket has zero successful vanilla conversions (0/107). The
  20k–100k bucket has 15/295. Their vanilla latency is not an equivalent-work
  lower bound. The original report now shows outcomes alongside size buckets.
- In the 11-query stage sample, each query has one large PG wrapper and one
  small inner region. Discovery tokens are reused; failed PG attempts on four
  small inner regions are negligible. This is not repeated full-query parsing.
- A Python tokenizer punctuation fast path passed failure-first regression and
  tokenization tests, but the full-corpus comparison showed no overall gain:
  80.42 ms baseline, 80.73 ms candidate. All 1,628 outcomes matched. Reverted the
  production change and retained the experimental patch as evidence.
- Instrumented GC consumed 1,502 ms of 8,661 ms across the large-query stage
  sample (17.3%, overlapping parser/tokenizer/generator times). One previously
  regressing input spent 113 of 168 ms collecting garbage in this run. This
  establishes a current source of variance, not every historical regression's cause.
- Benchmark-only GC threshold experiment, 11 large queries x 5 passes:
  threshold0=700: 778.50 ms mean, 250.6 MiB peak RSS;
  7,000: 732.81 ms, 408.2 MiB;
  70,000: 699.79 ms, 602.5 MiB.
  Other thresholds remain (10, 10). Production GC behavior is unchanged.

Completed full-corpus threshold=7,000 comparison: 80.42 -> 78.32 ms mean (2.6%
lower), >=100k 806.59 -> 793.51 ms (1.6% lower), peak RSS 403.7 MiB. All 1,628
outcomes match baseline. This single-pass result is exploratory, and does not
justify changing production GC defaults. No production change is adopted.

Verification: 5,049 recorded attempts across three full runs and three repeated
large-query samples; recomputed medians, stable outcomes, and full baseline hash
equivalence. Regenerated the original four-way report with success counts by size;
its timings and per-query CSV remain unchanged. Final findings and reproduction
commands are in `large_query_report.md`, with compressed evidence and manifests
under `results/large-query-2026-10-14/`.

## Native tokenizer follow-up

Approved pass: fix native Databricks escape parity, benchmark with the optimized
branch, and investigate remaining Python parser allocations. Preserve mixed-dialect
architecture and GC defaults. Use Vibe and clean-slice-rust; no default backend switch.

Regression demonstrated: native 0.6.1 retains unknown backslashes that Python drops
in Databricks (four failing subcases). Added dialect-injected configuration and a
private Rust predicate, then rebuilt 0.6.2 offline. Regression and token tests pass.
The new Python code requires the matching native wheel when Rust is selected.

Plan: run dialect/parser tests, full corpus Python/Rust comparisons including
vanilla to exercise the formerly differing escape cases, and inspect live versus
discarded parser-node allocations before selecting further changes.

Fresh allocation check: representative 333,705-character PG query creates 50,173
AST nodes, retaining 50,157 (only 16 discarded). This does not count all Python
object/dictionary allocations, but leaves little opportunity in discarded AST
nodes after the previous branch optimizations. No additional parser change chosen.

Native targeted suite: 196 tests run, two expected Python-allocation tests skipped.
Python targeted suite: 195 tests pass. Release build succeeds offline. Clippy with
the helper's targeted deny attributes passes with five existing warnings elsewhere;
crate-wide `-D warnings` remains blocked by those existing warnings.

Full benchmark complete: same branch, Python and repaired Rust, three full
passes each; one full vanilla pass per backend additionally checks escape parity.
All 13,024 attempts validated, zero backend outcome/hash differences in either
mode. Mean 77.74 -> 66.46 ms (14.5% reduction); >=100k 785.94 -> 673.99 ms (14.2%).
Peak process RSS 293.6 -> 346.4 MiB. GC defaults remain unchanged. Rust use still
requires the rebuilt extension; no service configuration has been changed.
Final native suite: 197 tests run, two expected skips. Cargo test target builds
successfully but contains zero Rust unit tests; behavior is covered by Python
integration tests and full-corpus comparisons. See `native_followup_report.md`.
