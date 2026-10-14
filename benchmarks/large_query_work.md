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

## Native scanner and token reuse pass

User authorized continued optimization, explicitly excluding caching. Baseline is
066f8569a and repaired native wheel 0.6.2. Preserve dialect selection and GC defaults.
Plan: demonstrate and eliminate scanner lookahead allocations, test bulk native
token relocation without copying token objects, and retain only measured wins.
Validate all 1,628 inputs, repeat latency measurements, and record peak memory.
Workflow: Vibe and clean-slice-rust, with request-owned state and no global caches.

Failure-first evidence: long Unicode string scanner produced 7,001 allocating
lookaheads before the change, zero after (Rust regression passes). Native region
reuse tests first showed two PG retokenizations and copied tokens; both pass with
bulk relocation. Focused token/reuse/escape suite: 23 tests, one expected skip.
Baseline 0.6.2 full-corpus three-pass run completed. Candidate 0.6.3 three-pass run
in progress; scanner-only attribution will use the baseline Python snapshot with
0.6.3. No concurrent compilation or test runs during timed measurements.

Completed: all three configurations ran three passes over all 1,628 queries.
Validated 14,652 attempts: identical 1,610 successful output hashes and 18 error
hashes across configurations. Baseline mean 70.47 ms; scanner-only 66.74 ms;
scanner + native reuse 63.95 ms (9.3% combined reduction). >=100k mean
707.14 -> 654.60 ms (7.4%). Peak RSS 353.6 -> 322.3 MiB. Both changes retained.
Code commit: c2ecee77c; native version 0.6.3. No caching or GC/service changes.

Verification: 215 tests on each backend, native one expected skip, Python four.
Rust allocation regression passes; release build succeeds. Clippy passes with
five existing unrelated warnings. Full evidence and per-query CSV are linked
from native_allocation_report.md. Changes committed locally, not pushed.

## Parser and allocation follow-up

User approved a focused pass on current native branch 490223fda, keeping caching
excluded. Refresh stage and function/allocation profiles first; target measured
parser/AST bookkeeping or redundant transforms. Demonstrate a failing work or
behavior regression before implementation, then run output parity and full-corpus
repeated benchmarks for any retained change. If necessary Python object creation
dominates, document a bounded native-parser prototype rather than assuming gains.
Workflow: continue Vibe task note; preserve unrelated outputs/.

Refreshed native stage profile: PG parsing 57.7%, generation 16.1%, transforms
7.6%. cProfile records 4.72m token-set checks and 4.65m enum-hash calls across 11
large inputs. Chosen experiments: TokenType identity hashing and collecting tables
during the existing transform walk. No new caches or parser/validation changes.

Failure-first tests reproduced 92 Python enum-hash calls for a small query and
38 child traversals for 19 nodes. Both now pass (zero hash calls; 19 traversals).
Targeted suite: 219 tests each backend, native one skip, Python four. All 567
dialect tests pass across the initial run and an environment-only retry of
lazy loading (subprocess needed the venv python on PATH).
Instrumented sample PG parse 3901 -> 3652 ms, transforms 516 -> 329 ms.
Full three-pass candidate benchmark is running against a fresh three-pass baseline.

Full corpus complete: 9,768 attempts validated, identical 1,610 SQL hashes and
18 error hashes. Mean latency 62.45 -> 58.75 ms (5.9%); >=100k 641.31 -> 602.61 ms
(6.0%). Peak RSS 412.1 -> 365.4 MiB; allocator/GC variability applies. Both small
changes retained. This pass does not implement a native parser; dispatch and
traversal still offered a measurable gain without grammar/AST validation changes.
See parser_followup_report.md and its raw evidence links. No new native build.

Code committed as 7bb20c6ad on codex/experiment-dialect-regions. Benchmarked
production source hashes verified against the commit; all 16 before/after stage
profile outcomes also match. Benchmark report and evidence committed separately.
This pass remains local; not pushed.
