# Parser and AST traversal follow-up — 14 October 2026

## Scope

Baseline: `490223fda`; candidate: `7bb20c6ad`, both using native tokenizer 0.6.3. No query caching, GC tuning,
service configuration changes or dialect-selection changes. Reuse the existing
region architecture and required AST validation.

## Refreshed profile

Stage instrumentation on 11 evenly spaced inputs of at least 100k characters:
6,756.46 ms total. Inclusive stages; GC overlaps the other stages. This sample
is for attribution, not a substitute for uninstrumented full-corpus latency.

| Stage | Total ms | Share |
|---|---:|---:|
| PG parsing | 3901.15 | 57.7% |
| SQL generation | 1088.94 | 16.1% |
| Generic native tokenization | 560.89 | 8.3% |
| AST transforms | 516.44 | 7.6% |
| Unicode-space normalization | 272.38 | 4.0% |
| Structural discovery | 247.25 | 3.7% |
| Native token reuse preparation | 78.77 | 1.2% |
| DBR parsing | 3.56 | 0.1% |
| GC, overlapping | 889.48 | 13.2% |

A separate cProfile run over the same 11 large inputs recorded 80.7 million calls
and 21.60 seconds including startup and instrumentation overhead. It recorded
4.72 million `_match_set` calls and 4.65 million `Enum.__hash__` calls. Expression
initialization ran 546,440 times. Its 0.495 seconds of self time is not a complete
allocation-cost measurement: child calls and GC account for additional work.
These results support testing dispatch/traversal improvements before deciding to
rewrite the parser or the AST representation.

## Experiments

1. Token enums use identity equality. Test using the matching built-in object
   hash directly, avoiding Python `Enum.__hash__` and name-hash calls at each
   lookup. Keep enum names, values, comparisons, serialization and token indexes.
   This does not memoize query results or add a cache.
2. Collect table nodes during the existing transform walk, then rename only those
   nodes after all aliases are known. Avoid a second traversal of every AST node.
   Keep existing alias-selection behavior, qualified-table handling, comment
   sanitization and VALUES-CTE rewriting.

## Full-corpus results

All 1,628 queries, including failures; three passes per configuration. Each query's
median latency is aggregated below. Fresh baseline and candidate processes ran
sequentially on the same machine with Python 3.11.6 and native wheel 0.6.3.
GC thresholds stayed 700/10/10. No concurrent agent tests/builds during timed runs;
no CPU affinity or thermal control. These are local conversion timings, not HTTP
latency or production throughput. Use this fresh paired baseline rather than
comparing absolute timings across reports.

| Characters | Queries | Before ms | After ms | Reduction |
|---|---:|---:|---:|---:|
| All | 1628 | 62.45 | 58.75 | 5.9% |
| <5k | 1092 | 2.49 | 2.40 | 3.4% |
| 5k–20k | 134 | 11.32 | 11.05 | 2.3% |
| 20k–100k | 295 | 97.69 | 91.71 | 6.1% |
| ≥100k | 107 | 641.31 | 602.61 | 6.0% |

Both changes are retained. Mean CPU time fell from 62.40 to 58.71 ms. Peak process
RSS was 412.1 MiB before and 365.4 MiB after; it includes imports, corpus loading,
and all passes and is subject to allocator/GC variability.

All **9,768 timed attempts** validated: identical **1,610 returned-SQL hashes and
18 error type/detail hashes** in every configuration and pass. This is output
regression evidence, not proof of database-level semantic equivalence.

## Verification

Failure-first work regressions reproduced 92 Python enum-hash calls on a small
query and 38 child traversals for 19 AST nodes. After the changes these became
zero and 19 respectively. Behavioral checks cover all token enum members' lookup,
identity and serialization; CTE renaming; qualified tables; comment sanitization;
VALUES-CTE wrapping and parent links.

Targeted suites ran 219 tests per backend (native: one expected skip; Python:
four). The all-dialect native suite ran 567 tests: 566 passed initially, and the
lazy-load subprocess test passed when rerun with the venv's `python` on PATH.
No Rust rebuild or native version change is required for this Python-only pass.

The after-change instrumented sample measured PG parsing at 3652.20 ms versus
3901.15 ms, and AST transforms at 328.74 ms versus 516.44 ms. Stage timings are
inclusive and noisy; the uninstrumented full-corpus table is the performance
result. The combined benchmark does not isolate each change's end-to-end gain.

## Architectural conclusion

Parsing remains the largest cost, but dispatch overhead and avoidable traversal
still yielded a measured improvement without changing grammar or AST validation.
This pass did not implement a native parser. A native expression-parser prototype
remains a larger option if the remaining latency misses the deployment target;
its evaluation must include Python/native AST conversion costs and whole-query
output parity, not just a parser microbenchmark.

## Reproduce and evidence

```sh
python benchmarks/large_query_profile.py --root . --tokenizer rust --output /tmp/stages.json
python benchmarks/four_way.py --root <checkout> --commit <revision> --variant our_branch \
  --tokenizer rust --repeats 3 --output /tmp/parser-next-<baseline-or-candidate>.json
python benchmarks/parser_followup_summary.py /tmp benchmarks
```

[Per-query CSV](results/parser-followup-2026-10-14/per_query.csv),
[summary](results/parser-followup-2026-10-14/summary.json),
[raw timing manifest](results/parser-followup-2026-10-14/manifest.json),
[profile and test manifest](results/parser-followup-2026-10-14/profile_manifest.json),
[function profile](results/parser-followup-2026-10-14/profile_top40.txt),
[provenance](results/parser-followup-2026-10-14/provenance.json).

