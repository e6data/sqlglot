# Large-query profiling and experiments — 14 October 2026

## Conclusion

The current large-query cost is primarily successful parsing and tokenization of the PostgreSQL wrapper. The sampled queries are already avoiding duplicate wrapper tokenization. Replacing the structural discovery AST or string stitching cannot remove most of this cost.

**Vanilla failed on all 107 inputs of at least 100k characters.** Its historical 214.30 ms mean is failed-attempt latency, not a completed-conversion baseline for our 794.24 ms. In the 20k–100k bucket, vanilla succeeds on only 15/295 inputs. Every input remains included in the reports.

No production changes were retained from this follow-up. The punctuation experiment was reverted after failing to improve full-corpus latency. GC thresholds are benchmark-only options; their memory trade-off and variable benefit do not justify a new default.

## Actual architecture

```text
Generic tokenize whole SQL
  -> LiteParser structural AST (skips most value expressions)
  -> Separate subquery tree with source text and offsets
  -> For each region: mask children with placeholders
       -> cleanup SQL; reuse tokens if compatible and unchanged
       -> PG parse; DBR tokenize + parse on failure
       -> e6 AST transforms
       -> generate a separate e6 SQL string
  -> recursively splice child SQL into parent placeholders
```

There is no single combined AST or single final generation. Combining ASTs would require preserving each region’s source-dialect generation semantics.

## Stage profile

11 evenly spaced inputs from the >=100k bucket, selected by `large_query_profile.py`; 3,184,255 total characters. Timings use lightweight wrappers and include their overhead. Stages are inclusive; GC overlaps other stages, and discovery includes LiteParser. The five earlier regressions are also captured in the raw evidence.

| Stage | Total ms across 11 queries | Share of instrumented wall time |
|---|---:|---:|
| Generic tokenization | 2415.43 | 27.9% |
| Structural discovery | 226.01 | 2.6% |
| Token reuse preparation | 279.12 | 3.2% |
| Cleanup | 312.75 | 3.6% |
| PG parsing | 3724.55 | 43.0% |
| DBR tokenization and parsing | 5.03 | 0.1% |
| AST transforms | 512.56 | 5.9% |
| SQL generation | 1139.46 | 13.2% |
| Masking and splicing | 0.45 | 0.0% |
| GC (overlaps above) | 1502.24 | 17.3% |

Instrumented total: 8660.98 ms. Each sampled query has one large wrapper and one small child. Four tiny children trigger DBR fallback. PG failure cost is negligible here; it is not a full-query retry.

One previous regression (`Q611055-482-20260825.095221.739`) spent 112.77 ms of 168.23 ms in GC in this run. This demonstrates a source of latency variance, without establishing the cause of every earlier regression.

## Rejected punctuation optimization

A per-dialect punctuation lookup avoided keyword-trie probes where no longer token could begin. Its failure-first work regression failed before implementation; the subsequent 22-test tokenizer/region suite passed. The production change was reverted because end-to-end latency did not improve. The patch is retained under results.

## Full corpus: fresh single-pass comparisons

All 1,628 inputs, including errors; identical order, sequential processes, Python tokenizer, Python 3.11.6, macOS arm64. Baseline is da8a92d74. No HTTP, queueing or log I/O. These single-pass results are exploratory and do not replace the original three-pass benchmark. Run order: baseline, punctuation candidate, then GC experiments followed by the full GC7000 run. No CPU affinity or thermal control.

| Workload | Inputs | Baseline ms | Punctuation ms | GC7000 ms | GC7000 reduction |
|---|---:|---:|---:|---:|---:|
| All | 1628 | 80.42 | 80.73 | 78.32 | 2.6% |
| Samsung | 704 | 179.39 | 180.02 | 174.67 | 2.6% |
| Conde Nast | 924 | 5.01 | 5.08 | 4.91 | 2.1% |
| >=100k | 107 | 806.59 | 806.23 | 793.51 | 1.6% |

All output/status/error-detail hashes match baseline for both full-corpus experiments (1,610 SQL outputs, 18 errors).

## GC threshold trade-off: repeated large-query sample

11 queries sampled by `four_way.py --large-sample`, five passes each, median per query then arithmetic mean. Fresh process per threshold; thresholds 1 and 2 remain 10. GC remains enabled. Peak RSS includes imports, corpus loading, and all repetitions; it is per process, not incremental AST memory. This is a latency/memory experiment, not a production recommendation.

| Threshold 0 | Mean ms | Reduction | Peak RSS MiB |
|---|---:|---:|---:|
| 700 | 778.50 | 0.0% | 250.6 |
| 7,000 | 732.81 | 5.9% | 408.2 |
| 70,000 | 699.79 | 10.1% | 602.5 |

Full-corpus GC7000 peak RSS: 403.7 MiB.

## Next priorities

1. Use completed conversion work as the performance baseline. Keep failures in workload-wide figures but show outcomes alongside timing.
2. Focus on successful PG expression parsing, token creation, and AST allocation. The proposed boundary-only scanner addresses only about 3% of this sample.
3. Evaluate native tokenization after resolving the previously observed Databricks escape discrepancy. A full native parser is a larger project; this follow-up did not measure one.
4. Treat GC tuning as a memory-budget decision and verify it under production concurrency. The full-corpus gain at 7000 was only 2.6%.

## Reproduction and evidence

```sh
# Use a clean checkout of da8a92d74 for the baseline.
PY=/private/tmp/sqlglot-tokenizer-bench/bin/python
$PY benchmarks/large_query_profile.py --root /path/to/baseline --output /tmp/stages.json
$PY benchmarks/four_way.py --root /path/to/baseline --commit da8a92d74 \
  --variant our_branch --output /tmp/default.json --repeats 1
$PY benchmarks/four_way.py --root /path/to/baseline --commit da8a92d74 \
  --variant our_branch --output /tmp/gc7000.json --repeats 1 --gc-threshold0 7000
# For each of 700, 7000, 70000, add --large-sample --repeats 5.
```

Use the current benchmark scripts against the chosen `--root`. The scripts configure the Python tokenizer and feature flags themselves. The rejected punctuation variant is reproduced by decompressing and applying its stored patch to a separate baseline checkout.

[Raw timings, attempt journals, stage events and SHA256 manifest](results/large-query-2026-10-14/manifest.json). [Rejected patch](results/large-query-2026-10-14/rejected-punctuation.patch.gz). [Original four-method benchmark, now with outcomes by size](four_way_report.md).
