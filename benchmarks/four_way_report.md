# Four-way transpilation benchmark

## Scope and methodology

All **1,628 queries** (704 Samsung, 924 Conde Nast) were sent unchanged to every method, including queries that fail. **Three full passes per method: 19,536 attempts.** No query is excluded from the headline tables.

Each table entry is computed from per-query median wall latency across three passes. Mean is arithmetic; p95/p99 use nearest rank. Total is the sum of per-query medians, not a concurrent throughput measurement. Failures contribute their measured time to all-attempt statistics.

| Method | Source | Route |
|---|---|---|
| Plain vanilla | `5de8561741` | Entire query parsed as Databricks |
| MULTIDIALECT | `5de8561741` | `split_pg_outer`, PG outer + DBR inners |
| SUBQUERY_TREE | `5de8561741` | Original lite parser; PG-first region parsing |
| Our branch | `48f12cfe30` | SUBQUERY_TREE with token reuse and fewer AST allocations |

Environment: `macOS-26.5.1-arm64-arm-64bit`; Python `3.11.6`; `arm64`; 12 visible logical CPUs. One sequential request at a time in one process; methods ran in separate sequential processes. This does not exercise a 4–6-worker deployment.

Python tokenizer (`SQLGLOTRS_TOKENIZER=0`), source Databricks, target e6, pretty-print off, comment stripping on. Hybrid mode, guardrail and Samsung alias mode disabled. The real `convert_query` handler is timed after import/warm-up; HTTP, queueing, startup, logging I/O, result hashing and evidence-file writes are excluded. CPU time is also recorded.

Fixed randomized query order, reversed on pass 2. Method order: vanilla, our branch, MULTIDIALECT, SUBQUERY_TREE. No CPU affinity or thermal controls were imposed. Small differences should not be treated as statistically established improvements.

## Findings

- **Combined:** our branch changes full-corpus mean latency from 92.20 to 78.64 ms versus SUBQUERY_TREE (14.7% reduction); p95 changes from 797.50 to 673.08 ms.
- **Samsung:** our branch changes full-corpus mean latency from 207.07 to 175.81 ms versus SUBQUERY_TREE (15.1% reduction); p95 changes from 1043.24 to 887.13 ms.
- **Conde Nast:** our branch changes full-corpus mean latency from 4.68 to 4.60 ms versus SUBQUERY_TREE (1.7% reduction); p95 changes from 14.66 to 14.67 ms.

The gap to plain vanilla is shown explicitly below. Because the complete workload contains mixed-dialect queries, vanilla's early failures prevent its all-attempt average from being interpreted as an achievable latency floor for equivalent output. The secondary shared-success table helps separate this effect from parsing overhead.

## 1. Full corpus: every attempt

**A failed parse can stop early.** Vanilla's all-attempt latency is useful as a cost measurement for the exact inputs, but is not a speed measurement for equivalent successful transpilation.

### Combined — 1628 queries

| Method | Success / failure | Mean ms | p50 ms | p95 ms | p99 ms | Max ms | Total s | Mean CPU ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain vanilla | 1151 / 477 | 24.65 | 4.11 | 190.60 | 245.67 | 257.35 | 40.13 | 24.65 |
| MULTIDIALECT | 1562 / 66 | 158.89 | 5.91 | 1416.29 | 1882.02 | 1973.87 | 258.67 | 158.78 |
| SUBQUERY_TREE | 1610 / 18 | 92.20 | 4.45 | 797.50 | 1072.11 | 1215.26 | 150.10 | 92.04 |
| Our branch | 1610 / 18 | 78.64 | 4.13 | 673.08 | 918.65 | 1011.39 | 128.02 | 78.57 |

### Samsung — 704 queries

| Method | Success / failure | Mean ms | p50 ms | p95 ms | p99 ms | Max ms | Total s | Mean CPU ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain vanilla | 250 / 454 | 50.82 | 18.60 | 240.54 | 247.65 | 257.35 | 35.77 | 50.81 |
| MULTIDIALECT | 704 / 0 | 358.72 | 115.26 | 1857.05 | 1893.53 | 1973.87 | 252.54 | 358.48 |
| SUBQUERY_TREE | 704 / 0 | 207.07 | 70.50 | 1043.24 | 1082.63 | 1215.26 | 145.77 | 206.70 |
| Our branch | 704 / 0 | 175.81 | 58.13 | 887.13 | 945.20 | 1011.39 | 123.77 | 175.65 |

### Conde Nast — 924 queries

| Method | Success / failure | Mean ms | p50 ms | p95 ms | p99 ms | Max ms | Total s | Mean CPU ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Plain vanilla | 901 / 23 | 4.72 | 2.95 | 16.12 | 25.25 | 34.47 | 4.36 | 4.72 |
| MULTIDIALECT | 858 / 66 | 6.63 | 3.99 | 21.33 | 37.66 | 57.58 | 6.13 | 6.63 |
| SUBQUERY_TREE | 906 / 18 | 4.68 | 2.91 | 14.66 | 24.68 | 38.47 | 4.33 | 4.68 |
| Our branch | 906 / 18 | 4.60 | 2.93 | 14.67 | 24.15 | 69.61 | 4.25 | 4.60 |

## 2. Our branch versus each method — all attempts

Ratios compare means over the same complete input sets, including failures. A ratio below 1 means lower latency for our branch; a negative reduction means slower.

| Corpus | Compared with | Our / their mean | Our latency reduction |
|---|---|---:|---:|
| all | Plain vanilla | 3.19× | -219.0% |
| all | MULTIDIALECT | 0.49× | 50.5% |
| all | SUBQUERY_TREE | 0.85× | 14.7% |
| samsung | Plain vanilla | 3.46× | -246.0% |
| samsung | MULTIDIALECT | 0.49× | 51.0% |
| samsung | SUBQUERY_TREE | 0.85× | 15.1% |
| condenast | Plain vanilla | 0.98× | 2.4% |
| condenast | MULTIDIALECT | 0.69× | 30.5% |
| condenast | SUBQUERY_TREE | 0.98× | 1.7% |

## 3. Effect of query size — all attempts

Size is SQL character count, including the wrapper. All outcomes remain included.

| Characters | Queries | Vanilla mean ms | MULTIDIALECT mean ms | SUBQUERY_TREE mean ms | Our mean ms |
|---|---:|---:|---:|---:|---:|
| <5k | 1092 | 3.33 | 4.78 | 3.63 | 3.45 |
| 5k–20k | 134 | 16.68 | 26.71 | 17.28 | 15.80 |
| 20k–100k | 295 | 38.40 | 245.59 | 148.70 | 125.96 |
| ≥100k | 107 | 214.30 | 1658.06 | 934.18 | 794.24 |

## 4. Successful and failed attempts, separately

These are diagnostic breakdowns; each method may have a different successful subset. Do not compare these means as if they represented identical workloads.

| Corpus | Method | Successful mean ms | Failed mean ms |
|---|---|---:|---:|
| all | Plain vanilla | 5.96 | 69.75 |
| all | MULTIDIALECT | 165.46 | 3.41 |
| all | SUBQUERY_TREE | 93.18 | 4.48 |
| all | Our branch | 79.47 | 4.47 |
| samsung | Plain vanilla | 10.25 | 73.15 |
| samsung | MULTIDIALECT | 358.72 | — |
| samsung | SUBQUERY_TREE | 207.07 | — |
| samsung | Our branch | 175.81 | — |
| condenast | Plain vanilla | 4.77 | 2.65 |
| condenast | MULTIDIALECT | 6.88 | 3.41 |
| condenast | SUBQUERY_TREE | 4.69 | 4.48 |
| condenast | Our branch | 4.61 | 4.47 |

## 5. Secondary comparison: inputs successful in all four methods

This intentionally excludes some inputs and is **not** the headline result. A returned SQL string is counted as success; semantic equivalence across all methods is not established.

| Corpus | Shared inputs | Vanilla mean ms | MULTIDIALECT mean ms | SUBQUERY_TREE mean ms | Our mean ms |
|---|---:|---:|---:|---:|---:|
| all | 1103 | 6.09 | 8.59 | 5.73 | 5.35 |
| samsung | 250 | 10.25 | 14.51 | 9.16 | 7.75 |
| condenast | 853 | 4.87 | 6.85 | 4.72 | 4.65 |

## 6. Ten slowest original SUBQUERY_TREE inputs

Times are median ms; `F` means the attempt failed. IDs can be joined to the repository corpus.

| Corpus / query ID | Characters | Vanilla | MULTIDIALECT | SUBQUERY_TREE | Our branch |
|---|---:|---:|---:|---:|---:|
| samsung / `Q611055-58-20260825.084001.407` | 335342 | 248.37 F | 1851.21 | 1215.26 | 918.65 |
| samsung / `Q611055-42-20260825.083722.128` | 333576 | 227.05 F | 1839.57 | 1136.41 | 834.83 |
| samsung / `Q611055-60-20260825.084001.425` | 335345 | 250.08 F | 1876.36 | 1107.77 | 903.38 |
| samsung / `Q611055-94-20260825.085336.518` | 333695 | 229.67 F | 1828.62 | 1107.13 | 848.96 |
| samsung / `Q611055-44-20260825.083754.364` | 340840 | 244.37 F | 1875.61 | 1105.35 | 908.52 |
| samsung / `Q611055-45-20260825.083754.367` | 340841 | 232.85 F | 1893.74 | 1094.39 | 1011.39 |
| samsung / `Q611055-46-20260825.083810.452` | 342654 | 237.99 F | 1874.12 | 1084.94 | 945.20 |
| samsung / `Q611055-55-20260825.084001.277` | 335304 | 246.82 F | 1855.45 | 1082.63 | 926.39 |
| samsung / `Q611055-56-20260825.084001.286` | 335343 | 231.63 F | 1868.52 | 1080.81 | 903.07 |
| samsung / `Q611055-90-20260825.085336.432` | 333695 | 237.50 F | 1810.14 | 1077.29 | 919.33 |

## 7. Largest per-query regressions versus SUBQUERY_TREE

Some individual queries regress despite the aggregate improvement. These are the five largest increases in per-query median latency; all remain included in the headline tables.

| Corpus / query ID | Characters | SUBQUERY_TREE ms | Our branch ms | Increase ms |
|---|---:|---:|---:|---:|
| samsung / `Q611055-704-20260825.102906.681` | 84879 | 252.03 | 384.87 | 132.85 |
| samsung / `Q611055-473-20260825.094937.836` | 25809 | 69.59 | 143.93 | 74.33 |
| samsung / `Q611055-432-20260825.093222.917` | 27442 | 69.60 | 130.25 | 60.65 |
| condenast / `Condenast::a6204f4a38116d46eb2b583a946031b8b3ac4e278f77a9593f2f2bf4475138aa#1` | 4996 | 12.49 | 69.61 | 57.13 |
| samsung / `Q611055-482-20260825.095221.739` | 25182 | 68.11 | 116.83 | 48.72 |

The 4,996-character Conde Nast query listed above took 69.61, 14.30, 88.43 ms on our branch, versus 12.49, 14.07, 12.41 ms on the original tree. This run does not isolate the cause of that variability; mean improvements should not be interpreted as a guarantee for every query.

## 8. Repeat variability and parity

Full-pass sums below include all attempts, without median aggregation. They expose run-to-run variation; they exclude time spent writing benchmark evidence.

| Method | Pass 1 seconds | Pass 2 seconds | Pass 3 seconds |
|---|---:|---:|---:|
| Plain vanilla | 40.06 | 40.38 | 40.40 |
| MULTIDIALECT | 260.15 | 262.25 | 265.90 |
| SUBQUERY_TREE | 152.44 | 152.35 | 156.86 |
| Our branch | 128.47 | 129.91 | 133.58 |

Original SUBQUERY_TREE versus our branch: **0 output/status/error-detail hash differences** across the full corpus. All repeated outcomes were stable.

## Evidence and reproduction

- [`four_way.py`](four_way.py): runner and exact flags.
- [`four_way_summary.py`](four_way_summary.py): aggregation and report generator.
- [`results/four-way-2026-10-14/summary.json`](results/four-way-2026-10-14/summary.json): machine-readable aggregates.
- [`results/four-way-2026-10-14/per_query.csv`](results/four-way-2026-10-14/per_query.csv): all query IDs, sizes, outcomes and median timings.
- [`results/four-way-2026-10-14/environment.json`](results/four-way-2026-10-14/environment.json): Python and dependency versions.
- The same results directory contains compressed worker results and attempt journals, with source/corpus hashes.

Extract the two commits into separate directories using `git archive`. Run each worker sequentially with the same Python environment:

```sh
python benchmarks/four_way.py --root <checkout> --commit <commit> \
  --variant <vanilla|multidialect|subquery_tree|our_branch> \
  --repeats 3 --output <results>/<variant>.json
python benchmarks/four_way_summary.py --input <results> --output <report-directory>
```

These measurements quantify local conversion cost. They do not establish production p95 under concurrent load, request queueing, worker scheduling or network overhead. Rust-tokenizer results from earlier experiments are not mixed into this comparison.
