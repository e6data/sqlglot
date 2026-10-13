# Rust tokenizer experiment — 2026-10-13

## Result

Building and enabling the existing `sqlglotrs` release extension reduces
`SUBQUERY_TREE` latency substantially. It does not remove the Python parser cost.
No application defaults or Rust source were changed in this experiment.

Base: Tanay's `wip/e6-multidialect-subquery-tree`, commit `5de856174`.
Environment: CPython 3.11.6, macOS ARM64, release-built sqlglotrs 0.6.1.
These are local serial measurements, not container/load-test predictions.

## Full corpus results

Each input ran once per backend/mode, in fresh processes. Python backend ran first.
Latency statistics below include paired successes only. Full outcomes were also
compared, including the 18 existing Conde Nast subtree failures.

| SUBQUERY_TREE workload | Cases | Python mean | Rust mean | Mean reduction | Python p95 | Rust p95 |
|---|---:|---:|---:|---:|---:|---:|
| Samsung | 704 | 240.0 ms | 155.8 ms | 35.1% | 1143.9 ms | 798.0 ms |
| Conde Nast | 924; 906 successes | 4.89 ms | 3.84 ms | 21.5% | 15.18 ms | 11.46 ms |

All 1,628 subtree input outcomes matched across backends; all 1,610 successful
SQL outputs were byte-identical (compared by SHA-256). Error-detail hashes also
matched for the 18 failures. This checks tokenizer parity, not the semantic
correctness of the existing transpiler against the source/target engines.

Repeated check: select 12 evenly spaced SQL-length ranks from each corpus, run
each five times, take its median, and run Rust before Python. The subtree mean
reduction was 26.1% for Samsung and 28.1% for Conde Nast. All sampled subtree
outcomes/outputs matched again. These ranges describe two different selections,
not a statistical confidence interval.

## Vanilla comparison and rollout limitation

The same runner also exercised the actual plain Databricks handler. It reproduced
11 backend-dependent output differences in Conde Nast queries involving regex
backslashes. For example, Python drops an unknown escape in `\d`; Rust retains it.
The cause is documented in `sqlglot/dialects/databricks.py`: the Python tokenizer's
`STRING_ESCAPE_SKIP_UNKNOWN=True` behavior is not implemented by Rust. This affects
SQL meaning and prevents treating the environment switch as a globally transparent
replacement. No change to the global default is included here.

Vanilla's full-corpus Conde Nast timing was unstable between measurement batches:
Rust's first pass was slower, but the reverse-order repeated sample was faster
(5.60 -> 4.90 ms mean). Do not interpret that first-pass aggregate as a stable
vanilla regression or use it to claim subtree beats vanilla. All raw full-pass
aggregates, including the unfavorable ones, are retained in the summary JSON.
The repeated Conde Nast sample measured subtree at 5.67 -> 4.08 ms and vanilla
at 5.60 -> 4.90 ms; these are small differences in absolute time and do not prove
one pipeline intrinsically faster. Do not compare full-corpus vanilla and subtree
percentiles across different successful input populations.

## Remaining cost with Rust active

Profiled Samsung query `Q611055-99-20260825.085440.746` (333,705 characters),
one warmup followed by three measured runs; median stage times:

| Stage | Rust-active time |
|---|---:|
| Discovery tokenization | 81.8 ms |
| Lightweight structural parse | 27.5 ms |
| Region cleanup | 44.8 ms |
| Region tokenization + parsing | 454.6 ms |
| AST transforms | 56.1 ms |
| SQL generation | 106.9 ms |
| Total | 786.8 ms |

Stage medians are computed independently and need not sum exactly to total.
Output matched the stored fixture. The earlier Python profile contained about
50,000 wrapper AST nodes; the inner query is only about 570 bytes. This case is
not dominated by many dialect regions. Even deleting discovery entirely would
remove only about 14% of the Rust-active elapsed time in this sample.

Next priorities:

1. Bring Rust string-escape behavior into parity before general enablement.
2. Retain tokens/source spans to avoid discovery and region retokenization, with
   explicit handling for dialect-dependent token differences.
3. Target Python parsing and AST allocation for the next major speedup. Region
   parsing is about 58% of this Rust-active sample; a native parser must avoid
   giving its savings back by constructing tens of thousands of Python objects.
4. Hoist the E6 keyword set and add a proven-safe normalization fast path as smaller
   optimizations. Neither replaces the parser work.

## Verification and reproduction

The targeted suite passed with each backend: 117 tests per run across
`tests.test_tokens`, `tests.test_subquery_tree`, `tests.dialects.test_e6`, and
`tests.dialects.test_postgres`. The suite does not cover the 11 corpus escape
differences above, so its success is not a global parity claim.

Build the repository's locked Rust extension into a benchmark virtual environment:

```sh
python -m maturin build --release --locked --manifest-path sqlglotrs/Cargo.toml --out /tmp/sqlglot-wheels
# Install the resulting wheel in the same environment used for the commands below.
python benchmarks/multidialect_tokenizer.py --output /tmp/tokenizer-full
python benchmarks/multidialect_tokenizer.py --samples 12 --repeats 5 --output /tmp/tokenizer-sample
python benchmarks/multidialect_tokenizer.py --summarize --output /tmp/tokenizer-full
```

The runner calls the actual async conversion handler directly. HTTP, process
startup, and logging are excluded; cleanup, transformations, quoting, and generation
are included. It requires the converter's dependencies and the built extension.
It verifies the requested backend is active and records mode, interpreter,
platform, source commit, output hashes, errors, and timings. It saves no SQL text.

Controlled settings: `PRETTY_PRINT=false`, `HYBRID_MULTIDIALECT=false`,
`SAMSUNG_TABLEAU_DASHBOARD=false`, `ENABLE_GUARDRAIL=false`,
`DBR_DOUBLE_QUOTED_IDENTIFIERS=false`,
`PRESERVE_DOUBLE_QUOTES_AROUND_IDENTIFIERS_DBR=false`, `E6_EXECUTOR_TYPE=java`,
`SKIP_COMMENT=true`, `FIX_QUOTE_ESCAPES=false`; backend selected with
`SQLGLOTRS_TOKENIZER=0/1`. Subtree is selected per worker; vanilla uses
`from_sql=databricks`. The Samsung alias method was not measured.

Checked-in evidence: [summary and stage profile](results/tokenizer-2026-10-13.json).
Per-query raw measurements from this run are in
`/private/tmp/sqlglot-tokenizer-full/` and `/private/tmp/sqlglot-tokenizer-repeat/`.
The benchmark environment and wheel are in `/private/tmp/sqlglot-tokenizer-bench/`
and `/private/tmp/sqlglot-tokenizer-wheels/`.
