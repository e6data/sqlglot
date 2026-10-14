# Repaired Rust tokenizer benchmark — 14 October 2026

## Changes

Native tokenizer 0.6.2 now receives `STRING_ESCAPE_SKIP_UNKNOWN` from Python and handles unknown escapes consistently with the Python tokenizer. The setting defaults to false; raw-string behavior and dialect selection are preserved. Install the matching rebuilt wheel with this Python source. No service flags or GC defaults were changed. The speed comparison measures switching from Python to the repaired native backend; the escape fix itself is a correctness change.

The Python allocation follow-up found 50,173 AST nodes created and 50,157 retained in a 333,705-character PG query. Only 16 were discarded. No additional Python parser change was made: most temporary AST allocation was already removed by the earlier branch work. This count does not measure all Python dictionary/string allocations.

## Method

Full 1,628-query corpus, including failures. Three passes per backend for our branch; medians per query, then aggregate statistics. One additional vanilla pass per backend checks the earlier Databricks escape discrepancy. Python 3.11.6, macOS arm64, release Rust extension, one process and one request at a time, no HTTP or log I/O. GC thresholds remain 700/10/10. Rust runs first, then Python; vanilla Rust then vanilla Python. No CPU affinity or thermal control. These are local conversion timings, not production throughput estimates.

## Our branch: Python versus repaired Rust

| Workload | Inputs | Python mean ms | Rust mean ms | Reduction | Python p95 ms | Rust p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| All | 1628 | 77.74 | 66.46 | 14.5% | 680.83 | 576.79 |
| samsung | 704 | 173.76 | 149.19 | 14.1% | 882.41 | 750.43 |
| condenast | 924 | 4.59 | 3.43 | 25.2% | 14.52 | 10.51 |
| <5k | 1092 | 3.42 | 2.68 | 21.5% | 8.97 | 7.20 |
| 5k–20k | 134 | 15.74 | 12.06 | 23.4% | 25.22 | 20.11 |
| 20k–100k | 295 | 124.15 | 106.91 | 13.9% | 256.59 | 206.38 |
| ≥100k | 107 | 785.94 | 673.99 | 14.2% | 926.54 | 793.01 |

## Outcomes and memory

| Mode | Backend | Returned SQL | Errors | Peak process RSS MiB |
|---|---|---:|---:|---:|
| our_branch | python | 1610 | 18 | 293.6 |
| our_branch | rust | 1610 | 18 | 346.4 |
| vanilla | python | 1151 | 477 | 268.5 |
| vanilla | rust | 1151 | 477 | 304.7 |

Outcome/hash differences: our branch **0**, vanilla **0**. Comparison includes successful SQL hashes and failed-attempt type/detail hashes. Returned SQL is not proof of database-level semantic equivalence.

Peak RSS includes imports, loading both corpora, and all timed passes; it is per process. Vanilla fails on every ≥100k query in this corpus, so its timing is not an equivalent-work target.

## Verification

The escape regression failed on native 0.6.1 before the fix. Tokenizer, parser, expression, PG, Spark, Databricks and region-reuse tests were run on both backends. The extension builds offline in release mode. Clippy passes with targeted deny attributes on the new helper; crate-wide `-D warnings` is blocked by five existing warnings outside the change.

## Reproduce

```sh
python -m maturin build --release --locked --manifest-path sqlglotrs/Cargo.toml --out /tmp/wheels
# Install the resulting wheel for the deployment's OS/Python version.
python benchmarks/four_way.py --root . --commit <revision> --variant our_branch \
  --tokenizer python --repeats 3 --output /tmp/native-followup-our_branch-python.json
# Repeat with --tokenizer rust and the corresponding output filename.
# For the parity-only vanilla comparison, use --variant vanilla --repeats 1.
```

[Per-query CSV](results/native-followup-2026-10-14/per_query.csv), [summary JSON](results/native-followup-2026-10-14/summary.json), [raw evidence manifest](results/native-followup-2026-10-14/manifest.json), [implementation and wheel provenance](results/native-followup-2026-10-14/provenance.json).
