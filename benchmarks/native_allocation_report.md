# Native allocation benchmark — 14 October 2026

## Changes

Scanner: replace per-character lookahead strings and temporary character strings with borrowed comparisons and direct appends; avoid cloning identifier escape sets. Reuse: move disjoint, request-owned discovery tokens into PG regions through one native bulk call, preserving token identity. Lexically incompatible regions retain normal tokenization. No query caching, dialect-selection changes, GC tuning, or service configuration changes.

## Method

All 1,628 inputs, including failures; three passes per configuration. Each row below averages per-query medians. Baseline: commit `066f8569a` with native 0.6.2. Scanner only: the same Python snapshot with native 0.6.3. Candidate: current branch Python token reuse plus native 0.6.3. Runs executed sequentially: baseline, candidate, scanner only. Python 3.11.6, macOS arm64, release extensions, one process, one conversion at a time; GC 700/10/10. No concurrent agent builds/tests during timed runs. No CPU affinity or thermal control. These are local conversion timings, not HTTP latency or production throughput.

## Results

| Characters | Queries | Baseline ms | Scanner only ms | Scanner + reuse ms | Combined reduction |
|---|---:|---:|---:|---:|---:|
| All | 1628 | 70.47 | 66.74 | 63.95 | 9.3% |
| <5k | 1092 | 3.01 | 2.70 | 2.57 | 14.6% |
| 5k–20k | 134 | 13.59 | 11.91 | 11.80 | 13.2% |
| 20k–100k | 295 | 115.13 | 107.07 | 100.62 | 12.6% |
| ≥100k | 107 | 707.14 | 677.71 | 654.60 | 7.4% |

Both changes are retained. Scanner-only reduces the overall mean by 5.3%; adding native reuse reduces it a further 4.2%, for 9.3% combined. These are sequential local measurements and remain subject to run-to-run variability.

## Memory and parity

| Configuration | Peak RSS MiB | Mean CPU ms | p95 wall ms |
|---|---:|---:|---:|
| baseline | 353.6 | 70.25 | 612.63 |
| scanner | 348.2 | 66.60 | 576.06 |
| candidate | 322.3 | 63.78 | 557.45 |

All configurations returned the same **1610 SQL hashes and 18 error type/detail hashes**. All 14,652 timed attempts validated. Output equality is regression evidence, not proof of database semantic equivalence. Peak RSS includes imports, loaded corpus and timed passes.

Vanilla fails on every ≥100k input in this corpus. Its failure time is not an equivalent-work latency target.

## Verification and deployment

Failure-first scanner regression: 7,001 allocating lookahead calls before, zero after, for a 7,000-character Unicode string. Native reuse regressions failed before implementation (two PG retokenizations and copied token objects), then passed. Position tests cover nested regions, siblings, Unicode and CR/LF/CRLF. Incompatible native regions leave token positions unchanged.

Targeted parser/expression/dialect/region/tokenizer suite: 215 tests on each backend (native: one expected skip; Python: four). Rust allocation regression passes. Release build succeeds. Clippy passes with five pre-existing warnings outside this change.

Install the matching rebuilt native 0.6.3 wheel and use `SQLGLOTRS_TOKENIZER=1` to benefit. The Python tokenizer path remains supported; older native wheels without the bulk API keep the existing retokenization path.

## Reproduce

```sh
python benchmarks/four_way.py --root <checkout> --commit <revision> --variant our_branch \
  --tokenizer rust --repeats 3 --output /tmp/native-next-<configuration>.json
# baseline: 066f8569a checkout + native 0.6.2
# scanner: same checkout + native 0.6.3
# candidate: c2ecee77c checkout + native 0.6.3
python benchmarks/native_allocation_summary.py /tmp benchmarks
```

[Per-query CSV](results/native-allocation-2026-10-14/per_query.csv), [summary](results/native-allocation-2026-10-14/summary.json), [raw evidence manifest](results/native-allocation-2026-10-14/manifest.json), [code and wheel provenance](results/native-allocation-2026-10-14/provenance.json).
