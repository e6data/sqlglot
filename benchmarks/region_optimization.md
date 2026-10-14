# Region parsing optimization

Requirement: bring SUBQUERY_TREE closer to vanilla without losing existing corpus
outputs. Preserve PostgreSQL-first selection and Databricks fallback.

Plan:
1. Retain discovery tokens and source spans; reuse lexically compatible tokens in
   masked regions. Retokenize when cleanup or dialect differences invalidate them.
2. Reduce speculative parser work and temporary AST allocation with focused tests.
3. Compare identical benchmark samples before/after and check full corpus hashes.

Workflow: Vibe, with failure-first regressions. No Rust implementation changes.
Baseline: commit 8a4435a39, Python tokenizer, actual converter handler, warmed runs.

## Implementation

- Retain generic discovery tokens and exact subquery source offsets. Each region
  consumes its disjoint Python tokens once, adjusting positions and whitespace in
  place. Only placeholder tokens are newly allocated. The original index of source
  positions stays immutable so parent processing cannot corrupt child lookup.
- Reuse only the lexical intersection of the generic and PostgreSQL tokenizers.
  Different keywords, dollar strings, prefixed strings, comments and special
  operators fall back to normal tokenization. Databricks always uses its tokenizer.
- Cleanup that changes a region invalidates the retained tokens. Existing custom
  two-argument emit hooks still work; the API uses the new token-aware hook.
- Native tokenizer tokens have immutable positions. The initial copy-based reuse
  experiment did not improve native timings, so native configurations retain
  normal tokenization and receive only the parser allocation improvements.
- Extend qualified Column nodes in place, avoid speculative Identifier nodes in
  simple function arguments, and look ahead for typed strings without constructing
  a temporary literal AST. Dialect-specific lambda parsers retain their old path.

## Verification

- Demonstrated failing allocation/tokenization tests before each optimization:
  two redundant PG tokenizations; four Column objects for one qualified column;
  four Identifier objects for two arguments; two literal objects for one typed
  literal. These regressions now pass.
- Full corpus comparison: all 1,628 outcomes identical to baseline, including
  1,610 successful SQL output hashes and 18 error-detail hashes.
- Tests also cover token offsets, line/column and whitespace, nested/sibling
  regions, Unicode, newline variants, preprocessing invalidation, Databricks
  fallback, and ambiguous substring masking.
- Initial broad discovery ran 910 tests with five environment errors (missing
  pytz/duckdb or an unavailable `python` command), no assertion failures. After
  installing dependencies and setting PATH, the final targeted run passed 752
  parser/expression/transpilation/dialect tests. An additional keyword-table
  compatibility regression passed separately. Native-backend validation passed
  127 tests with two Python-only allocation checks skipped.

## Measured results

Twelve inputs per corpus, evenly spaced by query length; five warmed repetitions
per input; table shows the mean of per-input medians over successful conversions.
Actual converter handler, no HTTP or log I/O. Baseline is a clean archive of
8a4435a39. All before/after samples match output/status/error-detail hashes.

| Tokenizer | Corpus | Before | After | Latency change |
|---|---|---:|---:|---:|
| Python | Samsung | 230.42 ms | 204.19 ms | 11.4% lower |
| Python | Conde Nast | 5.68 ms | 5.65 ms | 0.6% lower |
| Rust | Samsung | 167.97 ms | 174.13 ms | 3.7% higher |
| Rust | Conde Nast | 4.16 ms | 4.66 ms | 12.1% higher |

These are local measurements, not production latency predictions. The Python
Samsung improvement is demonstrated; the small Python workload is essentially
unchanged. Native timings did not improve and should be checked on the deployment
hardware before adopting these shared parser changes with that backend. Native
token reuse was disabled after the copy-based prototype failed to improve timing.
The remaining native differences cannot be attributed to token reuse.

For Samsung query `Q611055-99-20260825.085440.746`, instrumentation of a whole-query
PostgreSQL parse counted **56,826 -> 50,173 AST allocations** (11.7% fewer). Both
parses retain 50,157 nodes and emit identical SQL. Discarded Column nodes fell from
4,343 to 2; discarded Identifier nodes from 2,321 to 9. This measures allocations,
not latency, and excludes the converter's other stages.

Evidence, per-input timings, configuration and source-file hashes:
[`results/region-2026-10-14.json`](results/region-2026-10-14.json).

Reproduce each measurement with `benchmarks/multidialect_tokenizer.py --backend
python --mode subtree --samples 12 --repeats 5 --output <file>`, in separate clean
baseline and modified checkouts. Use `SQLGLOTRS_TOKENIZER=0`,
`HYBRID_MULTIDIALECT=false`, `E6_EXECUTOR_TYPE=java`, `SKIP_COMMENT=true`, and the
other flags recorded in the tokenizer benchmark runner. Use samples=0 for parity.

## Logical flow

```text
before:
    discovery_tokens = generic_tokenize(query)
    regions = lite_parse(discovery_tokens)
    for region:
        sql = clean(mask_children(region))
        tree = postgres_parse(postgres_tokenize(sql))
        if PostgreSQL failed:
            tree = databricks_parse(databricks_tokenize(sql))
        emit_and_stitch(tree)

after (Python tokenizer):
    discovery_tokens = generic_tokenize(query)
    regions = lite_parse(discovery_tokens)  # retain exact source spans
    for region:
        sql = mask_children(region)
        tokens = consume_and_reposition_region_tokens(discovery_tokens)
        cleaned = clean(sql)
        if lexical_difference or cleaned != sql:
            tokens = postgres_tokenize(cleaned)
        tree = postgres_parse(tokens)
        if PostgreSQL failed:
            tree = databricks_parse(databricks_tokenize(cleaned))
        emit_and_stitch(tree)
```

Parser allocation changes apply to both tokenizer backends. This keeps the existing
region discovery and PG-first policy; it does not solve dialect ambiguity or make
mixed parsing as cheap as one known-dialect parse. The native-tokenizer Databricks
escape mismatch documented in the earlier report also remains outside this change.
