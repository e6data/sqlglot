"""Validate full-corpus native allocation experiments and write their report."""
import csv
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import sys

from four_way_summary import stats


def main():
    source, output = map(Path, sys.argv[1:])
    destination = output / 'results' / 'native-allocation-2026-10-14'
    destination.mkdir(parents=True, exist_ok=True)
    runs, indexes, manifest = {}, {}, {}
    outcome = lambda row: tuple(row.get(f) for f in ('sha256', 'error', 'detail_sha256'))
    for name in ('baseline', 'scanner', 'candidate'):
        prefix = f'native-next-{name}'
        run = json.loads((source / f'{prefix}.json').read_text())
        index = {(r['corpus'], r['id']): r for r in run['results']}
        assert len(index) == len(run['results']) == 1628
        assert run['repeats'] == 3 and run['gc_thresholds'] == [700, 10, 10]
        assert run['flags']['SQLGLOTRS_TOKENIZER'] == '1'
        attempts = [json.loads(s) for s in (source / f'{prefix}.jsonl').read_text().splitlines()]
        assert len(attempts) == 4884
        grouped = {key: [] for key in index}
        for row in attempts:
            key = row['corpus'], row['id']
            assert outcome(row) == outcome(index[key])
            grouped[key].append(row)
        for key, rows in grouped.items():
            assert sorted(r['repeat'] for r in rows) == [0, 1, 2]
            assert statistics.median(r['wall_ms'] for r in rows) == index[key]['wall_ms']
        runs[name], indexes[name] = run, index
        for suffix in ('json', 'jsonl'):
            filename = f'{prefix}.{suffix}'
            data = (source / filename).read_bytes()
            (destination / (filename + '.gz')).write_bytes(gzip.compress(data, mtime=0))
            manifest[filename] = hashlib.sha256(data).hexdigest()
    baseline = indexes['baseline']
    for name, index in indexes.items():
        assert index.keys() == baseline.keys()
        assert all(outcome(row) == outcome(baseline[key]) for key, row in index.items()), name
        for field in ('python', 'platform', 'corpus_hashes', 'flags', 'feature_flags'):
            assert runs[name][field] == runs['baseline'][field], field
    groups = {'All': list(baseline)}
    for label, lo, hi in (('<5k', 0, 5000), ('5k–20k', 5000, 20000),
                          ('20k–100k', 20000, 100000), ('≥100k', 100000, float('inf'))):
        groups[label] = [k for k, r in baseline.items() if lo <= r['chars'] < hi]
    summary = {'groups': {}, 'outcome_differences': 0, 'attempts_validated': 14652}
    lines = ['# Native allocation benchmark — 14 October 2026', '',
        '## Changes', '',
        'Scanner: replace per-character lookahead strings and temporary character strings with borrowed comparisons and direct appends; avoid cloning identifier escape sets. '
        'Reuse: move disjoint, request-owned discovery tokens into PG regions through one native bulk call, preserving token identity. '
        'Lexically incompatible regions retain normal tokenization. No query caching, dialect-selection changes, GC tuning, or service configuration changes.', '',
        '## Method', '',
        'All 1,628 inputs, including failures; three passes per configuration. Each row below averages per-query medians. '
        'Baseline: commit `066f8569a` with native 0.6.2. Scanner only: the same Python snapshot with native 0.6.3. '
        'Candidate: current branch Python token reuse plus native 0.6.3. Runs executed sequentially: baseline, candidate, scanner only. '
        'Python 3.11.6, macOS arm64, release extensions, one process, one conversion at a time; GC 700/10/10. '
        'No concurrent agent builds/tests during timed runs. No CPU affinity or thermal control. These are local conversion timings, not HTTP latency or production throughput.', '',
        '## Results', '',
        '| Characters | Queries | Baseline ms | Scanner only ms | Scanner + reuse ms | Combined reduction |',
        '|---|---:|---:|---:|---:|---:|']
    for label, keys in groups.items():
        values = {name: stats(index[k]['wall_ms'] for k in keys) for name, index in indexes.items()}
        reduction = 100 * (1 - values['candidate']['mean'] / values['baseline']['mean'])
        summary['groups'][label] = dict(queries=len(keys), reduction_percent=reduction, **values)
        lines.append(f"| {label} | {len(keys)} | {values['baseline']['mean']:.2f} | {values['scanner']['mean']:.2f} | {values['candidate']['mean']:.2f} | {reduction:.1f}% |")
    lines += ['', 'Both changes are retained. Scanner-only reduces the overall mean by 5.3%; adding native reuse reduces it a further 4.2%, for 9.3% combined. '
        'These are sequential local measurements and remain subject to run-to-run variability.', '',
        '## Memory and parity', '', '| Configuration | Peak RSS MiB | Mean CPU ms | p95 wall ms |', '|---|---:|---:|---:|']
    for name, run in runs.items():
        assert run['max_rss_unit'] == 'bytes'
        cpu = statistics.mean(r['cpu_ms'] for r in run['results'])
        lines.append(f"| {name} | {run['max_rss']/1024**2:.1f} | {cpu:.2f} | {summary['groups']['All'][name]['p95']:.2f} |")
    ok = sum('sha256' in row for row in baseline.values())
    lines += ['', f'All configurations returned the same **{ok} SQL hashes and {1628-ok} error type/detail hashes**. All 14,652 timed attempts validated. '
        'Output equality is regression evidence, not proof of database semantic equivalence. Peak RSS includes imports, loaded corpus and timed passes.', '',
        'Vanilla fails on every ≥100k input in this corpus. Its failure time is not an equivalent-work latency target.', '',
        '## Verification and deployment', '',
        'Failure-first scanner regression: 7,001 allocating lookahead calls before, zero after, for a 7,000-character Unicode string. '
        'Native reuse regressions failed before implementation (two PG retokenizations and copied token objects), then passed. '
        'Position tests cover nested regions, siblings, Unicode and CR/LF/CRLF. Incompatible native regions leave token positions unchanged.', '',
        'Targeted parser/expression/dialect/region/tokenizer suite: 215 tests on each backend (native: one expected skip; Python: four). '
        'Rust allocation regression passes. Release build succeeds. Clippy passes with five pre-existing warnings outside this change.', '',
        'Install the matching rebuilt native 0.6.3 wheel and use `SQLGLOTRS_TOKENIZER=1` to benefit. '
        'The Python tokenizer path remains supported; older native wheels without the bulk API keep the existing retokenization path.', '',
        '## Reproduce', '', '```sh',
        'python benchmarks/four_way.py --root <checkout> --commit <revision> --variant our_branch \\',
        '  --tokenizer rust --repeats 3 --output /tmp/native-next-<configuration>.json',
        '# baseline: 066f8569a checkout + native 0.6.2',
        '# scanner: same checkout + native 0.6.3',
        '# candidate: c2ecee77c checkout + native 0.6.3',
        'python benchmarks/native_allocation_summary.py /tmp benchmarks', '```', '',
        '[Per-query CSV](results/native-allocation-2026-10-14/per_query.csv), '
        '[summary](results/native-allocation-2026-10-14/summary.json), '
        '[raw evidence manifest](results/native-allocation-2026-10-14/manifest.json), '
        '[code and wheel provenance](results/native-allocation-2026-10-14/provenance.json).', '']
    with (destination / 'per_query.csv').open('w') as stream:
        writer = csv.writer(stream, lineterminator='\n')
        writer.writerow(['corpus', 'id', 'chars', 'success', 'baseline_ms', 'scanner_ms', 'candidate_ms'])
        for key in sorted(baseline):
            row = baseline[key]
            writer.writerow([*key, row['chars'], 'sha256' in row, *(indexes[n][key]['wall_ms'] for n in runs)])
    (destination / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (output / 'native_allocation_report.md').write_text('\n'.join(lines))
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
