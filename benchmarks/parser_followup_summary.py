"""Validate and summarize the full-corpus parser follow-up measurements."""
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
    destination = output / 'results' / 'parser-followup-2026-10-14'
    destination.mkdir(parents=True, exist_ok=True)
    runs, indexes, manifest = {}, {}, {}
    fields = ('sha256', 'error', 'detail_sha256')
    for name in ('baseline', 'candidate'):
        prefix = f'parser-next-{name}'
        run = json.loads((source / f'{prefix}.json').read_text())
        index = {(r['corpus'], r['id']): r for r in run['results']}
        assert len(index) == len(run['results']) == 1628
        assert run['repeats'] == 3 and run['gc_thresholds'] == [700, 10, 10]
        assert run['flags']['SQLGLOTRS_TOKENIZER'] == '1'
        grouped = {key: [] for key in index}
        for line in (source / f'{prefix}.jsonl').read_text().splitlines():
            row = json.loads(line)
            key = row['corpus'], row['id']
            assert all(row.get(f) == index[key].get(f) for f in fields)
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
    base, candidate = indexes.values()
    assert base.keys() == candidate.keys()
    assert all(all(row.get(f) == candidate[k].get(f) for f in fields) for k, row in base.items())
    for field in ('python', 'platform', 'corpus_hashes', 'flags', 'feature_flags'):
        assert runs['baseline'][field] == runs['candidate'][field], field
    groups = {'All': list(base)}
    for label, lo, hi in (('<5k', 0, 5000), ('5k–20k', 5000, 20000),
                         ('20k–100k', 20000, 100000), ('≥100k', 100000, float('inf'))):
        groups[label] = [k for k, r in base.items() if lo <= r['chars'] < hi]
    summary = {'attempts_validated': 9768, 'outcome_differences': 0, 'groups': {}}
    table = ['| Characters | Queries | Before ms | After ms | Reduction |', '|---|---:|---:|---:|---:|']
    for label, keys in groups.items():
        a, b = (stats(index[k]['wall_ms'] for k in keys) for index in (base, candidate))
        reduction = 100 * (1 - b['mean']/a['mean'])
        summary['groups'][label] = dict(queries=len(keys), before=a, after=b, reduction_percent=reduction)
        table.append(f"| {label} | {len(keys)} | {a['mean']:.2f} | {b['mean']:.2f} | {reduction:.1f}% |")
    for name, run in runs.items():
        assert run['max_rss_unit'] == 'bytes'
        summary[name] = dict(peak_rss_mib=run['max_rss']/1024**2,
                            mean_cpu_ms=statistics.mean(r['cpu_ms'] for r in run['results']))
    with (destination/'per_query.csv').open('w') as stream:
        writer = csv.writer(stream, lineterminator='\n')
        writer.writerow(['corpus', 'id', 'chars', 'success', 'before_ms', 'after_ms'])
        for key in sorted(base):
            r = base[key]
            writer.writerow([*key, r['chars'], 'sha256' in r, r['wall_ms'], candidate[key]['wall_ms']])
    (destination/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    (destination/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    (destination/'table.md').write_text('\n'.join(table)+'\n')
    print('\n'.join(table))
    print(json.dumps({k: summary[k] for k in ('baseline', 'candidate')}, indent=2))


if __name__ == '__main__':
    main()
