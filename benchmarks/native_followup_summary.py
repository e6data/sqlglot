"""Validate and summarize the native-tokenizer follow-up evidence."""
import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path

from four_way_summary import stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    destination = args.output / "results" / "native-followup-2026-10-14"
    destination.mkdir(parents=True, exist_ok=True)
    runs, indexes, manifest = {}, {}, {}
    outcome_fields = ("sha256", "error", "detail_sha256")
    for mode in ("our_branch", "vanilla"):
        for backend in ("python", "rust"):
            name = f"native-followup-{mode}-{backend}"
            run = json.loads((args.input / f"{name}.json").read_text())
            index = {(r["corpus"], r["id"]): r for r in run["results"]}
            assert len(index) == len(run["results"]) == 1628
            assert run["repeats"] == (3 if mode == "our_branch" else 1)
            assert run["gc_thresholds"] == [700, 10, 10]
            assert run["flags"]["SQLGLOTRS_TOKENIZER"] == ("1" if backend == "rust" else "0")
            journal = [json.loads(line) for line in (args.input / f"{name}.jsonl").read_text().splitlines()]
            assert len(journal) == 1628 * run["repeats"]
            grouped = {key: [] for key in index}
            for record in journal:
                grouped[(record["corpus"], record["id"])].append(record)
            for key, values in grouped.items():
                assert len(values) == run["repeats"]
                assert stats(r["wall_ms"] for r in values)["p50"] == index[key]["wall_ms"]
                assert all(tuple(r.get(f) for f in outcome_fields) == tuple(index[key].get(f) for f in outcome_fields) for r in values)
            runs[mode, backend], indexes[mode, backend] = run, index
            for suffix in ("json", "jsonl"):
                filename = f"{name}.{suffix}"
                data = (args.input / filename).read_bytes()
                (destination / (filename + ".gz")).write_bytes(gzip.compress(data, mtime=0))
                manifest[filename] = hashlib.sha256(data).hexdigest()

    differences = {}
    for mode in ("our_branch", "vanilla"):
        py, rs = indexes[mode, "python"], indexes[mode, "rust"]
        assert py.keys() == rs.keys()
        for field in ("python", "platform", "corpus_hashes", "source_hashes", "feature_flags"):
            assert runs[mode, "python"][field] == runs[mode, "rust"][field], field
        differences[mode] = [key for key in py if any(py[key].get(f) != rs[key].get(f) for f in outcome_fields)]

    baseline = indexes["our_branch", "python"]
    groups = {"All": list(baseline)}
    groups.update({c: [k for k in baseline if k[0] == c] for c in ("samsung", "condenast")})
    for label, lo, hi in (("<5k", 0, 5000), ("5k–20k", 5000, 20000), ("20k–100k", 20000, 100000), ("≥100k", 100000, float("inf"))):
        groups[label] = [k for k in baseline if lo <= baseline[k]["chars"] < hi]
    summary = {"outcome_differences": differences, "groups": {}}
    lines = ["# Repaired Rust tokenizer benchmark — 14 October 2026", "",
        "## Changes", "",
        "Native tokenizer 0.6.2 now receives `STRING_ESCAPE_SKIP_UNKNOWN` from Python and handles unknown escapes consistently with the Python tokenizer. "
        "The setting defaults to false; raw-string behavior and dialect selection are preserved. "
        "Install the matching rebuilt wheel with this Python source. No service flags or GC defaults were changed. "
        "The speed comparison measures switching from Python to the repaired native backend; the escape fix itself is a correctness change.", "",
        "The Python allocation follow-up found 50,173 AST nodes created and 50,157 retained in a 333,705-character PG query. "
        "Only 16 were discarded. No additional Python parser change was made: most temporary AST allocation was already removed by the earlier branch work. "
        "This count does not measure all Python dictionary/string allocations.", "",
        "## Method", "",
        "Full 1,628-query corpus, including failures. Three passes per backend for our branch; medians per query, then aggregate statistics. "
        "One additional vanilla pass per backend checks the earlier Databricks escape discrepancy. "
        "Python 3.11.6, macOS arm64, release Rust extension, one process and one request at a time, no HTTP or log I/O. "
        "GC thresholds remain 700/10/10. Rust runs first, then Python; vanilla Rust then vanilla Python. "
        "No CPU affinity or thermal control. These are local conversion timings, not production throughput estimates.", "",
        "## Our branch: Python versus repaired Rust", "",
        "| Workload | Inputs | Python mean ms | Rust mean ms | Reduction | Python p95 ms | Rust p95 ms |",
        "|---|---:|---:|---:|---:|---:|---:|"]
    for label, keys in groups.items():
        values = {backend: stats(indexes["our_branch", backend][k]["wall_ms"] for k in keys) for backend in ("python", "rust")}
        reduction = 100 * (1 - values["rust"]["mean"] / values["python"]["mean"])
        summary["groups"][label] = dict(inputs=len(keys), reduction_percent=reduction, **values)
        lines.append(f"| {label} | {len(keys)} | {values['python']['mean']:.2f} | {values['rust']['mean']:.2f} | {reduction:.1f}% | {values['python']['p95']:.2f} | {values['rust']['p95']:.2f} |")
    lines.extend(["", "## Outcomes and memory", "",
        "| Mode | Backend | Returned SQL | Errors | Peak process RSS MiB |", "|---|---|---:|---:|---:|"])
    for (mode, backend), run in runs.items():
        ok = sum("sha256" in r for r in run["results"])
        assert run["max_rss_unit"] == "bytes"
        lines.append(f"| {mode} | {backend} | {ok} | {1628-ok} | {run['max_rss']/1024**2:.1f} |")
    lines.extend(["", f"Outcome/hash differences: our branch **{len(differences['our_branch'])}**, vanilla **{len(differences['vanilla'])}**. "
        "Comparison includes successful SQL hashes and failed-attempt type/detail hashes. Returned SQL is not proof of database-level semantic equivalence.", "",
        "Peak RSS includes imports, loading both corpora, and all timed passes; it is per process. "
        "Vanilla fails on every ≥100k query in this corpus, so its timing is not an equivalent-work target.", "",
        "## Verification", "",
        "The escape regression failed on native 0.6.1 before the fix. Tokenizer, parser, expression, PG, Spark, Databricks and region-reuse tests were run on both backends. "
        "The extension builds offline in release mode. Clippy passes with targeted deny attributes on the new helper; "
        "crate-wide `-D warnings` is blocked by five existing warnings outside the change.", "",
        "## Reproduce", "", "```sh",
        "python -m maturin build --release --locked --manifest-path sqlglotrs/Cargo.toml --out /tmp/wheels",
        "# Install the resulting wheel for the deployment's OS/Python version.",
        "python benchmarks/four_way.py --root . --commit <revision> --variant our_branch \\",
        "  --tokenizer python --repeats 3 --output /tmp/native-followup-our_branch-python.json",
        "# Repeat with --tokenizer rust and the corresponding output filename.",
        "# For the parity-only vanilla comparison, use --variant vanilla --repeats 1.", "```", "",
        "[Per-query CSV](results/native-followup-2026-10-14/per_query.csv), "
        "[summary JSON](results/native-followup-2026-10-14/summary.json), "
        "[raw evidence manifest](results/native-followup-2026-10-14/manifest.json), "
        "[implementation and wheel provenance](results/native-followup-2026-10-14/provenance.json).", ""])
    with (destination / "per_query.csv").open("w") as stream:
        fields = ["corpus", "id", "chars"] + [f"{m}_{b}_{k}" for m in ("our_branch", "vanilla") for b in ("python", "rust") for k in ("success", "wall_ms")]
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for key in sorted(baseline):
            row = {f: baseline[key][f] for f in ("corpus", "id", "chars")}
            for (mode, backend), index in indexes.items():
                row[f"{mode}_{backend}_success"] = "sha256" in index[key]
                row[f"{mode}_{backend}_wall_ms"] = index[key]["wall_ms"]
            writer.writerow(row)
    (destination / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (args.output / "native_followup_report.md").write_text("\n".join(lines))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
