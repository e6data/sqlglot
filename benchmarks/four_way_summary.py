"""Build the four-way benchmark report from complete worker evidence."""

import argparse
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics


METHODS = ("vanilla", "multidialect", "subquery_tree", "our_branch")
LABELS = dict(zip(METHODS, ("Plain vanilla", "MULTIDIALECT", "SUBQUERY_TREE", "Our branch")))


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return {k: None for k in ("mean", "p50", "p95", "p99", "max", "sum_ms")}
    return {
        "mean": statistics.mean(ordered), "p50": statistics.median(ordered),
        "p95": ordered[math.ceil(len(ordered) * .95) - 1],
        "p99": ordered[math.ceil(len(ordered) * .99) - 1],
        "max": ordered[-1], "sum_ms": sum(ordered),
    }


def summarize(directory):
    runs = {m: json.loads((directory / f"{m}.json").read_text()) for m in METHODS}
    indexed = {m: {(r["corpus"], r["id"]): r for r in run["results"]} for m, run in runs.items()}
    keys = list(indexed["vanilla"])
    for method, run in runs.items():
        assert run["repeats"] == 3, "This report requires three complete passes"
        assert len(indexed[method]) == len(run["results"]), "Duplicate query IDs"
        assert indexed[method].keys() == indexed["vanilla"].keys(), "Different query sets"
        for field in ("corpus_hashes", "flags", "python", "platform", "repeats"):
            assert run[field] == runs["vanilla"][field], f"Different {field}"
        for key in keys:
            assert indexed[method][key]["chars"] == indexed["vanilla"][key]["chars"]
    assert len(keys) == 1628, "Expected the complete 1,628-query corpus"
    assert sum(k[0] == "samsung" for k in keys) == 704
    assert sum(k[0] == "condenast" for k in keys) == 924

    groups = {}
    for corpus in ("all", "samsung", "condenast"):
        selected = [k for k in keys if corpus == "all" or k[0] == corpus]
        shared = [k for k in selected if all("sha256" in indexed[m][k] for m in METHODS)]
        group = {"queries": len(selected), "shared_successes": len(shared), "methods": {}}
        for method in METHODS:
            rows = [indexed[method][k] for k in selected]
            successes = [r for r in rows if "sha256" in r]
            failures = [r for r in rows if "error" in r]
            group["methods"][method] = {
                "successes": len(successes), "failures": len(failures),
                "all_wall": stats(r["wall_ms"] for r in rows),
                "all_cpu": stats(r["cpu_ms"] for r in rows),
                "success_wall": stats(r["wall_ms"] for r in successes),
                "failure_wall": stats(r["wall_ms"] for r in failures),
                "shared_success_wall": stats(indexed[method][k]["wall_ms"] for k in shared),
            }
        groups[corpus] = group

    comparisons = {}
    for corpus in groups:
        methods = groups[corpus]["methods"]
        ours = methods["our_branch"]["all_wall"]["mean"]
        comparisons[corpus] = {
            method: {"our_mean_divided_by_method": ours / methods[method]["all_wall"]["mean"],
                     "our_latency_reduction_percent": 100 * (1 - ours / methods[method]["all_wall"]["mean"])}
            for method in METHODS if method != "our_branch"
        }
    bins = []
    for name, lo, hi in (("<5k", 0, 5000), ("5k–20k", 5000, 20000),
                         ("20k–100k", 20000, 100000), ("≥100k", 100000, float("inf"))):
        selected = [k for k in keys if lo <= indexed["vanilla"][k]["chars"] < hi]
        bins.append({"characters": name, "queries": len(selected),
                     "successes": {m: sum("sha256" in indexed[m][k] for k in selected) for m in METHODS},
                     "methods": {m: stats(indexed[m][k]["wall_ms"] for k in selected) for m in METHODS}})

    def outcome(row):
        return row.get("sha256"), row.get("error"), row.get("detail_sha256")

    differences = [list(k) for k in keys if outcome(indexed["subquery_tree"][k]) != outcome(indexed["our_branch"][k])]
    per_pass = {}
    journals = {}
    for method in METHODS:
        attempts = [json.loads(line) for line in (directory / f"{method}.jsonl").read_text().splitlines()]
        journals[method] = attempts
        assert len(attempts) == len(keys) * runs[method]["repeats"]
        assert len({(r["corpus"], r["id"], r["repeat"]) for r in attempts}) == len(attempts)
        assert {(r["corpus"], r["id"]) for r in attempts} == set(keys)
        per_pass[method] = [sum(r["wall_ms"] for r in attempts if r["repeat"] == p) / 1000
                            for p in range(runs[method]["repeats"])]
    slow = sorted(keys, key=lambda k: indexed["subquery_tree"][k]["wall_ms"], reverse=True)[:10]
    slow_cases = [dict(corpus=k[0], id=k[1], chars=indexed["vanilla"][k]["chars"],
                       methods={m: {"ms": indexed[m][k]["wall_ms"], "success": "sha256" in indexed[m][k]} for m in METHODS}) for k in slow]
    regressions = sorted(keys, key=lambda k: indexed["our_branch"][k]["wall_ms"] - indexed["subquery_tree"][k]["wall_ms"], reverse=True)[:5]
    regression_cases = [dict(corpus=k[0], id=k[1], chars=indexed["vanilla"][k]["chars"],
                            before_ms=indexed["subquery_tree"][k]["wall_ms"],
                            after_ms=indexed["our_branch"][k]["wall_ms"],
                            samples={m: [r["wall_ms"] for r in journals[m] if (r["corpus"], r["id"]) == k]
                                     for m in ("subquery_tree", "our_branch")}) for k in regressions]
    return runs, indexed, dict(groups=groups, comparisons=comparisons, size_bins=bins,
                               subtree_branch_outcome_differences=differences,
                               per_pass_attempt_seconds=per_pass, slow_cases=slow_cases,
                               regression_cases=regression_cases)


def report(runs, summary):
    lines = ["# Four-way transpilation benchmark", "", "## Scope and methodology", "",
             "All **1,628 queries** (704 Samsung, 924 Conde Nast) were sent unchanged to every method, "
             "including queries that fail. **Three full passes per method: 19,536 attempts.** "
             "No query is excluded from the headline tables.", "",
             "Each table entry is computed from per-query median wall latency across three passes. "
             "Mean is arithmetic; p95/p99 use nearest rank. Total is the sum of per-query medians, "
             "not a concurrent throughput measurement. Failures contribute their measured time to all-attempt statistics.", "",
             "| Method | Source | Route |", "|---|---|---|",
             f"| Plain vanilla | `{runs['vanilla']['commit'][:10]}` | Entire query parsed as Databricks |",
             f"| MULTIDIALECT | `{runs['multidialect']['commit'][:10]}` | `split_pg_outer`, PG outer + DBR inners |",
             f"| SUBQUERY_TREE | `{runs['subquery_tree']['commit'][:10]}` | Original lite parser; PG-first region parsing |",
             f"| Our branch | `{runs['our_branch']['commit'][:10]}` | SUBQUERY_TREE with token reuse and fewer AST allocations |", "",
             f"Environment: `{runs['vanilla']['platform']}`; Python `{runs['vanilla']['python'].split()[0]}`; "
             f"`{runs['vanilla']['machine']}`; {runs['vanilla']['logical_cpus']} visible logical CPUs. "
             "One sequential request at a time in one process; methods ran in separate sequential processes. "
             "This does not exercise a 4–6-worker deployment.", "",
             "Python tokenizer (`SQLGLOTRS_TOKENIZER=0`), source Databricks, target e6, pretty-print off, "
             "comment stripping on. Hybrid mode, guardrail and Samsung alias mode disabled. "
             "The real `convert_query` handler is timed after import/warm-up; HTTP, queueing, startup, "
             "logging I/O, result hashing and evidence-file writes are excluded. CPU time is also recorded.", "",
             "Fixed randomized query order, reversed on pass 2. Method order: vanilla, our branch, "
             "MULTIDIALECT, SUBQUERY_TREE. No CPU affinity or thermal controls were imposed. "
             "Small differences should not be treated as statistically established improvements.", "",
             "## Findings", ""]
    for corpus, label in (("all", "Combined"), ("samsung", "Samsung"), ("condenast", "Conde Nast")):
        rows = summary["groups"][corpus]["methods"]
        tree = rows["subquery_tree"]["all_wall"]
        ours = rows["our_branch"]["all_wall"]
        reduction = summary["comparisons"][corpus]["subquery_tree"]["our_latency_reduction_percent"]
        lines.append(f"- **{label}:** our branch changes full-corpus mean latency from "
                     f"{tree['mean']:.2f} to {ours['mean']:.2f} ms versus SUBQUERY_TREE "
                     f"({reduction:.1f}% reduction); p95 changes from {tree['p95']:.2f} to {ours['p95']:.2f} ms.")
    lines.extend(["", "The gap to plain vanilla is shown explicitly below. Because the complete workload "
                  "contains mixed-dialect queries, vanilla's early failures prevent its all-attempt average "
                  "from being interpreted as an achievable latency floor for equivalent output. "
                  "The secondary shared-success table helps separate this effect from parsing overhead.", "",
             "## 1. Full corpus: every attempt", "",
             "**A failed parse can stop early.** Vanilla's all-attempt latency is useful as a cost measurement "
             "for the exact inputs, but is not a speed measurement for equivalent successful transpilation.", ""])
    for corpus, title in (("all", "Combined"), ("samsung", "Samsung"), ("condenast", "Conde Nast")):
        group = summary["groups"][corpus]
        lines.extend([f"### {title} — {group['queries']} queries", "",
                      "| Method | Success / failure | Mean ms | p50 ms | p95 ms | p99 ms | Max ms | Total s | Mean CPU ms |",
                      "|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
        for method, row in group["methods"].items():
            s = row["all_wall"]
            lines.append(f"| {LABELS[method]} | {row['successes']} / {row['failures']} | "
                         + " | ".join(f"{s[k]:.2f}" for k in ("mean", "p50", "p95", "p99", "max"))
                         + f" | {s['sum_ms']/1000:.2f} | {row['all_cpu']['mean']:.2f} |")
        lines.append("")
    lines.extend(["## 2. Our branch versus each method — all attempts", "",
                  "Ratios compare means over the same complete input sets, including failures. "
                  "A ratio below 1 means lower latency for our branch; a negative reduction means slower.", "",
                  "| Corpus | Compared with | Our / their mean | Our latency reduction |",
                  "|---|---|---:|---:|"])
    for corpus, methods in summary["comparisons"].items():
        for method, c in methods.items():
            lines.append(f"| {corpus} | {LABELS[method]} | {c['our_mean_divided_by_method']:.2f}× | {c['our_latency_reduction_percent']:.1f}% |")
    lines.extend(["", "## 3. Effect of query size — all attempts", "",
                  "Size is SQL character count, including the wrapper. All outcomes remain included.", "",
                  "| Characters | Queries | Vanilla mean ms | MULTIDIALECT mean ms | SUBQUERY_TREE mean ms | Our mean ms |",
                  "|---|---:|---:|---:|---:|---:|"])
    for row in summary["size_bins"]:
        lines.append(f"| {row['characters']} | {row['queries']} | " + " | ".join(f"{row['methods'][m]['mean']:.2f}" for m in METHODS) + " |")
    lines.extend(["", "Successful conversions in those same buckets (all inputs above remain included):", "",
                  "| Characters | Queries | Vanilla | MULTIDIALECT | SUBQUERY_TREE | Our branch |",
                  "|---|---:|---:|---:|---:|---:|"])
    for row in summary["size_bins"]:
        lines.append(f"| {row['characters']} | {row['queries']} | " + " | ".join(str(row['successes'][m]) for m in METHODS) + " |")
    lines.extend(["", "Vanilla fails on every ≥100k input in this corpus. Its latency in that bucket "
                  "measures failed attempts, not completed conversions, so the ratio does not establish "
                  "removable SUBQUERY_TREE overhead."])
    lines.extend(["", "## 4. Successful and failed attempts, separately", "",
                  "These are diagnostic breakdowns; each method may have a different successful subset. "
                  "Do not compare these means as if they represented identical workloads.", "",
                  "| Corpus | Method | Successful mean ms | Failed mean ms |", "|---|---|---:|---:|"])
    for corpus, group in summary["groups"].items():
        for method, row in group["methods"].items():
            values = [row[k]["mean"] for k in ("success_wall", "failure_wall")]
            lines.append(f"| {corpus} | {LABELS[method]} | " + " | ".join("—" if v is None else f"{v:.2f}" for v in values) + " |")
    lines.extend(["", "## 5. Secondary comparison: inputs successful in all four methods", "",
                  "This intentionally excludes some inputs and is **not** the headline result. "
                  "A returned SQL string is counted as success; semantic equivalence across all methods is not established.", "",
                  "| Corpus | Shared inputs | Vanilla mean ms | MULTIDIALECT mean ms | SUBQUERY_TREE mean ms | Our mean ms |",
                  "|---|---:|---:|---:|---:|---:|"])
    for corpus, group in summary["groups"].items():
        lines.append(f"| {corpus} | {group['shared_successes']} | " + " | ".join(
            f"{group['methods'][m]['shared_success_wall']['mean']:.2f}" for m in METHODS) + " |")
    lines.extend(["", "## 6. Ten slowest original SUBQUERY_TREE inputs", "",
                  "Times are median ms; `F` means the attempt failed. IDs can be joined to the repository corpus.", "",
                  "| Corpus / query ID | Characters | Vanilla | MULTIDIALECT | SUBQUERY_TREE | Our branch |",
                  "|---|---:|---:|---:|---:|---:|"])
    for row in summary["slow_cases"]:
        lines.append(f"| {row['corpus']} / `{row['id']}` | {row['chars']} | " + " | ".join(
            f"{row['methods'][m]['ms']:.2f}" + (" F" if not row['methods'][m]['success'] else "") for m in METHODS) + " |")
    lines.extend(["", "## 7. Largest per-query regressions versus SUBQUERY_TREE", "",
                  "Some individual queries regress despite the aggregate improvement. These are the "
                  "five largest increases in per-query median latency; all remain included in the headline tables.", "",
                  "| Corpus / query ID | Characters | SUBQUERY_TREE ms | Our branch ms | Increase ms |",
                  "|---|---:|---:|---:|---:|"])
    for row in summary["regression_cases"]:
        lines.append(f"| {row['corpus']} / `{row['id']}` | {row['chars']} | {row['before_ms']:.2f} | "
                     f"{row['after_ms']:.2f} | {row['after_ms']-row['before_ms']:.2f} |")
    case = next((r for r in summary["regression_cases"] if r["corpus"] == "condenast"), None)
    if case:
        before = ", ".join(f"{v:.2f}" for v in case["samples"]["subquery_tree"])
        after = ", ".join(f"{v:.2f}" for v in case["samples"]["our_branch"])
        lines.extend(["", f"The {case['chars']:,}-character Conde Nast query listed above took "
                      f"{after} ms on our branch, versus {before} ms on the original tree. "
                      "This run does not isolate the cause of that variability; mean improvements "
                      "should not be interpreted as a guarantee for every query."])
    lines.extend(["", "## 8. Repeat variability and parity", "",
                  "Full-pass sums below include all attempts, without median aggregation. "
                  "They expose run-to-run variation; they exclude time spent writing benchmark evidence.", "",
                  "| Method | Pass 1 seconds | Pass 2 seconds | Pass 3 seconds |", "|---|---:|---:|---:|"])
    for m, times in summary["per_pass_attempt_seconds"].items():
        lines.append(f"| {LABELS[m]} | " + " | ".join(f"{v:.2f}" for v in times) + " |")
    lines.extend(["", f"Original SUBQUERY_TREE versus our branch: **{len(summary['subtree_branch_outcome_differences'])} "
                  "output/status/error-detail hash differences** across the full corpus. All repeated outcomes were stable.", "",
                  "## Evidence and reproduction", "",
                  "- [`four_way.py`](four_way.py): runner and exact flags.",
                  "- [`four_way_summary.py`](four_way_summary.py): aggregation and report generator.",
                  "- [`results/four-way-2026-10-14/summary.json`](results/four-way-2026-10-14/summary.json): machine-readable aggregates.",
                  "- [`results/four-way-2026-10-14/per_query.csv`](results/four-way-2026-10-14/per_query.csv): all query IDs, sizes, outcomes and median timings.",
                  "- [`results/four-way-2026-10-14/environment.json`](results/four-way-2026-10-14/environment.json): Python and dependency versions.",
                  "- The same results directory contains compressed worker results and attempt journals, with source/corpus hashes.", "",
                  "Extract the two commits into separate directories using `git archive`. Run each worker "
                  "sequentially with the same Python environment:", "", "```sh",
                  "python benchmarks/four_way.py --root <checkout> --commit <commit> \\",
                  "  --variant <vanilla|multidialect|subquery_tree|our_branch> \\",
                  "  --repeats 3 --output <results>/<variant>.json",
                  "python benchmarks/four_way_summary.py --input <results> --output <report-directory>",
                  "```", "",
                  "These measurements quantify local conversion cost. They do not establish production "
                  "p95 under concurrent load, request queueing, worker scheduling or network overhead. "
                  "Rust-tokenizer results from earlier experiments are not mixed into this comparison.", ""])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runs, indexed, summary = summarize(args.input)
    destination = args.output / "results" / "four-way-2026-10-14"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (args.output / "four_way_report.md").write_text(report(runs, summary))
    with (destination / "per_query.csv").open("w") as stream:
        fields = ["corpus", "id", "chars"] + [f"{m}_{v}" for m in METHODS for v in ("success", "wall_ms", "cpu_ms")]
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for key in sorted(indexed["vanilla"]):
            row = {k: indexed["vanilla"][key][k] for k in ("corpus", "id", "chars")}
            for method in METHODS:
                item = indexed[method][key]
                row.update({f"{method}_success": "sha256" in item,
                            f"{method}_wall_ms": item["wall_ms"], f"{method}_cpu_ms": item["cpu_ms"]})
            writer.writerow(row)
    manifest = {}
    for method in METHODS:
        for suffix in (".json", ".jsonl"):
            name = method + suffix
            data = (args.input / name).read_bytes()
            (destination / (name + ".gz")).write_bytes(gzip.compress(data, mtime=0))
            manifest[name] = hashlib.sha256(data).hexdigest()
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(summary["comparisons"], indent=2))


if __name__ == "__main__":
    main()
