"""Compare the API conversion pipelines in fresh Python/Rust-tokenizer processes.

Run with the benchmark venv's Python. Reports contain hashes, never query text.
Full corpus: --samples 0. Sampling uses evenly spaced ranks by SQL length per corpus.
Timings exclude HTTP, logging and startup; failures are recorded separately.
"""

import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def summarize(directory):
    """Compare only paired successes; keep output/status regressions visible."""
    summary = {}
    for mode in ("subtree", "vanilla"):
        runs = {backend: json.loads((directory / f"{backend}-{mode}.json").read_text())
                for backend in ("python", "rust")}
        indexed = {backend: {(r["corpus"], r["id"]): r for r in run["results"]}
                   for backend, run in runs.items()}
        if any(len(indexed[b]) != len(runs[b]["results"]) for b in runs):
            raise RuntimeError("Duplicate query IDs within a corpus")
        a, b = indexed["python"], indexed["rust"]
        if a.keys() != b.keys():
            raise RuntimeError("Benchmark workloads differ")
        differences = [list(k) for k in a if
                       (a[k].get("sha256"), a[k].get("error")) !=
                       (b[k].get("sha256"), b[k].get("error"))]
        error_details = [list(k) for k in a if "error" in a[k] and "error" in b[k]
                         and a[k].get("detail_sha256") != b[k].get("detail_sha256")]
        groups = {}
        for corpus in ("samsung", "condenast"):
            keys = [k for k in a if k[0] == corpus]
            paired = [k for k in keys if "sha256" in a[k] and "sha256" in b[k]]
            group = {"cases": len(keys), "paired_successes": len(paired)}
            for backend, rows in indexed.items():
                values = sorted(rows[k]["ms"] for k in paired)
                group[backend] = {
                    "successes": sum("sha256" in rows[k] for k in keys),
                    "mean_ms": statistics.mean(values) if values else None,
                    "p50_ms": statistics.median(values) if values else None,
                    # Nearest-rank percentile.
                    "p95_ms": values[max(0, (95 * len(values) + 99) // 100 - 1)]
                    if values else None,
                }
            groups[corpus] = group
        summary[mode] = {"output_or_status_differences": differences,
                         "error_detail_differences": error_details, "groups": groups}
    (directory / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


async def measure(args):
    logging.disable(logging.CRITICAL)
    import converter_api as api
    import sqlglot.tokens as tokens

    expected = args.backend == "rust"
    if tokens.USE_RS_TOKENIZER != expected:
        raise RuntimeError(f"Requested {args.backend}; Rust active={tokens.USE_RS_TOKENIZER}")
    api.SAMSUNG_TABLEAU_DASHBOARD = "False"
    api.SUBQUERY_TREE = args.mode == "subtree"

    async def convert(sql):
        return await api.convert_query(
            query=sql, query_id="tokenizer-benchmark", from_sql="databricks",
            to_sql="e6", feature_flags='{"PRETTY_PRINT": false}',
        )

    # Warm dialect loading and both wrapper/inner code paths outside measurements.
    for sql in ("SELECT 1", 'SELECT "a" FROM (SELECT `a` FROM t) s'):
        try:
            await convert(sql)
        except Exception:
            pass
    csv.field_size_limit(sys.maxsize)
    results = []
    for corpus in ("samsung", "condenast"):
        with gzip.open(ROOT / "tests" / corpus / "queries.csv.gz", "rt") as stream:
            rows = sorted(csv.DictReader(stream), key=lambda r: len(r["sql"]))
        if args.samples and len(rows) > args.samples:
            indices = {round(i * (len(rows) - 1) / max(1, args.samples - 1))
                       for i in range(args.samples)}
            rows = [rows[i] for i in sorted(indices)]
        for row in rows:
            times, outcomes = [], []
            for _ in range(args.repeats):
                started = time.perf_counter()
                try:
                    response = await convert(row["sql"])
                    output = response["converted_query"]
                    outcome = {"sha256": hashlib.sha256(output.encode()).hexdigest()}
                except Exception as exc:
                    # Hash the error detail too: don't hide different failures under HTTPException.
                    detail = str(getattr(exc, "detail", str(exc)))
                    outcome = {"error": type(exc).__name__,
                               "detail_sha256": hashlib.sha256(detail.encode()).hexdigest()}
                times.append((time.perf_counter() - started) * 1000)
                outcomes.append(outcome)
            if any(value != outcomes[0] for value in outcomes):
                raise RuntimeError(f"Nondeterministic result: {corpus}/{row['query_id']}")
            results.append({"corpus": corpus, "id": row["query_id"],
                            "group": row["group"], "chars": len(row["sql"]),
                            "ms": statistics.median(times), **outcomes[0]})
        print(f"{args.backend}/{args.mode}: {corpus} {len(rows)} cases", flush=True)
    return {"backend": args.backend, "mode": args.mode, "rust_active": expected,
            "python": sys.version, "platform": platform.platform(),
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                              text=True).strip(),
            "repeats": args.repeats, "samples": args.samples, "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--backend", choices=("python", "rust"))
    parser.add_argument("--mode", choices=("vanilla", "subtree"))
    parser.add_argument("--summarize", action="store_true")
    args = parser.parse_args()
    if args.summarize:
        summarize(args.output)
        return
    if args.repeats < 1 or args.samples < 0:
        parser.error("repeats must be positive and samples nonnegative")
    if args.backend:
        if not args.mode:
            parser.error("--mode is required with --backend")
        args.output.write_text(json.dumps(asyncio.run(measure(args)), indent=2))
        return
    args.output.mkdir(parents=True, exist_ok=True)
    for backend in ("python", "rust"):
        for mode in ("subtree", "vanilla"):
            env = dict(os.environ, SQLGLOTRS_TOKENIZER="1" if backend == "rust" else "0",
                       HYBRID_MULTIDIALECT="false", SUBQUERY_TREE="false",
                       SAMSUNG_TABLEAU_DASHBOARD="false", ENABLE_GUARDRAIL="false",
                       DBR_DOUBLE_QUOTED_IDENTIFIERS="false",
                       PRESERVE_DOUBLE_QUOTES_AROUND_IDENTIFIERS_DBR="false",
                       E6_EXECUTOR_TYPE="java", SKIP_COMMENT="true", FIX_QUOTE_ESCAPES="false")
            subprocess.run([sys.executable, str(Path(__file__).resolve()),
                            "--backend", backend, "--mode", mode,
                            "--samples", str(args.samples), "--repeats", str(args.repeats),
                            "--output", str(args.output / f"{backend}-{mode}.json")],
                           cwd=ROOT, env=env, check=True)
    summarize(args.output)


if __name__ == "__main__":
    main()
