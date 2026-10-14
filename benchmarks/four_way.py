"""Full-corpus, single-process conversion latency for one benchmark variant.

Invoke this same runner against a clean --root checkout for each method. Every
input is measured, including failures. Output contains hashes, never SQL text.
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
import random
import statistics
import sys
import time


FLAGS = {
    "SQLGLOTRS_TOKENIZER": "0", "HYBRID_MULTIDIALECT": "false",
    "SUBQUERY_TREE": "false", "SAMSUNG_TABLEAU_DASHBOARD": "false",
    "ENABLE_GUARDRAIL": "false", "DBR_DOUBLE_QUOTED_IDENTIFIERS": "false",
    "PRESERVE_DOUBLE_QUOTES_AROUND_IDENTIFIERS_DBR": "false",
    "E6_EXECUTOR_TYPE": "java", "SKIP_COMMENT": "true", "FIX_QUOTE_ESCAPES": "false",
}


async def run(args):
    os.environ.update(FLAGS)
    sys.path.insert(0, str(args.root.resolve()))
    logging.disable(logging.CRITICAL)
    import converter_api as api
    from sqlglot.tokens import USE_RS_TOKENIZER

    assert not USE_RS_TOKENIZER
    assert Path(api.__file__).resolve() == (args.root / "converter_api.py").resolve()
    api.SUBQUERY_TREE = args.variant in ("subquery_tree", "our_branch")
    api.SAMSUNG_TABLEAU_DASHBOARD = "false"
    flags = {"PRETTY_PRINT": False}
    if args.variant == "multidialect":
        flags.update(MULTIDIALECT=True, INNER_DIALECT="databricks")

    async def convert(sql):
        return await api.convert_query(
            query=sql, query_id="four-way-benchmark", from_sql="databricks",
            to_sql="e6", feature_flags=json.dumps(flags),
        )

    for sql in ("SELECT 1", 'SELECT "a" FROM (SELECT `a` FROM t) s'):
        try:
            await convert(sql)
        except Exception:
            pass

    csv.field_size_limit(sys.maxsize)
    rows = []
    corpus_hashes = {}
    for corpus in ("samsung", "condenast"):
        path = args.root / "tests" / corpus / "queries.csv.gz"
        corpus_hashes[corpus] = hashlib.sha256(path.read_bytes()).hexdigest()
        with gzip.open(path, "rt") as stream:
            rows.extend(dict(row, corpus=corpus) for row in csv.DictReader(stream))
    # Fixed randomized order prevents query size from tracking sustained CPU load.
    random.Random(20261014).shuffle(rows)
    if args.limit:
        rows = rows[:args.limit]
    assert len({(r["corpus"], r["query_id"]) for r in rows}) == len(rows)
    samples = {(r["corpus"], r["query_id"]): [] for r in rows}
    started = time.time()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    journal = args.output.with_suffix(".jsonl")
    with journal.open("w") as stream:
        for repeat in range(args.repeats):
            order = rows if repeat % 2 == 0 else list(reversed(rows))
            for index, row in enumerate(order):
                wall = time.perf_counter_ns()
                cpu = time.process_time_ns()
                try:
                    response = await convert(row["sql"])
                    output = response["converted_query"]
                    error = None
                except Exception as exc:
                    error = (type(exc).__name__, str(getattr(exc, "detail", str(exc))))
                cpu_ms = (time.process_time_ns() - cpu) / 1e6
                wall_ms = (time.perf_counter_ns() - wall) / 1e6
                outcome = (
                    {"error": error[0], "detail_sha256": hashlib.sha256(error[1].encode()).hexdigest()}
                    if error else {"sha256": hashlib.sha256(output.encode()).hexdigest()}
                )
                record = dict(corpus=row["corpus"], id=row["query_id"], repeat=repeat,
                              wall_ms=wall_ms, cpu_ms=cpu_ms, **outcome)
                samples[(row["corpus"], row["query_id"])].append(record)
                stream.write(json.dumps(record) + "\n")
                if (index + 1) % 100 == 0 or index == len(order) - 1:
                    stream.flush()
                    print(f"{args.variant}: pass {repeat + 1}/{args.repeats}, {index + 1}/{len(rows)}", flush=True)
    results = []
    for row in rows:
        values = samples[(row["corpus"], row["query_id"])]
        outcomes = [{k: r[k] for k in ("sha256", "error", "detail_sha256") if k in r} for r in values]
        if any(outcome != outcomes[0] for outcome in outcomes):
            raise RuntimeError(f"Unstable outcome: {row['corpus']}/{row['query_id']}")
        results.append(dict(corpus=row["corpus"], id=row["query_id"], group=row["group"],
                            chars=len(row["sql"]), wall_ms=statistics.median(r["wall_ms"] for r in values),
                            cpu_ms=statistics.median(r["cpu_ms"] for r in values), **outcomes[0]))
    source_files = ["converter_api.py", "sqlglot/parser.py", "apis/utils/subquery_tree.py",
                    "apis/utils/lite_parser.py", "apis/utils/multidialect.py"]
    evidence = dict(variant=args.variant, commit=args.commit, repeats=args.repeats,
                    python=sys.version, platform=platform.platform(), machine=platform.machine(),
                    logical_cpus=os.cpu_count(), flags=FLAGS, feature_flags=flags,
                    corpus_hashes=corpus_hashes, started_unix=started, finished_unix=time.time(),
                    source_hashes={f: hashlib.sha256((args.root / f).read_bytes()).hexdigest() for f in source_files},
                    results=results)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--variant", choices=("vanilla", "multidialect", "subquery_tree", "our_branch"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--limit", type=int, default=0, help="Smoke check only; omit for a full run")
    args = parser.parse_args()
    if args.repeats < 1 or args.limit < 0:
        parser.error("repeats must be positive; limit cannot be negative")
    asyncio.run(run(args))
