"""Stage timings for a deterministic large-query sample and prior regressions.

Run with --root pointing to the checkout being measured. These wrappers add
overhead; use four_way.py without wrappers for end-to-end latency comparisons.
"""
import argparse
import asyncio
import csv
import gc
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from unittest.mock import patch

from four_way import FLAGS


async def run(args):
    os.environ.update(dict(FLAGS, SQLGLOTRS_TOKENIZER="1" if args.tokenizer == "rust" else "0"))
    sys.path.insert(0, str(args.root.resolve()))
    logging.disable(logging.CRITICAL)
    import converter_api as api
    import apis.utils.subquery_tree as regions
    from sqlglot import exp
    from sqlglot.dialects.dialect import Dialect
    from sqlglot.parser import Parser
    from sqlglot.tokens import USE_RS_TOKENIZER

    assert USE_RS_TOKENIZER == (args.tokenizer == "rust"), "Requested tokenizer is unavailable"

    api.SUBQUERY_TREE = True
    csv.field_size_limit(sys.maxsize)
    rows = []
    for corpus in ("samsung", "condenast"):
        with gzip.open(args.root / "tests" / corpus / "queries.csv.gz", "rt") as f:
            rows.extend(dict(row, corpus=corpus) for row in csv.DictReader(f))
    large = sorted((r for r in rows if len(r["sql"]) >= 100000), key=lambda r: len(r["sql"]))[::10]
    regression_ids = {
        "Q611055-704-20260825.102906.681", "Q611055-473-20260825.094937.836",
        "Q611055-432-20260825.093222.917", "Q611055-482-20260825.095221.739",
        "Condenast::a6204f4a38116d46eb2b583a946031b8b3ac4e278f77a9593f2f2bf4475138aa#1",
    }
    selected = large + [r for r in rows if r["query_id"] in regression_ids]
    assert len(selected) == 16
    events = []
    patches = []

    def instrument(obj, name, label, size=None):
        original = getattr(obj, name)

        def wrapped(*pos, **kw):
            event = {"stage": label(pos) if callable(label) else label}
            if size:
                event.update(size(pos))
            started = time.perf_counter_ns()
            try:
                result = original(*pos, **kw)
                if name == "mask":
                    event["reused"] = result is not None
                return result
            except Exception:
                event["failed"] = True
                raise
            finally:
                event["ms"] = (time.perf_counter_ns() - started) / 1e6
                events.append(event)

        patches.append(patch.object(obj, name, wrapped))

    instrument(Dialect, "tokenize", lambda p: "tokenize/" + type(p[0]).__name__,
               lambda p: {"chars": len(p[1])})
    instrument(Parser, "parse", lambda p: "parse/" + type(p[0]).__qualname__,
               lambda p: {"tokens": len(p[1])})
    instrument(regions, "build_tree", "discovery")
    instrument(regions.RegionTokens, "mask", "token_reuse")
    instrument(regions, "_mask", "mask")
    instrument(regions, "_splice", "splice")
    for name in ("normalize_unicode_spaces", "strip_comment", "extract_large_in_clauses",
                 "apply_e6_ast_transforms", "replace_struct_in_query", "restore_large_in_clauses"):
        instrument(api, name, name)
    instrument(exp.Expression, "sql", "generate")

    async def convert(sql):
        return await api.convert_query(query=sql, query_id="large-profile",
            from_sql="databricks", to_sql="e6", feature_flags='{"PRETTY_PRINT":false}')

    await convert("SELECT 1")
    records = []
    gc_started = {}

    def gc_event(phase, info):
        generation = info["generation"]
        if phase == "start":
            gc_started[generation] = time.perf_counter_ns()
        elif generation in gc_started:
            events.append({"stage": f"gc/{generation}",
                "ms": (time.perf_counter_ns() - gc_started.pop(generation)) / 1e6})

    try:
        gc.callbacks.append(gc_event)
        for p in patches:
            p.start()
        for row in selected:
            events.clear()
            started = time.perf_counter_ns()
            try:
                out = await convert(row["sql"])
                outcome = {"sha256": hashlib.sha256(out["converted_query"].encode()).hexdigest()}
            except Exception as exc:
                outcome = {"error": type(exc).__name__}
            records.append(dict(id=row["query_id"], corpus=row["corpus"], chars=len(row["sql"]),
                wall_ms=(time.perf_counter_ns() - started) / 1e6, events=list(events), **outcome))
    finally:
        gc.callbacks.remove(gc_event)
        for p in reversed(patches):
            p.stop()
    args.output.write_text(json.dumps({"root": str(args.root), "tokenizer": args.tokenizer,
        "records": records}, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tokenizer", choices=("python", "rust"), default="python")
    asyncio.run(run(parser.parse_args()))
