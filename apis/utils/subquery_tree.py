"""A dialect-agnostic subquery tree for multidialect transpile.

One lightweight parse (:mod:`apis.utils.lite_parser`) locates every subquery and its
verbatim source. Each subquery is a labelled node holding that source (``raw``);
nesting comes from the parse hierarchy; the whole query is the ROOT.

- :func:`reproduce` rebuilds the exact query from the tree (``reproduce(build_tree(q))
  == q``), so nothing is lost going through the tree.
- :func:`transpile_multidialect` walks the tree and transpiles each subquery to e6,
  splicing the results back. How a region becomes e6 is the ``emit`` hook, so the
  converter can plug its own pipeline in without this module importing it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Token

from apis.utils.lite_parser import lite_parse
from apis.utils.multidialect import MARKER, _splice
from apis.utils.region_tokens import RegionTokens

logger = logging.getLogger(__name__)

@dataclass
class SQNode:
    """A subquery: ``label`` (``"ROOT"`` / ``"S0"`` / ...), verbatim ``raw`` source,
    and ``children`` (directly-nested subqueries, left to right)."""

    label: str
    raw: str
    children: list["SQNode"] = field(default_factory=list)
    start: int = 0
    end: int = 0


def build_tree(query: str, tokens=None) -> SQNode:
    """Build the labelled subquery tree. Each subquery's parent is the nearest
    enclosing subquery, else ROOT."""
    _t = time.perf_counter()
    ast = lite_parse(query, tokens=tokens)
    root = SQNode(label="ROOT", raw=query, end=len(query))

    nodes: dict[int, SQNode] = {}
    for sq in ast.find_all(exp.Subquery):
        if sq.meta.get("raw_sql"):
            nodes[id(sq)] = SQNode(
                label=f"S{len(nodes)}", raw=sq.meta["raw_sql"],
                start=sq.meta["raw_start"], end=sq.meta["raw_end"],
            )

    for sq in ast.find_all(exp.Subquery):
        node = nodes.get(id(sq))
        if node is None:
            continue
        anc = sq.find_ancestor(exp.Subquery)
        while anc is not None and id(anc) not in nodes:
            anc = anc.find_ancestor(exp.Subquery)
        (nodes[id(anc)] if anc is not None else root).children.append(node)

    logger.info(
        "[TRANSPILE-TIMING] build_tree=%.1f ms (%d subquery node(s))",
        (time.perf_counter() - _t) * 1000,
        len(nodes),
    )
    return root


def _mask(node: SQNode) -> tuple[str, dict[str, SQNode]]:
    """``node.raw`` with each direct child swapped for a ``(SELECT NULL AS marker)``
    placeholder. Returns the masked body and the ``marker -> child`` map."""
    body = node.raw
    children: dict[str, SQNode] = {}
    for k, child in enumerate(node.children):
        marker = MARKER.format(k)
        body = body.replace(child.raw, f"(SELECT NULL AS {marker})", 1)
        children[marker] = child
    return body, children


def reproduce(node: SQNode) -> str:
    """Rebuild the exact source of ``node`` from the tree (round-trip proof)."""
    body, children = _mask(node)
    for marker, child in children.items():
        body = _splice(body, marker, reproduce(child))
    return body


def parse_region(region: str, tokens=None) -> tuple[str, exp.Expression]:
    """A region is Postgres unless it fails to parse as Postgres, then Databricks.
    Returns ``(dialect, tree)`` so the deciding parse is the one that gets transpiled
    (the region is never parsed twice)."""
    _t = time.perf_counter()
    try:
        if tokens is None:
            tree = sqlglot.parse_one(region, read="postgres")
        else:
            trees = Dialect.get_or_raise("postgres").parser().parse(tokens, region)
            tree = trees[0] if trees else None
            if tree is None:
                raise ParseError(f"No expression was parsed from '{region}'")
        dialect = "postgres"
    except (ParseError, TokenError):
        dialect, tree = "databricks", sqlglot.parse_one(region, read="databricks", error_level=None)
    logger.info(
        "[TRANSPILE-TIMING] parse_region=%.1f ms -> %s (%d chars)",
        (time.perf_counter() - _t) * 1000,
        dialect,
        len(region),
    )
    return dialect, tree


def _default_emit(region: str, pretty: bool, tokens=None) -> str:
    """Bare region -> e6 (used when no ``emit`` is given)."""
    dialect, tree = parse_region(region, tokens=tokens)
    return tree.sql(dialect="e6", from_dialect=dialect, pretty=pretty, copy=False)


def transpile_multidialect(query: str, pretty: bool = True, emit=None, emit_tokens=None) -> str:
    """Walk the tree; transpile each subquery via ``emit(region, pretty)`` and splice
    children back. ``emit_tokens(region, pretty, tokens)`` opts into token reuse;
    ``tokens`` is None when the region needs normal dialect tokenization. Existing
    two-argument emit hooks remain supported. The default emitter reuses tokens.
    """
    if emit is None and emit_tokens is None:
        emit_tokens = _default_emit
    _t_total = time.perf_counter()
    tokens = Dialect().tokenize(query)
    logger.info(
        "[TRANSPILE-TIMING] discovery tokenize=%.1f ms (%d tokens)",
        (time.perf_counter() - _t_total) * 1000,
        len(tokens),
    )
    root = build_tree(query, tokens=tokens)
    # Native token positions are immutable. Copying them into Python tokens costs
    # more than native retokenization in the measured workloads.
    retained = (
        RegionTokens(query, tokens)
        if emit_tokens is not None and (not tokens or isinstance(tokens[0], Token))
        else None
    )

    def walk(node: SQNode) -> str:
        body, children = _mask(node)
        _t = time.perf_counter()
        if emit_tokens is not None:
            e6 = emit_tokens(body, pretty, retained.mask(node, body) if retained else None)
        else:
            e6 = emit(body, pretty)
        logger.info(
            "[TRANSPILE-TIMING] region %s: emit=%.1f ms (%d chars, %d child(ren))",
            node.label,
            (time.perf_counter() - _t) * 1000,
            len(body),
            len(children),
        )
        for marker, child in children.items():
            child_e6 = walk(child)
            _t = time.perf_counter()
            e6 = _splice(e6, marker, child_e6)
            logger.info(
                "[TRANSPILE-TIMING] region %s: splice %s=%.1f ms",
                node.label,
                child.label,
                (time.perf_counter() - _t) * 1000,
            )
        return e6

    out = walk(root)
    logger.info(
        "[TRANSPILE-TIMING] subquery_tree TOTAL=%.1f ms (%d chars)",
        (time.perf_counter() - _t_total) * 1000,
        len(query),
    )
    return out
