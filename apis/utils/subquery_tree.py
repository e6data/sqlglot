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

from apis.utils.lite_parser import lite_parse
from apis.utils.multidialect import MARKER, _splice

logger = logging.getLogger(__name__)

@dataclass
class SQNode:
    """A subquery: ``label`` (``"ROOT"`` / ``"S0"`` / ...), verbatim ``raw`` source,
    and ``children`` (directly-nested subqueries, left to right)."""

    label: str
    raw: str
    children: list["SQNode"] = field(default_factory=list)


def build_tree(query: str) -> SQNode:
    """Build the labelled subquery tree. Each subquery's parent is the nearest
    enclosing subquery, else ROOT."""
    _t = time.perf_counter()
    ast = lite_parse(query)
    root = SQNode(label="ROOT", raw=query)

    nodes: dict[int, SQNode] = {}
    for sq in ast.find_all(exp.Subquery):
        if sq.meta.get("raw_sql"):
            nodes[id(sq)] = SQNode(label=f"S{len(nodes)}", raw=sq.meta["raw_sql"])

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


def parse_region(region: str) -> tuple[str, exp.Expression]:
    """A region is Postgres unless it fails to parse as Postgres, then Databricks.
    Returns ``(dialect, tree)`` so the deciding parse is the one that gets transpiled
    (the region is never parsed twice)."""
    _t = time.perf_counter()
    try:
        dialect, tree = "postgres", sqlglot.parse_one(region, read="postgres")
    except (ParseError, TokenError):
        dialect, tree = "databricks", sqlglot.parse_one(region, read="databricks", error_level=None)
    logger.info(
        "[TRANSPILE-TIMING] parse_region=%.1f ms -> %s (%d chars)",
        (time.perf_counter() - _t) * 1000,
        dialect,
        len(region),
    )
    return dialect, tree


def _default_emit(region: str, pretty: bool) -> str:
    """Bare region -> e6 (used when no ``emit`` is given)."""
    dialect, tree = parse_region(region)
    return tree.sql(dialect="e6", from_dialect=dialect, pretty=pretty, copy=False)


def transpile_multidialect(query: str, pretty: bool = True, emit=None) -> str:
    """Walk the tree; transpile each subquery via ``emit(region, pretty)`` and splice
    children back. ``emit`` defaults to :func:`_default_emit`."""
    emit = emit or _default_emit
    _t_total = time.perf_counter()

    def walk(node: SQNode) -> str:
        body, children = _mask(node)
        _t = time.perf_counter()
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

    out = walk(build_tree(query))
    logger.info(
        "[TRANSPILE-TIMING] subquery_tree TOTAL=%.1f ms (%d chars)",
        (time.perf_counter() - _t_total) * 1000,
        len(query),
    )
    return out
