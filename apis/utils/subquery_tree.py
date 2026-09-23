"""Multidialect transpile via a subquery tree.

Parse the query once (Databricks, errors ignored) and reuse sqlglot's own parsed
hierarchy: every ``exp.Subquery`` it captures becomes a tree node holding that
subquery's verbatim source (``meta["raw_sql"]``), nested by ``find_ancestor``.
Then walk the tree bottom-up and transpile each subquery to e6, trying Postgres
first and switching to Databricks for that subquery if the Postgres parse fails.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError, TokenError

from apis.utils.multidialect import MARKER, _splice


@dataclass
class SQNode:
    """A subquery in the tree. ROOT's ``raw`` is the whole query."""

    raw: str
    children: list["SQNode"] = field(default_factory=list)


def build_tree(query: str) -> SQNode:
    """Abstract the subqueries + ROOT from sqlglot's parsed hierarchy.

    One sqlglot parse (Databricks; ``error_level=IGNORE`` keeps it best-effort so a
    syntax it can't fully read doesn't abort the hierarchy). Each ``exp.Subquery``
    -> a node holding its ``meta["raw_sql"]``; nesting comes straight from
    ``find_ancestor``. ROOT's ``raw`` is the whole query.
    """
    ast = sqlglot.parse_one(query, read="databricks", error_level=ErrorLevel.IGNORE)
    root = SQNode(raw=query)
    nodes: dict[int, SQNode] = {
        id(sq): SQNode(raw=sq.meta["raw_sql"])
        for sq in ast.find_all(exp.Subquery)
        if sq.meta.get("raw_sql")
    }
    for sq in ast.find_all(exp.Subquery):
        node = nodes.get(id(sq))
        if node is None:
            continue
        anc = sq.find_ancestor(exp.Subquery)
        while anc is not None and id(anc) not in nodes:
            anc = anc.find_ancestor(exp.Subquery)
        (nodes[id(anc)] if anc is not None else root).children.append(node)
    return root


def transpile_multidialect(query: str, pretty: bool = True) -> str:
    """Walk the subquery tree bottom-up; per subquery try Postgres, switch to
    Databricks on any parse failure."""

    def walk(node: SQNode) -> str:
        body = node.raw
        children: dict[str, str] = {}
        for k, child in enumerate(node.children):
            marker = MARKER.format(k)
            body = body.replace(child.raw, f"(SELECT NULL AS {marker})", 1)
            children[marker] = walk(child)

        try:
            dialect = "postgres"
            tree = sqlglot.parse_one(body, read=dialect)
        except (ParseError, TokenError):
            dialect = "databricks"
            tree = sqlglot.parse_one(body, read=dialect)

        e6 = tree.sql(dialect="e6", from_dialect=dialect, pretty=pretty)
        for marker, child_e6 in children.items():
            e6 = _splice(e6, marker, child_e6)
        return e6

    return walk(build_tree(query))
