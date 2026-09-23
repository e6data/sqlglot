"""Multidialect transpile via one reused parse (approach B).

No subquery tree: normalize Databricks backticks to double quotes, parse the
whole query once as Postgres, and emit e6 in one generator walk. The e6
generator's HYBRID_MULTIDIALECT hook reparses any subquery/join that carries a
Databricks-only tell (an Anonymous function) from its verbatim source as
Databricks -- so the single Postgres parse is reused for everything else.

Requires the HYBRID_MULTIDIALECT hook, so set the flag before the e6 dialect is
imported (done at the top of this module).

Entry point: ``transpile_multidialect(query, pretty=True)``.
"""

from __future__ import annotations

import os

os.environ.setdefault("HYBRID_MULTIDIALECT", "true")  # before the e6 dialect import

import sqlglot
from sqlglot.tokens import TokenType

from apis.utils.helpers import (
    normalize_unicode_spaces,
    strip_comment,
    extract_large_in_clauses,
    apply_e6_ast_transforms,
    replace_struct_in_query,
    restore_large_in_clauses,
)


def _normalize_backticks(query: str) -> str:
    """Rewrite Databricks backtick identifiers to double quotes so the whole
    query lexes as Postgres. Backticks tokenize as UNKNOWN with correct offsets."""
    chars = list(query)
    for tok in sqlglot.tokenize(query, dialect="postgres"):
        if tok.token_type == TokenType.UNKNOWN and tok.text == "`":
            chars[tok.start] = '"'
    return "".join(chars)


def transpile_multidialect(query: str, pretty: bool = True) -> str:
    """Transpile a mixed Postgres/Databricks query to e6 with one reused parse."""
    s = _normalize_backticks(query)
    s = normalize_unicode_spaces(s)
    s, _ = strip_comment(s)
    s, inr = extract_large_in_clauses(s)
    tree = sqlglot.parse_one(s, read="postgres", error_level=None)   # one parse
    tree = apply_e6_ast_transforms(tree)
    out = tree.sql(dialect="e6", from_dialect="postgres", pretty=pretty,
                   quote_reserved_keywords=True)                     # one walk
    return restore_large_in_clauses(replace_struct_in_query(out), inr)
