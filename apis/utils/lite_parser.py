"""A lightweight sqlglot parser that finds subqueries without the cost of a full parse.

Why this exists
---------------
sqlglot's parser already does the hard work we want: when it opens a ``( ... )``
subquery it records that subquery's verbatim source on the node --
``subquery.meta["raw_sql"] = self.sql[lparen.start : rparen.end + 1]``
(``parser.py`` ``_parse_select_query``, line 3371). So a single parse yields every
subquery, its exact source text, and the nesting.

The only problem is cost. Reading a Postgres-outer BI query as Databricks makes the
parser grind through a huge SELECT list and WHERE full of cross-dialect expressions
(~1000 internal type sub-parses). But none of that work helps us: a dialect-boundary
subquery only ever lives in FROM / JOIN, never in the projection list or WHERE.

What LiteParser does
--------------------
Keep the parts that walk table structure (FROM, JOIN, LATERAL -- these descend into
subqueries and record ``raw_sql``); skip the parts that only parse value expressions
(the SELECT list, and every trailing modifier: WHERE, GROUP BY, HAVING, QUALIFY,
WINDOW, ORDER BY, LIMIT ...). "Skip" means: advance over the clause's tokens without
building expressions from them. Two overrides cover all of it.

Dialect-agnostic
----------------
Locating subqueries is pure structure (paren-balance + a query-opener keyword), so
this inherits the **base** ``Parser`` with the generic ``Dialect`` -- no dialect
assumptions. Backticks, ``"..."`` identifiers, ``$$`` etc. that a specific dialect
would interpret just tokenize as harmless ``UNKNOWN`` tokens here, with correct
offsets, so paren-matching and each subquery's verbatim ``raw_sql`` stay exact. Each
subquery is reparsed in its real dialect later, during transpile -- not here.
"""

from __future__ import annotations

import logging
import time
import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.parser import Parser
from sqlglot.tokens import TokenType

logger = logging.getLogger(__name__)

class LiteParser(Parser):
    """The base sqlglot parser with the value-expression clauses skipped. It still
    walks FROM/JOIN/LATERAL, so sqlglot records each subquery's ``meta["raw_sql"]`` --
    we just don't pay to parse the value expressions (the thrash)."""

    # At paren-depth 0, any of these ends the region we are skipping. A closing ")"
    # (the enclosing subquery) and end-of-input also end it; those are handled below.
    _SET_OPS = frozenset(
        {TokenType.UNION, TokenType.EXCEPT, TokenType.INTERSECT, TokenType.SEMICOLON}
    )

    def _skip(self, extra: t.AbstractSet[TokenType] = frozenset()) -> None:
        """Advance over tokens until a clause boundary at paren-depth 0. Paren-aware,
        so parentheses inside the skipped clause (a scalar subquery, a function call,
        a CASE) never make us stop early or run past the region."""
        depth = 0
        while self._curr:
            tt = self._curr.token_type
            if tt == TokenType.L_PAREN:
                depth += 1
            elif tt == TokenType.R_PAREN:
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and (tt in self._SET_OPS or tt in extra):
                break
            self._advance()

    # -- override 1: the SELECT list (skip up to FROM) -------------------------
    def _parse_projections(self) -> t.List[exp.Expression]:
        self._skip(extra={TokenType.FROM})
        return [exp.Star()]

    # -- override 2: everything after FROM's joins/laterals --------------------
    def _parse_query_modifiers(
        self, this: t.Optional[exp.Expression]
    ) -> t.Optional[exp.Expression]:
        if isinstance(this, self.MODIFIABLES):
            # Keep joins and laterals: they descend into subqueries and record raw_sql.
            for join in self._parse_joins():
                this.append("joins", join)
            for lateral in iter(self._parse_lateral, None):
                this.append("laterals", lateral)
            # Skip WHERE / GROUP BY / HAVING / QUALIFY / WINDOW / ORDER BY / LIMIT ...
            # up to the end of this query (a ")" of the enclosing subquery, a set op,
            # or end-of-input). None of them hold a dialect-boundary subquery.
            self._skip()
        return this


def lite_parse(query: str) -> exp.Expression:
    """Parse ``query`` with :class:`LiteParser` and return the AST. Every subquery in
    it carries ``meta["raw_sql"]`` (its verbatim source)."""
    dialect = Dialect()
    _t = time.perf_counter()
    tokens = dialect.tokenize(query)
    _tokenize_ms = (time.perf_counter() - _t) * 1000
    _t = time.perf_counter()
    ast = LiteParser(dialect=dialect).parse(tokens, query)[0]
    logger.info(
        "[TRANSPILE-TIMING] lite_parse: tokenize=%.1f ms (%d tokens), parse=%.1f ms (%d chars)",
        _tokenize_ms,
        len(tokens),
        (time.perf_counter() - _t) * 1000,
        len(query),
    )
    return ast


def iter_subqueries(ast: exp.Expression) -> t.Iterator[exp.Subquery]:
    """Yield the subqueries LiteParser captured (those with a recorded ``raw_sql``)."""
    for sq in ast.find_all(exp.Subquery):
        if sq.meta.get("raw_sql"):
            yield sq
