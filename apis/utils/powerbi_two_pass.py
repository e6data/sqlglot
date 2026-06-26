"""Two-pass Postgres -> Databricks transpile for Power BI mixed-dialect queries.

A Power BI query has a Postgres outer wrapper (``"..."`` = identifier) wrapping inner
Databricks subqueries (`` `...` `` = identifier, ``"..."`` = string literal). No single
dialect can read both correctly. So we transpile the Postgres outer to Databricks and
keep the inner Databricks subqueries verbatim, producing one uniform Databricks string.
The caller then runs the normal ``databricks -> e6`` step (where inner ``"x"`` -> ``'x'``
and `` `id` `` -> ``"id"``).

How we find the inner subqueries: try ``postgres -> databricks``; the parse error points
at a Databricks subquery Postgres can't read. Pull that subquery out (replace it with a
placeholder), and repeat one error at a time until the outer parses as plain Postgres.

Raises ``ValueError`` when it can't apply (error not inside a subquery, or no
convergence) so the caller can fall back to the old path.
"""

import sqlglot
from sqlglot import tokenize
from sqlglot.errors import ErrorLevel, ParseError, TokenError
from sqlglot.tokens import TokenType

# A "(" opens a subquery only when the next token starts a query. This skips
# function-call, IN-list, grouping and window OVER(...) parens.
SUBQUERY_OPENERS = {TokenType.SELECT, TokenType.WITH, TokenType.VALUES, TokenType.TABLE}

MARKER = "__E6PBI_INNER_{}__"
MAX_ROUNDS = 64


def _error_offset(text):
    """Return the char offset of the first ``postgres -> databricks`` error, or None
    if the query transpiles cleanly."""
    try:
        sqlglot.transpile(text, read="postgres", write="databricks", error_level=ErrorLevel.RAISE)
        return None
    except (ParseError, TokenError) as e:
        errors = getattr(e, "errors", None)
        if not errors or not errors[0].get("line"):
            raise ValueError("Power BI two-pass: parse error without a position")
        err = errors[0]
        lines = text.splitlines(keepends=True)
        offset = sum(len(ln) for ln in lines[: err["line"] - 1]) + (err["col"] - 1)
        return max(0, min(offset, len(text) - 1))


def _subquery_span(text, offset):
    """Find the innermost ``( ... )`` subquery that contains ``offset``.

    Balances parens on the token stream (so parens inside strings/comments don't count)
    and only accepts a "(" that actually opens a subquery. Returns the inclusive
    (start, end) char span, or None if ``offset`` is not inside a subquery.
    """
    toks = tokenize(text, dialect="postgres")
    start = [i for i, tok in enumerate(toks) if tok.start <= offset]
    if not start:
        return None

    depth = 0
    for i in range(start[-1], -1, -1):
        kind = toks[i].token_type
        if kind == TokenType.R_PAREN:
            depth += 1
        elif kind == TokenType.L_PAREN:
            if depth > 0:
                depth -= 1
            elif (toks[i + 1].token_type if i + 1 < len(toks) else None) in SUBQUERY_OPENERS:
                # forward-balance to this "(" 's matching ")"
                d = 0
                for j in range(i, len(toks)):
                    if toks[j].token_type == TokenType.L_PAREN:
                        d += 1
                    elif toks[j].token_type == TokenType.R_PAREN:
                        d -= 1
                        if d == 0:
                            return toks[i].start, toks[j].end
                return None
            # else: a non-subquery "(" -- keep looking further out
    return None


def _splice(text, marker, raw):
    """Put ``raw`` back where its placeholder subquery ``(SELECT NULL AS marker)`` sits.
    The placeholder has no inner parens, so its "(" / ")" are just the ones around the
    marker."""
    p = text.find(marker)
    if p == -1:
        return text
    open_paren = text.rfind("(", 0, p)
    close_paren = text.find(")", p)
    return text[:open_paren] + raw + text[close_paren + 1 :]


def pg_outer_to_databricks(query):
    """Return ``query`` as a uniform Databricks string: the Postgres outer transpiled to
    Databricks with the inner Databricks subqueries kept verbatim in place."""
    work = query
    raw = {}
    rounds = 0
    while True:
        offset = _error_offset(work)
        if offset is None:
            break  # outer now parses as plain Postgres
        if rounds >= MAX_ROUNDS:
            raise ValueError("Power BI two-pass: did not converge")
        span = _subquery_span(work, offset)
        if span is None:
            raise ValueError("Power BI two-pass: parse error not inside a subquery")
        s, e = span
        marker = MARKER.format(rounds)
        raw[marker] = work[s : e + 1]
        work = work[:s] + f"(SELECT NULL AS {marker})" + work[e + 1 :]
        rounds += 1

    # PASS 1: transpile the clean Postgres outer to Databricks (placeholders ride along).
    outer = sqlglot.transpile(work, read="postgres", write="databricks")[0]

    # Put the raw Databricks subqueries back (repeat in case one nests another).
    for _ in range(len(raw) + 1):
        for marker, raw_sql in raw.items():
            outer = _splice(outer, marker, raw_sql)
        if not any(marker in outer for marker in raw):
            break
    return outer
