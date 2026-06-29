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

WORKED EXAMPLE (full flow for ``pg_outer_to_databricks``)::

    input              : SELECT "a" FROM (SELECT `x` FROM `t`) "s"
                         (the parenthesized part is the inner Databricks subquery)
    1. _error_offset   -> 26   (postgres can't read the backtick `x`)
    2. _subquery_span  -> (16, 36) == "(SELECT `x` FROM `t`)"
       cut into marker : SELECT "a" FROM (SELECT NULL AS __E6PBI_INNER_0__) "s"
                         raw["__E6PBI_INNER_0__"] = "(SELECT `x` FROM `t`)"
    3. loop again      : _error_offset -> None  (residual is clean Postgres)
    4. PASS 1 pg->dbr  : SELECT `a` FROM (SELECT NULL AS __E6PBI_INNER_0__) AS `s`
    5. splice raw back : SELECT `a` FROM (SELECT `x` FROM `t`) AS `s`     <-- returned
       caller dbr->e6  : SELECT "a" FROM (SELECT "x" FROM "t") AS "s"
"""

# sqlglot.transpile/tokenize drive both passes; the error/token types let us detect the
# Postgres parse failure and walk the raw token stream by character offset.
import sqlglot
from sqlglot import tokenize
from sqlglot.errors import ErrorLevel, ParseError, TokenError
from sqlglot.tokens import TokenType

# Token types that may immediately follow a "(" that truly opens a subquery. Used to tell
# a subquery "(" apart from a function-call / IN-list / grouping / window OVER(...) "(".
#   e.g.  "(SELECT ..."  -> opener (SELECT in set)         -> a subquery
#         "(CONCAT(...)" -> next token is a column/func    -> NOT a subquery
SUBQUERY_OPENERS = {TokenType.SELECT, TokenType.WITH, TokenType.VALUES, TokenType.TABLE}

# Placeholder name (formatted with the round number) swapped in for each pulled-out
# subquery; unlikely to collide with a real identifier. MAX_ROUNDS bounds the loop so a
# query that never converges fails fast instead of spinning.
MARKER = "__E6PBI_INNER_{}__"
MAX_ROUNDS = 64


def _error_offset(text):
    """Return the char offset of the first ``postgres -> databricks`` error, or None
    if the query transpiles cleanly.

    Example: for ``SELECT "a" FROM (SELECT `x` FROM `t`) "s"`` Postgres chokes on the
    backtick, so this returns ``26`` (the index of that `` ` ``). For a query with no
    Databricks-only syntax it returns ``None``.
    """
    # Try the full postgres->databricks transpile with errors raised. If it succeeds the
    # text is now clean Postgres (no Databricks-only constructs left) -> signal "done".
    try:
        sqlglot.transpile(text, read="postgres", write="databricks", error_level=ErrorLevel.RAISE)
        return None
    except (ParseError, TokenError) as e:
        # The parser attaches the failure location (1-based line/col) to errors[0]. Bail
        # out (caller falls back) if there is no usable position to work from.
        errors = getattr(e, "errors", None)
        if not errors or not errors[0].get("line"):
            raise ValueError("Power BI two-pass: parse error without a position")
        err = errors[0]
        # Convert (line, col) into an absolute character offset into ``text``: sum the
        # lengths of all earlier lines, then add the column. Clamp into range for safety.
        #   e.g. line=1, col=27  ->  0 (no earlier lines) + 26  ->  offset 26
        lines = text.splitlines(keepends=True)
        offset = sum(len(ln) for ln in lines[: err["line"] - 1]) + (err["col"] - 1)
        return max(0, min(offset, len(text) - 1))


def _subquery_span(text, offset):
    """Find the innermost ``( ... )`` subquery that contains ``offset``.

    Balances parens on the token stream (so parens inside strings/comments don't count)
    and only accepts a "(" that actually opens a subquery. Returns the inclusive
    (start, end) char span, or None if ``offset`` is not inside a subquery.

    Example 1 -- simple::
        text   = SELECT "a" FROM (SELECT `x` FROM `t`) "s"
        offset = 26  (the backtick)        ->  returns (16, 36) == "(SELECT `x` FROM `t`)"

    Example 2 -- a backtick inside a function call must NOT grab the function paren::
        text   = SELECT z FROM (SELECT CONCAT(`x`, 'y') AS c FROM `t`) s
        offset = 30  (inside CONCAT(...))  ->  returns the whole
                 "(SELECT CONCAT(`x`, 'y') AS c FROM `t`)", skipping CONCAT's own "(".
    """
    # Tokenize as Postgres so each "(" / ")" is a real paren token (parens inside string
    # literals or comments become part of a single STRING/comment token and are ignored).
    toks = tokenize(text, dialect="postgres")
    # Locate the token at/just before the error offset -- our starting point to scan out.
    start = [i for i, tok in enumerate(toks) if tok.start <= offset]
    if not start:
        return None

    # Walk LEFT from the error, tracking paren depth, to find the nearest enclosing "(".
    # depth counts ")"s seen so each matching "(" cancels one; the first "(" reached at
    # depth 0 is an enclosing opener at the error's level.
    #   In example 2, scanning left from `x` first meets CONCAT's "(" (depth 0) but its
    #   next token is `x` (not an opener), so we keep going and stop at "(SELECT ...".
    depth = 0
    for i in range(start[-1], -1, -1):
        kind = toks[i].token_type
        if kind == TokenType.R_PAREN:
            depth += 1
        elif kind == TokenType.L_PAREN:
            if depth > 0:
                # This "(" only closes a ")" we already passed -- not enclosing. Skip it.
                depth -= 1
            elif (toks[i + 1].token_type if i + 1 < len(toks) else None) in SUBQUERY_OPENERS:
                # Enclosing "(" whose next token starts a query -> a real subquery.
                # Forward-balance from here to its matching ")" and return the char span.
                #   "(SELECT `x` FROM `t`)"  ->  (start_of_"(", end_of_matching_")")
                d = 0
                for j in range(i, len(toks)):
                    if toks[j].token_type == TokenType.L_PAREN:
                        d += 1
                    elif toks[j].token_type == TokenType.R_PAREN:
                        d -= 1
                        if d == 0:
                            return toks[i].start, toks[j].end
                return None  # opening "(" never closed -> give up
            # else: enclosing "(" that is a function/grouping/IN-list, not a subquery --
            # leave depth at 0 and keep scanning further left for the real subquery "(".
    return None


def _splice(text, marker, raw):
    """Put ``raw`` back where its placeholder subquery ``(SELECT NULL AS marker)`` sits.
    The placeholder has no inner parens, so its "(" / ")" are just the ones around the
    marker.

    Example::
        text   = SELECT `a` FROM (SELECT NULL AS __E6PBI_INNER_0__) AS `s`
        marker = __E6PBI_INNER_0__
        raw    = (SELECT `x` FROM `t`)
        result = SELECT `a` FROM (SELECT `x` FROM `t`) AS `s`
    """
    # Find the marker; if it is gone (already spliced) there is nothing to do.
    p = text.find(marker)
    if p == -1:
        return text
    # The placeholder body has no nested parens, so the nearest "(" before the marker and
    # the nearest ")" after it delimit exactly the placeholder subquery. Swap in ``raw``.
    open_paren = text.rfind("(", 0, p)
    close_paren = text.find(")", p)
    return text[:open_paren] + raw + text[close_paren + 1 :]


def pg_outer_to_databricks(query):
    """Return ``query`` as a uniform Databricks string: the Postgres outer transpiled to
    Databricks with the inner Databricks subqueries kept verbatim in place.

    Example::
        in : SELECT "a" FROM (SELECT `x` FROM `t`) "s"
        out: SELECT `a` FROM (SELECT `x` FROM `t`) AS `s`
             (outer "a"/"s" -> backticks via PASS 1; inner `x`/`t` kept verbatim)
    """
    # ``work`` is the text we progressively rewrite; ``raw`` maps each placeholder marker
    # to the verbatim Databricks subquery it replaced; ``rounds`` counts/labels iterations.
    work = query
    raw = {}
    rounds = 0

    # DETECTION LOOP: peel off one failing (Databricks) subquery per iteration until the
    # remaining text parses as plain Postgres.
    #   round 0: work = SELECT "a" FROM (SELECT `x` FROM `t`) "s"
    #            -> cut -> SELECT "a" FROM (SELECT NULL AS __E6PBI_INNER_0__) "s"
    #   round 1: _error_offset == None -> stop
    while True:
        offset = _error_offset(work)
        if offset is None:
            break  # no more Postgres parse errors -> outer is clean, detection is done
        # Guard against a query that never converges (e.g. a genuine syntax error).
        if rounds >= MAX_ROUNDS:
            raise ValueError("Power BI two-pass: did not converge")
        # Map the error location to the enclosing subquery; if the error is not inside a
        # subquery we cannot split it -> bail so the caller falls back.
        span = _subquery_span(work, offset)
        if span is None:
            raise ValueError("Power BI two-pass: parse error not inside a subquery")
        # Stash the subquery's exact source under a fresh marker, then replace it in-place
        # with a trivial placeholder subquery (valid wherever a subquery may appear, so
        # the residual stays parseable as Postgres).
        #   raw["__E6PBI_INNER_0__"] = "(SELECT `x` FROM `t`)"
        s, e = span
        marker = MARKER.format(rounds)
        raw[marker] = work[s : e + 1]
        work = work[:s] + f"(SELECT NULL AS {marker})" + work[e + 1 :]
        rounds += 1

    # PASS 1: the residual is now clean Postgres -> transpile it to Databricks. The
    # placeholder subqueries are valid Postgres, so they survive into the Databricks text.
    #   SELECT "a" FROM (SELECT NULL AS __E6PBI_INNER_0__) "s"
    #     ->  SELECT `a` FROM (SELECT NULL AS __E6PBI_INNER_0__) AS `s`
    outer = sqlglot.transpile(work, read="postgres", write="databricks")[0]

    # SPLICE: drop each verbatim Databricks subquery back over its placeholder. Repeat up
    # to len(raw)+1 times so a subquery that itself contains an earlier marker (nesting)
    # also gets resolved; stop as soon as no markers remain.
    #   ...(SELECT NULL AS __E6PBI_INNER_0__)...  ->  ...(SELECT `x` FROM `t`)...
    for _ in range(len(raw) + 1):
        for marker, raw_sql in raw.items():
            outer = _splice(outer, marker, raw_sql)
        if not any(marker in outer for marker in raw):
            break
    return outer