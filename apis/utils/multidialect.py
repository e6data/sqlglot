"""Two-pass Postgres-outer / inner-dialect transpile for BI-tool mixed-dialect queries.

Some BI tools (Power BI, Tableau, ThoughtSpot) emit a single SQL string whose **outer
wrapper is Postgres** (``"..."`` = identifier) wrapping **inner native subqueries** in a
different dialect -- Databricks (`` `...` `` = identifier, ``"..."`` = string literal) or
Snowflake. No single dialect can read both correctly. So we transpile the Postgres outer
to the inner dialect and keep the inner subqueries verbatim, producing one uniform
inner-dialect string. The caller then runs the normal ``inner -> e6`` step (where, for a
Databricks inner, ``"x"`` -> ``'x'`` and `` `id` `` -> ``"id"``).

How we find the inner subqueries: try ``postgres -> databricks``; the parse error points
at an inner-dialect subquery Postgres can't read. (The ``write`` target of the detection
transpile is irrelevant -- the failure comes from the Postgres *read* side -- so the same
detection works whether the inner is Databricks or Snowflake.) Pull that subquery out
(replace it with a placeholder), and repeat one error at a time until the outer parses as
plain Postgres.

Raises ``ValueError`` when it can't apply (error not inside a subquery, or no
convergence) so the caller can fall back to the old path.

WORKED EXAMPLE (full flow for ``pg_outer_to_inner``, inner = databricks)::

    input              : SELECT "a" FROM (SELECT `x` FROM `t`) "s"
                         (the parenthesized part is the inner Databricks subquery)
    1. _error_offset   -> 26   (postgres can't read the backtick `x`)
    2. _subquery_span  -> (16, 36) == "(SELECT `x` FROM `t`)"
       cut into marker : SELECT "a" FROM (SELECT NULL AS __E6_INNER_0__) "s"
                         raw["__E6_INNER_0__"] = "(SELECT `x` FROM `t`)"
    3. loop again      : _error_offset -> None  (residual is clean Postgres)
    4. PASS 1 pg->dbr  : SELECT `a` FROM (SELECT NULL AS __E6_INNER_0__) AS `s`
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
MARKER = "__E6_INNER_{}__"
MAX_ROUNDS = 64


def _error_offset(text):
    """Return the char offset of the first ``postgres -> databricks`` error, or None
    if the query transpiles cleanly.

    Example: for ``SELECT "a" FROM (SELECT `x` FROM `t`) "s"`` Postgres chokes on the
    backtick, so this returns ``26`` (the index of that `` ` ``). For a query with no
    inner-dialect-only syntax it returns ``None``.
    """
    # Try the full postgres->databricks transpile with errors raised. If it succeeds the
    # text is now clean Postgres (no inner-dialect-only constructs left) -> signal "done".
    # The "databricks" write target here is arbitrary: the error we key off comes from the
    # Postgres *read*, so this same probe detects a Snowflake inner subquery just as well.
    try:
        sqlglot.transpile(text, read="postgres", write="databricks", error_level=ErrorLevel.RAISE)
        return None
    except (ParseError, TokenError) as e:
        # The parser attaches the failure location (1-based line/col) to errors[0]. Bail
        # out (caller falls back) if there is no usable position to work from.
        errors = getattr(e, "errors", None)
        if not errors or not errors[0].get("line"):
            raise ValueError("Multidialect two-pass: parse error without a position")
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
        text   = SELECT `a` FROM (SELECT NULL AS __E6_INNER_0__) AS `s`
        marker = __E6_INNER_0__
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


def pg_outer_to_inner(query, write="databricks"):
    """Return ``query`` as a uniform ``write``-dialect string: the Postgres outer
    transpiled to ``write`` with the inner subqueries (already in that dialect) kept
    verbatim in place.

    ``write`` is the inner dialect of the query -- "databricks" (default) or "snowflake".
    The caller then runs one ``write -> e6`` pass over the result.

    Example (write="databricks")::
        in : SELECT "a" FROM (SELECT `x` FROM `t`) "s"
        out: SELECT `a` FROM (SELECT `x` FROM `t`) AS `s`
             (outer "a"/"s" -> backticks via PASS 1; inner `x`/`t` kept verbatim)
    """
    # ``work`` is the text we progressively rewrite; ``raw`` maps each placeholder marker
    # to the verbatim inner subquery it replaced; ``rounds`` counts/labels iterations.
    work = query
    raw = {}
    rounds = 0

    # DETECTION LOOP: peel off one failing (inner-dialect) subquery per iteration until the
    # remaining text parses as plain Postgres.
    #   round 0: work = SELECT "a" FROM (SELECT `x` FROM `t`) "s"
    #            -> cut -> SELECT "a" FROM (SELECT NULL AS __E6_INNER_0__) "s"
    #   round 1: _error_offset == None -> stop
    while True:
        offset = _error_offset(work)
        if offset is None:
            break  # no more Postgres parse errors -> outer is clean, detection is done
        # Guard against a query that never converges (e.g. a genuine syntax error).
        if rounds >= MAX_ROUNDS:
            raise ValueError("Multidialect two-pass: did not converge")
        # Map the error location to the enclosing subquery; if the error is not inside a
        # subquery we cannot split it -> bail so the caller falls back.
        span = _subquery_span(work, offset)
        if span is None:
            raise ValueError("Multidialect two-pass: parse error not inside a subquery")
        # Stash the subquery's exact source under a fresh marker, then replace it in-place
        # with a trivial placeholder subquery (valid wherever a subquery may appear, so
        # the residual stays parseable as Postgres).
        #   raw["__E6_INNER_0__"] = "(SELECT `x` FROM `t`)"
        s, e = span
        marker = MARKER.format(rounds)
        raw[marker] = work[s : e + 1]
        work = work[:s] + f"(SELECT NULL AS {marker})" + work[e + 1 :]
        rounds += 1

    # PASS 1: the residual is now clean Postgres -> transpile it to the ``write`` dialect.
    # The placeholder subqueries are valid Postgres, so they survive into that text.
    #   SELECT "a" FROM (SELECT NULL AS __E6_INNER_0__) "s"
    #     ->  SELECT `a` FROM (SELECT NULL AS __E6_INNER_0__) AS `s`   (write=databricks)
    outer = sqlglot.transpile(work, read="postgres", write=write)[0]

    # SPLICE: drop each verbatim inner subquery back over its placeholder. Repeat up
    # to len(raw)+1 times so a subquery that itself contains an earlier marker (nesting)
    # also gets resolved; stop as soon as no markers remain.
    #   ...(SELECT NULL AS __E6_INNER_0__)...  ->  ...(SELECT `x` FROM `t`)...
    for _ in range(len(raw) + 1):
        for marker, raw_sql in raw.items():
            outer = _splice(outer, marker, raw_sql)
        if not any(marker in outer for marker in raw):
            break
    return outer


def split_pg_outer(query):
    """Run the same detection loop as ``pg_outer_to_inner`` but return the split
    ``(outer, raw)`` *without* transpiling anything.

    Why this exists
    ---------------
    ``pg_outer_to_inner`` does the split internally and then immediately transpiles
    the outer pg -> inner and splices the raw subqueries back, returning one inner-dialect
    string. The pg -> e6 FALLBACK in converter_api needs the split pieces *separately*,
    because it transpiles each region with a different dialect (outer as "postgres", inner
    subqueries as the inner dialect) instead of merging them. So this function exposes just
    the split step.

    What it returns
    ---------------
    - ``outer``: ``query`` with every inner subquery replaced by a harmless placeholder
      subquery ``(SELECT NULL AS <marker>)`` -- so ``outer`` now parses as plain Postgres
      (no backticks / inner-dialect-only syntax left).
    - ``raw``: a dict mapping each placeholder ``<marker>`` to the *verbatim* inner
      subquery text it replaced, so the caller can convert and splice it back later.

    Worked example
    --------------
        query = SELECT "a" FROM (SELECT `x` FROM `t`) "s"
        ->
        outer = SELECT "a" FROM (SELECT NULL AS __E6_INNER_0__) "s"
        raw   = {"__E6_INNER_0__": "(SELECT `x` FROM `t`)"}

    Raises ``ValueError`` (so converter_api can fall back / surface the error) if a parse
    error is not inside a subquery, or if the loop fails to converge within MAX_ROUNDS.
    """
    # ``outer`` is the text we progressively rewrite; ``raw`` collects the pulled-out
    # subqueries; ``rounds`` both bounds the loop and names each placeholder uniquely.
    outer, raw, rounds = query, {}, 0
    while True:
        # Try postgres -> databricks on the current text. _error_offset returns None once
        # nothing inner-dialect-only remains (the outer is clean Postgres) -> we're done.
        offset = _error_offset(outer)
        if offset is None:
            return outer, raw
        # Safety valve: a query that never stops erroring (e.g. a genuine syntax error,
        # not an inner-dialect subquery) must not loop forever.
        if rounds >= MAX_ROUNDS:
            raise ValueError("Multidialect two-pass: did not converge")
        # Turn the error position into the span of the subquery that contains it. If the
        # error isn't inside a subquery we can't split the query -> bail to the caller.
        span = _subquery_span(outer, offset)
        if span is None:
            raise ValueError("Multidialect two-pass: parse error not inside a subquery")
        # Record the failing subquery verbatim under a fresh marker, then replace it in
        # ``outer`` with a trivial placeholder subquery. The placeholder is valid wherever
        # a subquery can appear, so ``outer`` stays parseable as Postgres; the next loop
        # iteration then finds the *next* inner subquery (one error at a time).
        s, e = span
        marker = MARKER.format(rounds)
        raw[marker] = outer[s : e + 1]
        outer = outer[:s] + f"(SELECT NULL AS {marker})" + outer[e + 1 :]
        rounds += 1
