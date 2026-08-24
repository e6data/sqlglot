"""Two-pass Postgres-outer / inner-dialect transpile for BI-tool mixed-dialect queries.

Some BI tools (Power BI, Tableau, ThoughtSpot) emit a single SQL string whose **outer
wrapper is Postgres** (``"..."`` = identifier) wrapping **inner native subqueries** in a
different dialect -- Databricks (`` `...` `` = identifier, ``"..."`` = string literal) or
Snowflake. No single dialect can read both correctly. So we transpile the Postgres outer
to the inner dialect and keep the inner subqueries verbatim, producing one uniform
inner-dialect string. The caller then runs the normal ``inner -> e6`` step (where, for a
Databricks inner, ``"x"`` -> ``'x'`` and `` `id` `` -> ``"id"``).

How we find the inner subqueries: a Databricks inner subquery carries backtick identifiers
(Postgres emits each as an UNKNOWN "`" token), so one tokenize pass pulls out every backtick
subquery, reusing ``_subquery_span`` to find each one's enclosing subquery. A fallback loop
then catches anything the backtick scan can't see (a non-backtick inner, or a Snowflake
inner): try ``postgres -> databricks``; the parse error points at an inner-dialect subquery
Postgres can't read; pull it out and repeat one error at a time until the outer parses as
plain Postgres. (The ``write`` target is irrelevant -- the failure comes from the Postgres
*read* side -- so the same detection works whether the inner is Databricks or Snowflake.)

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


def _subquery_span(text, offset, toks=None):
    """Find the innermost ``( ... )`` subquery that contains ``offset``.

    Balances parens on the token stream (so parens inside strings/comments don't count)
    and only accepts a "(" that actually opens a subquery. Returns the inclusive
    (start, end) char span, or None if ``offset`` is not inside a subquery. Pass ``toks``
    (a pre-tokenized ``text``) to reuse one tokenization across many calls.

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
    if toks is None:
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


# pg_outer_to_inner (removed / dead code): it merged the mixed query into one
# <inner_dialect> string (pg outer -> databricks) before a single -> e6 pass. That merge
# mis-read Postgres constructs (numeric TRUNC -> date-truncation crash; literal SPLIT
# delimiter -> regex \Q..\E). Superseded by the split-per-region path in converter_api:
# split_pg_outer() + _region_to_e6() + _splice() — each region transpiled to e6 in its
# own dialect, no inner-dialect merge.


def split_pg_outer(query):
    """Split ``query`` into a plain-Postgres ``outer`` and a ``raw`` map of the inner
    subqueries it replaced, *without* transpiling anything.

    Why this exists
    ---------------
    The pg -> e6 path in converter_api transpiles each region with a different dialect
    (outer as "postgres", inner subqueries as the inner dialect) instead of merging them,
    so it needs the split pieces *separately*. This function exposes just the split step.

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
    # Fast path: pull every Databricks (backtick) inner subquery in a single tokenize pass.
    # Postgres emits each backtick as an UNKNOWN "`" token; for each one not already inside
    # a subquery we've pulled, reuse _subquery_span to take its enclosing subquery. This is
    # the common multidialect case, and avoids re-parsing the whole query once per subquery.
    toks = tokenize(query, dialect="postgres")
    spans, covered = [], -1
    for tok in toks:
        if tok.token_type == TokenType.UNKNOWN and tok.text == "`" and tok.start > covered:
            span = _subquery_span(query, tok.start, toks)
            if span:
                spans.append(span)
                covered = span[1]  # skip the other backticks inside this same subquery
    # Keep only the outermost subqueries: a backtick in a projection scalar finds that
    # scalar, but a later backtick may find the enclosing subquery too -- drop the nested
    # one so the placeholder swaps below never overlap and no pulled region hides another.
    spans.sort()
    outermost = []
    for s, e in spans:
        if not outermost or s > outermost[-1][1]:
            outermost.append((s, e))
    outer, raw = query, {}
    for s, e in reversed(outermost):  # right-to-left so earlier offsets stay valid
        marker = MARKER.format(len(raw))
        raw[marker] = query[s : e + 1]
        outer = outer[:s] + f"(SELECT NULL AS {marker})" + outer[e + 1 :]

    # Fallback: catch any inner subquery the backtick scan can't see (a non-backtick inner,
    # or a Snowflake inner), one parse error at a time. For a pure Databricks query the fast
    # path already removed everything, so this just confirms the outer is clean and returns.
    # Bounded by MAX_ROUNDS so a query that never converges fails fast instead of spinning.
    for _ in range(MAX_ROUNDS + 1):
        offset = _error_offset(outer)
        if offset is None:
            return outer, raw
        span = _subquery_span(outer, offset)
        if span is None:
            raise ValueError("Multidialect two-pass: parse error not inside a subquery")
        s, e = span
        marker = MARKER.format(len(raw))
        raw[marker] = outer[s : e + 1]
        outer = outer[:s] + f"(SELECT NULL AS {marker})" + outer[e + 1 :]
    raise ValueError("Multidialect two-pass: did not converge")
