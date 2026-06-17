"""Snowflake variant that also accepts Databricks backtick identifiers.

This is an e6data-specific dialect kept in its own module (rather than edited
into the upstream ``snowflake.py``) so syncing with tobymao/main never conflicts.

Why it exists
-------------
Power BI emits Snowflake-shaped SQL, but by the time it reaches the converter it
can be *mixed-quoting*: the outer wrapper uses ANSI double-quoted identifiers
(``"ID"``, ``"$Outer"``, ``GROUP BY "ID"``) while inner CTEs reference tables with
Databricks backtick identifiers (`` `CookieTimeSeriesTransactionHeaderDayGrain` ``).

- Reading it as **Databricks** is wrong: Databricks lexes ``"..."`` as a *string
  literal*, so ``"ID"`` collapses to ``'ID'`` (a constant, not a column).
- Reading it as plain **Snowflake** fails: Snowflake can't tokenize the backticks
  (``ParseError``), and ``error_level`` can't rescue it — the backtick is mangled
  at the lexer (the table name collapses to an empty identifier) before any parser
  error handling runs.

Snowflake's quote semantics are unambiguous and match Power BI exactly
(``"`` = identifier, ``'`` = string), so the only missing piece is tolerating the
backtick. Adding ``` ` ``` to ``IDENTIFIERS`` lets the whole mixed query parse
correctly, with **no** risk of misreading a string literal as an identifier
(unlike forcing ``"`` -> identifier onto Databricks).

Usage
-----
Used only as the read dialect of the ``POWERBI_SF_TO_DBR`` intermediary
Snowflake -> Databricks transpile in ``converter_api.py``. It is never the default
Snowflake dialect. Reference it by the class object (not a string name) so it works
regardless of dialect import ordering.
"""

from __future__ import annotations

from sqlglot.dialects.snowflake import Snowflake


class SnowflakeBackticks(Snowflake):
    """Snowflake that treats both ``"`` and ``` ` ``` as quoted identifiers."""

    class Tokenizer(Snowflake.Tokenizer):
        # Snowflake default is ['"']; add the backtick so Databricks-style
        # identifiers in a mixed Power BI query parse as identifiers. QUOTES
        # (single-quote string literals) is inherited unchanged.
        IDENTIFIERS = ['"', "`"]
