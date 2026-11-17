"""The subquery-tree lite parser for HYBRID_MULTIDIALECT (Freshworks).

In hybrid mode the whole query is read as Postgres, and the e6 generator reparses a subquery
or a join as Databricks when it carries a tell (a function Postgres can't resolve, i.e. an
Anonymous node) -- see ``E6.Generator.subquery_sql`` and ``join_sql``. That only works when
the subquery is parsed inside its parent: ``raw_sql`` is recorded there, and the join rule
reads the parent's joins. So unlike :class:`LiteParser`, which makes every FROM/JOIN subquery
a tree node, :class:`HybridLiteParser` splits out only a subquery Postgres cannot parse.
"""

from __future__ import annotations

import typing as t

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from apis.utils.lite_parser import LiteParser


class HybridLiteParser(LiteParser):
    """Keep a subquery's ``meta["raw_sql"]`` (making it a tree node) only if Postgres cannot
    parse it. The subqueries already split out of it are masked first, so a Postgres subquery
    is not split out just for holding a Databricks one."""

    def _parse_select_query(self, *args, **kwargs) -> t.Optional[exp.Expression]:
        this = super()._parse_select_query(*args, **kwargs)
        if isinstance(this, exp.Subquery) and this.meta.get("raw_sql"):
            raw = this.meta["raw_sql"]
            for sq in this.this.find_all(exp.Subquery):
                if sq.meta.get("raw_sql"):
                    raw = raw.replace(sq.meta["raw_sql"], "(SELECT NULL)", 1)
            try:
                sqlglot.parse_one(raw, read="postgres")
                del this.meta["raw_sql"]
            except (ParseError, TokenError):
                pass
        return this
