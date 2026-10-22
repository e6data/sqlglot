"""HYBRID_MULTIDIALECT through the subquery tree (apis.utils.hybrid_lite_parser).

Each query must give the same e6 under the subtree as under the MULTIDIALECT split path.
``arr[1]`` is the probe: ELEMENT_AT(arr, 1) on the postgres path, ELEMENT_AT(arr, 2) once
reparsed as Databricks (0-based), so it shows which regions were read as Databricks.
``col:a.b`` is Databricks-only syntax Postgres cannot parse, which forces the tree.
"""

import json
import unittest
from contextlib import ExitStack
from unittest import mock

CASES = {
    # A backtick alone does not make a subquery Databricks.
    "backtick_no_tell": (
        'SELECT "s"."x" "x" FROM (SELECT `a` AS x FROM `t`) "s"',
        'SELECT "s"."x" AS "x" FROM (SELECT "a" AS x FROM "t") AS "s"',
    ),
    # A Postgres subquery holding a Databricks-only child is not split out itself, so the
    # join-ON tell still reparses it as Databricks (ELEMENT_AT(arr, 2)).
    "pg_fail_child_of_tell_linked": (
        'SELECT "s".x "x" FROM (SELECT arr[1] AS x, k, ts FROM (SELECT col:a.b AS x, k, ts FROM t) c) '
        'AS "s" LEFT JOIN dim AS "b" ON "s".k = "b".k AND timestampadd(MICROSECOND, -1, "s".ts) >= "b".eff',
        'SELECT "s".x AS "x" FROM (SELECT ELEMENT_AT(arr, 2) AS x, k, ts FROM (SELECT '
        "JSON_EXTRACT(col, '$.a.b') AS x, \"k\", ts FROM t) AS c) AS \"s\" LEFT JOIN dim AS \"b\" "
        'ON "s"."k" = "b"."k" AND TIMESTAMP_ADD(\'MICROSECOND\', -1, "s".ts) >= "b".eff',
    ),
    # A sibling that Postgres cannot parse is split out; the tell-linked subquery is not.
    "pg_fail_sibling_of_tell": (
        'SELECT "a".x, "b".y FROM (SELECT col:a.b AS x FROM t) "a" JOIN (SELECT arr[1] AS y, k FROM u) "b" '
        'ON "a".x = "b".k AND timestampadd(MICROSECOND, -1, "b".k) >= 0',
        "SELECT \"a\".x, \"b\".y FROM (SELECT JSON_EXTRACT(col, '$.a.b') AS x FROM t) AS \"a\" JOIN "
        '(SELECT ELEMENT_AT(arr, 2) AS y, "k" FROM u) AS "b" ON "a".x = "b"."k" AND '
        "TIMESTAMP_ADD('MICROSECOND', -1, \"b\".\"k\") >= 0",
    ),
    # A subquery with its own tell is reparsed as Databricks in its parent's region.
    "pg_fail_plus_pg_tell_subquery": (
        'SELECT "a".x, "b".y FROM (SELECT col:a.b AS x FROM t) "a" JOIN '
        '(SELECT timestampadd(MICROSECOND, -1, ts) AS y, arr[1] AS k FROM u) "b" ON "a".x = "b".k',
        "SELECT \"a\".x, \"b\".y FROM (SELECT JSON_EXTRACT(col, '$.a.b') AS x FROM t) AS \"a\" JOIN "
        "(SELECT TIMESTAMP_ADD('MICROSECOND', -1, ts) AS y, ELEMENT_AT(arr, 2) AS k FROM u) AS \"b\" "
        'ON "a".x = "b"."k"',
    ),
}


class TestHybridSubqueryTree(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        import converter_api
        from sqlglot.dialects import databricks, e6

        cls.converter_api = converter_api
        cls.client = TestClient(converter_api.app)
        cls.flags = [
            (converter_api, "HYBRID_MULTIDIALECT"),
            (e6, "HYBRID_MULTIDIALECT"),
            (databricks, "DBR_DOUBLE_QUOTED_IDENTIFIERS"),
        ]

    def convert(self, query, subquery_tree):
        with ExitStack() as stack:
            for module, flag in self.flags:
                stack.enter_context(mock.patch.object(module, flag, True))
            stack.enter_context(mock.patch.object(self.converter_api, "SUBQUERY_TREE", subquery_tree))
            response = self.client.post(
                "/convert-query",
                data={
                    "query": query,
                    "from_sql": "postgres",
                    "feature_flags": json.dumps({"PRETTY_PRINT": False, "MULTIDIALECT": True}),
                },
            )
        return response.json().get("converted_query")

    def test_subtree_matches_multidialect(self):
        for name, (query, expected) in CASES.items():
            for subquery_tree in (False, True):
                with self.subTest(name=name, subquery_tree=subquery_tree):
                    self.assertEqual(self.convert(query, subquery_tree), expected)


if __name__ == "__main__":
    unittest.main()
