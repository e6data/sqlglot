import itertools
import json
import unittest
from unittest import mock

from apis.utils.subquery_tree import build_tree, reproduce


class TestSubqueryTreeReproduce(unittest.TestCase):
    """The tree must rebuild the exact original query -- transpile relies on the
    subquery ``raw`` spans, so nothing may be lost going through the tree."""

    def assertRoundTrips(self, query):
        # reproduce(build_tree(q)) must equal q byte for byte.
        self.assertEqual(reproduce(build_tree(query)), query)

    def test_no_subquery(self):
        self.assertRoundTrips("SELECT a, b FROM t WHERE a > 1")

    def test_single_from_subquery(self):
        self.assertRoundTrips("SELECT x FROM (SELECT a AS x FROM t) AS s")

    def test_nested_subqueries(self):
        self.assertRoundTrips(
            "SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS inner_q) AS outer_q"
        )

    def test_sibling_subqueries(self):
        self.assertRoundTrips("SELECT * FROM (SELECT 1 AS a) x, (SELECT 2 AS b) y")

    def test_join_subquery(self):
        self.assertRoundTrips("SELECT * FROM t JOIN (SELECT id FROM u) AS j ON t.id = j.id")

    def test_union_of_subqueries(self):
        self.assertRoundTrips(
            "SELECT a FROM (SELECT 1 AS a) x UNION ALL SELECT b FROM (SELECT 2 AS b) y"
        )

    def test_subquery_in_where_is_not_lost(self):
        # LiteParser skips WHERE, so this subquery is not isolated as a node -- but it
        # stays as body text, so the query still reproduces exactly.
        self.assertRoundTrips("SELECT a FROM t WHERE a IN (SELECT id FROM allowed)")

    def test_cte_body(self):
        self.assertRoundTrips("WITH c AS (SELECT a FROM t) SELECT * FROM c")

    def test_multidialect_backtick_custom_sql(self):
        # Postgres outer ("...") wrapping a Databricks inner (`...`), Tableau-style.
        self.assertRoundTrips('SELECT "col" FROM (SELECT `x`.`y` FROM `db`.`t`) "Custom SQL Query"')

    def test_whitespace_and_newlines_preserved(self):
        self.assertRoundTrips("SELECT   *\n  FROM (\n    SELECT  a\n    FROM t\n  ) AS s\n")

    def test_plain_table_between_subqueries(self):
        # A non-subquery table sits between two subqueries in the FROM.
        self.assertRoundTrips("SELECT * FROM (SELECT 1 AS a) x, plain_table t, (SELECT 2 AS b) y")

    def test_plain_content_before_between_and_after(self):
        # Plain columns, a real table, and modifiers surround the subqueries.
        self.assertRoundTrips(
            "SELECT r.x, r.y FROM real_table r "
            "JOIN (SELECT a FROM t1) s1 ON r.a = s1.a "
            "JOIN (SELECT b FROM t2) s2 ON r.b = s2.b "
            "WHERE r.x > 5 GROUP BY r.x, r.y"
        )

    def test_plain_table_joined_inside_subquery(self):
        # Non-subquery content interleaved with a subquery at a deeper level.
        self.assertRoundTrips(
            "SELECT * FROM (SELECT * FROM plain p " "JOIN (SELECT a FROM t) i ON p.id = i.a) AS o"
        )

    def test_subquery_only_in_the_middle(self):
        # Leading and trailing plain SQL, one subquery in the middle.
        self.assertRoundTrips("SELECT col1, col2, col3 FROM a, b, (SELECT id FROM c) mid, d, e")


class TestSubqueryTreeStructure(unittest.TestCase):
    """Spot-check labels and hierarchy so transpile can walk them."""

    def test_labels_and_nesting(self):
        root = build_tree("SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS i) AS o")
        self.assertEqual(root.label, "ROOT")
        # One top-level subquery (o), which itself has one child (i).
        self.assertEqual(len(root.children), 1)
        outer = root.children[0]
        self.assertEqual(len(outer.children), 1)
        inner = outer.children[0]
        self.assertEqual(inner.children, [])
        # Every node's raw is a standalone parenthesized subquery.
        self.assertTrue(outer.raw.startswith("(") and outer.raw.endswith(")"))
        self.assertTrue(inner.raw.startswith("(") and inner.raw.endswith(")"))
        self.assertIn("SELECT a FROM t", inner.raw)

    def test_siblings_in_document_order(self):
        root = build_tree("SELECT * FROM (SELECT 1 AS a) x, (SELECT 2 AS b) y")
        self.assertEqual(len(root.children), 2)
        self.assertIn("a", root.children[0].raw)
        self.assertIn("b", root.children[1].raw)

    def test_join_condition_is_skipped(self):
        # A JOIN's ON / USING is not parsed, so a dialect-specific token there (a backtick)
        # neither fails the parse nor hides the subquery joined after it. LEFT(...) is a
        # function, not a join.
        for query in (
            "SELECT * FROM t JOIN u ON CAST(u.`x` AS INT) = t.id JOIN (SELECT id FROM v) j ON t.id = j.id",
            "SELECT * FROM t JOIN u ON COALESCE(u.`x`, 0) = t.id, (SELECT id FROM v) j",
            "SELECT * FROM t JOIN u USING (`x`) LEFT JOIN (SELECT id FROM v) j ON t.id = j.id",
            "SELECT * FROM t JOIN u ON LEFT(u.`x`, 2) = t.id LEFT JOIN (SELECT id FROM v) j ON 1 = 1",
        ):
            with self.subTest(query=query):
                root = build_tree(query)
                self.assertEqual(reproduce(root), query)
                self.assertEqual([c.raw for c in root.children], ["(SELECT id FROM v)"])


class TestSubqueryTreeTrigger(unittest.TestCase):
    """SUBQUERY_TREE uses the tree for every query; SUBQUERY_TREE_MULTIDIALECT only when the
    planner sends the MULTIDIALECT flag."""

    def test_subquery_tree_triggers_the_tree(self):
        from fastapi.testclient import TestClient

        import converter_api

        client = TestClient(converter_api.app)
        for subquery_tree, tree_md, multidialect in itertools.product((False, True), repeat=3):
            with (
                self.subTest(subquery_tree=subquery_tree, md=tree_md, multidialect=multidialect),
                mock.patch.object(converter_api, "SUBQUERY_TREE", subquery_tree),
                mock.patch.object(converter_api, "SUBQUERY_TREE_MULTIDIALECT", tree_md),
                mock.patch.object(
                    converter_api, "subquery_tree_transpile", return_value="SELECT 1"
                ) as tree,
            ):
                client.post(
                    "/convert-query",
                    data={
                        "query": "SELECT 1",
                        "from_sql": "databricks",
                        "feature_flags": json.dumps({"MULTIDIALECT": multidialect}),
                    },
                )
                self.assertEqual(tree.called, subquery_tree or (tree_md and multidialect))

    def test_backtick_in_join_condition(self):
        # Without the MULTIDIALECT flag, a Databricks query with a backtick in a JOIN's ON
        # transpiles through the tree to the same e6 as the plain Databricks path.
        from fastapi.testclient import TestClient

        import converter_api

        client = TestClient(converter_api.app)
        query = "SELECT t.a FROM t JOIN u ON CAST(u.`x` AS INT) = t.id"
        outputs = []
        for subquery_tree in (False, True):
            with mock.patch.object(converter_api, "SUBQUERY_TREE", subquery_tree):
                response = client.post(
                    "/convert-query",
                    data={
                        "query": query,
                        "from_sql": "databricks",
                        "feature_flags": json.dumps({"PRETTY_PRINT": False}),
                    },
                )
            outputs.append(response.json().get("converted_query"))
        self.assertEqual(outputs[1], 'SELECT t.a FROM t JOIN u ON CAST(u."x" AS INT) = t.id')
        self.assertEqual(outputs[1], outputs[0])

    def test_tableau_escape_strings_without_samsung_flag(self):
        # A Samsung Tableau query whose parameter arrived as E'...\n' goes through the subquery
        # tree with SUBQUERY_TREE + STRIP_E_STRING_PREFIX + STRIP_STRING_NEWLINES, no
        # SAMSUNG_TABLEAU_DASHBOARD. The flags are read at import, so this runs in a subprocess.
        import os
        import subprocess
        import sys
        from pathlib import Path

        query = (
            'SELECT "Custom SQL Query"."du_id" AS "du_id" FROM (SELECT * FROM `dlt`.`w`.`t` '
            "WHERE (E'VZ_1,\\nVZ_2' = 'All' OR GNODEB_ID IN (SELECT trim(v) AS GNODEB_ID "
            "FROM (SELECT explode(split(E'VZ_1,\\nVZ_2', ',')) AS v) g))) \"Custom SQL Query\""
        )
        code = (
            "import json, logging, sys\n"
            "logging.disable(logging.CRITICAL)\n"
            "from fastapi.testclient import TestClient\n"
            "import converter_api\n"
            "r = TestClient(converter_api.app).post('/convert-query', data={'query': sys.argv[1], "
            "'from_sql': 'postgres', 'feature_flags': json.dumps({'PRETTY_PRINT': False})})\n"
            "print(r.json()['converted_query'])\n"
        )
        env = {
            **{k: v for k, v in os.environ.items() if k != "SAMSUNG_TABLEAU_DASHBOARD"},
            "SUBQUERY_TREE": "true",
            "STRIP_E_STRING_PREFIX": "true",
            "STRIP_STRING_NEWLINES": "true",
        }
        result = subprocess.run(
            [sys.executable, "-c", code, query],
            capture_output=True,
            text=True,
            env=env,
            cwd=Path(__file__).parents[1],
        )
        # (importing converter_api prints a storage-client banner first, so take the last line)
        self.assertEqual(
            result.stdout.splitlines()[-1] if result.stdout else result.stderr,
            'SELECT "Custom SQL Query"."du_id" AS "du_id" FROM (SELECT * FROM "dlt"."w"."t" '
            "WHERE ('VZ_1,VZ_2' = 'All' OR GNODEB_ID IN (SELECT TRIM(v) AS GNODEB_ID "
            "FROM (SELECT EXPLODE(SPLIT('VZ_1,VZ_2', ',')) AS v) AS \"g\"))) AS \"Custom SQL Query\"",
        )

    def test_inner_dialect_matches_split_path(self):
        # A region that fails the Postgres parse is read as INNER_DIALECT, as in split_pg_outer.
        from fastapi.testclient import TestClient

        import converter_api

        client = TestClient(converter_api.app)
        query = 'SELECT "$Table"."v" AS "v" FROM (SELECT a::VARIANT:b::STRING AS v FROM t) "$Table"'
        flags = {"PRETTY_PRINT": False, "MULTIDIALECT": True, "INNER_DIALECT": "snowflake"}
        outputs = []
        for subquery_tree in (False, True):
            with mock.patch.object(converter_api, "SUBQUERY_TREE", subquery_tree):
                response = client.post(
                    "/convert-query",
                    data={
                        "query": query,
                        "from_sql": "snowflake",
                        "feature_flags": json.dumps(flags),
                    },
                )
            outputs.append(response.json().get("converted_query"))
        self.assertEqual(
            outputs[1],
            'SELECT "$Table"."v" AS "v" FROM (SELECT CAST(JSON_EXTRACT(CAST(a AS VARIANT), '
            "'$.b') AS VARCHAR) AS v FROM t) AS \"$Table\"",
        )
        self.assertEqual(outputs[1], outputs[0])


if __name__ == "__main__":
    unittest.main()
