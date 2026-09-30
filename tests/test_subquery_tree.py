import unittest

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
        self.assertRoundTrips('SELECT x FROM (SELECT a AS x FROM t) AS s')

    def test_nested_subqueries(self):
        self.assertRoundTrips(
            "SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS inner_q) AS outer_q"
        )

    def test_sibling_subqueries(self):
        self.assertRoundTrips("SELECT * FROM (SELECT 1 AS a) x, (SELECT 2 AS b) y")

    def test_join_subquery(self):
        self.assertRoundTrips(
            "SELECT * FROM t JOIN (SELECT id FROM u) AS j ON t.id = j.id"
        )

    def test_union_of_subqueries(self):
        self.assertRoundTrips(
            "SELECT a FROM (SELECT 1 AS a) x UNION ALL SELECT b FROM (SELECT 2 AS b) y"
        )

    def test_subquery_in_where_is_not_lost(self):
        # LiteParser skips WHERE, so this subquery is not isolated as a node -- but it
        # stays as body text, so the query still reproduces exactly.
        self.assertRoundTrips(
            "SELECT a FROM t WHERE a IN (SELECT id FROM allowed)"
        )

    def test_cte_body(self):
        self.assertRoundTrips(
            "WITH c AS (SELECT a FROM t) SELECT * FROM c"
        )

    def test_multidialect_backtick_custom_sql(self):
        # Postgres outer ("...") wrapping a Databricks inner (`...`), Tableau-style.
        self.assertRoundTrips(
            'SELECT "col" FROM (SELECT `x`.`y` FROM `db`.`t`) "Custom SQL Query"'
        )

    def test_whitespace_and_newlines_preserved(self):
        self.assertRoundTrips(
            "SELECT   *\n  FROM (\n    SELECT  a\n    FROM t\n  ) AS s\n"
        )

    def test_plain_table_between_subqueries(self):
        # A non-subquery table sits between two subqueries in the FROM.
        self.assertRoundTrips(
            "SELECT * FROM (SELECT 1 AS a) x, plain_table t, (SELECT 2 AS b) y"
        )

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
            "SELECT * FROM (SELECT * FROM plain p "
            "JOIN (SELECT a FROM t) i ON p.id = i.a) AS o"
        )

    def test_subquery_only_in_the_middle(self):
        # Leading and trailing plain SQL, one subquery in the middle.
        self.assertRoundTrips(
            "SELECT col1, col2, col3 FROM a, b, (SELECT id FROM c) mid, d, e"
        )


class TestSubqueryTreeStructure(unittest.TestCase):
    """Spot-check labels and hierarchy so transpile can walk them."""

    def test_labels_and_nesting(self):
        root = build_tree(
            "SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS i) AS o"
        )
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


if __name__ == "__main__":
    unittest.main()
