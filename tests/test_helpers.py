import unittest
from apis.utils.helpers import (
    normalize_unicode_spaces,
    transform_table_part,
    set_cte_names_case_sensitively,
    transform_catalog_schema_only,
    extract_large_in_clauses,
    restore_large_in_clauses,
)

from sqlglot import parse_one, exp


class TestHelpers(unittest.TestCase):
    def test_normalize_unicode_spaces(self):
        # Basic space normalization
        self.assertEqual(
            normalize_unicode_spaces("SELECT\u00a0*\u2009FROM\tusers"), "SELECT * FROM users"
        )

        # Preserve quoted literals
        self.assertEqual(
            normalize_unicode_spaces("SELECT 'a\u00a0b' FROM table"), "SELECT 'a\u00a0b' FROM table"
        )

        # Preserve double quoted identifiers
        self.assertEqual(
            normalize_unicode_spaces('SELECT "col\u00a0name" FROM table'),
            'SELECT "col\u00a0name" FROM table',
        )

        # Escaped single quotes inside string literal
        self.assertEqual(
            normalize_unicode_spaces("SELECT 'it''s\u00a0ok' FROM test"),
            "SELECT 'it''s\u00a0ok' FROM test",
        )

        # Replacement character (�) replaced with space
        self.assertEqual(
            normalize_unicode_spaces("SELECT name FROM tab\ufffdle"), "SELECT name FROM tab le"
        )

        # Mix of multiple spaces and newline preserved
        self.assertEqual(
            normalize_unicode_spaces("SELECT\n\u2028*\u00a0FROM\rusers"), "SELECT\n * FROM\rusers"
        )

    def test_transform_table_part(self):
        def create_ast(query: str) -> exp.Expression:
            return parse_one(query)

        def create_query(ast: exp.Expression) -> str:
            return ast.sql()

        self.assertEqual(
            create_query(
                transform_table_part(
                    create_ast("SELECT catalogn.dbn.tablen.column from catalogn.dbn.tablen")
                )
            ),
            "SELECT catalogn_dbn.tablen.column FROM catalogn_dbn.tablen",
        )

    def test_transform_table_part_while_skipping_e6_tranpilation(self):
        self.assertEqual(
            transform_catalog_schema_only(
                "SELECT `col` FROM catalogn.dbn.tablen", from_sql="spark"
            ),
            "SELECT `col` FROM catalogn_dbn.tablen",
        )


# class TestAutoQuoteReserved(unittest.TestCase):
#     """
#     Unit tests for the auto_quote_reserved(sql, dialect=E6, extra_reserved=None) helper.
#     """
#
#     def test_cte_name_is_quoted(self):
#         raw = "WITH join AS (SELECT 1) SELECT * FROM join"
#         expected = 'WITH "join" AS (SELECT 1) SELECT * FROM "join"'
#         self.assertEqual(auto_quote_reserved(raw), expected)
#
#     def test_from_table_is_quoted(self):
#         raw = "SELECT * FROM join"
#         expected = 'SELECT * FROM "join"'
#         self.assertEqual(auto_quote_reserved(raw), expected)
#
#     def test_join_table_is_quoted(self):
#         raw = "SELECT o.id, j.val " "FROM orders o " "JOIN join j ON o.id = j.id"
#         expected = "SELECT o.id, j.val " "FROM orders o " 'JOIN "join" j ON o.id = j.id'
#         self.assertEqual(auto_quote_reserved(raw), expected)
#
#     def test_dot_alias_is_quoted(self):
#         raw = "SELECT join.col FROM join"
#         expected = 'SELECT "join".col FROM "join"'
#         self.assertEqual(auto_quote_reserved(raw), expected)
#
#     def test_non_reserved_identifier_unchanged(self):
#         raw = "WITH customers AS (SELECT 1) SELECT * FROM customers"
#         self.assertEqual(auto_quote_reserved(raw), raw)
#
#     def test_already_quoted_stays_quoted(self):
#         raw = 'WITH "join" AS (SELECT 1) SELECT * FROM "join"'
#         self.assertEqual(auto_quote_reserved(raw), raw)
#
#     def test_extra_reserved_set(self):
#         raw = "SELECT * FROM temp"
#         expected = 'SELECT * FROM "temp"'
#         self.assertEqual(
#             auto_quote_reserved(raw, extra_reserved={"temp"}),
#             expected,
#         )


class TestCteNamesCaseSensitivity(unittest.TestCase):
    def test_set_cte_names_case_sensitively(self):
        raw = "with final as(select 1, 2, 3) select * from Final"
        expected = "WITH final AS (SELECT 1, 2, 3) SELECT * FROM final"
        raw_ast = parse_one(raw)
        set_ast = set_cte_names_case_sensitively(raw_ast)
        handled_sql = set_ast.sql()
        self.assertEqual(handled_sql, expected)


class TestLargeInClauseOptimization(unittest.TestCase):
    """Tests for extract_large_in_clauses / restore_large_in_clauses."""

    def _make_string_values(self, n):
        """Generate n single-quoted hex-string values."""
        return ",".join(f"'6a{i:010x}'" for i in range(n))

    def _make_numeric_values(self, n):
        """Generate n bare numeric values."""
        return ",".join(str(i) for i in range(n))

    # ---- extraction tests ----

    def test_below_threshold_not_extracted(self):
        values = self._make_string_values(100)
        sql = f"SELECT * FROM t WHERE id IN ({values})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(replacements, {})
        self.assertEqual(simplified, sql)

    def test_at_threshold_extracted(self):
        values = self._make_string_values(500)
        sql = f"SELECT * FROM t WHERE id IN ({values})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(len(replacements), 1)
        self.assertIn("__LARGE_IN_0__", simplified)
        self.assertNotIn("6a", simplified)

    def test_large_string_values_extracted(self):
        values = self._make_string_values(1000)
        sql = f"SELECT * FROM t WHERE id IN ({values})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(len(replacements), 1)
        self.assertIn("IN ('__LARGE_IN_0__')", simplified)

    def test_large_numeric_values_extracted(self):
        values = self._make_numeric_values(600)
        sql = f"SELECT * FROM t WHERE id IN ({values})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(len(replacements), 1)

    def test_subquery_not_extracted(self):
        sql = "SELECT * FROM t WHERE id IN (SELECT id FROM other)"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(replacements, {})
        self.assertEqual(simplified, sql)

    def test_function_call_in_values_not_extracted(self):
        values = ",".join(f"'val{i}'" for i in range(500)) + ",UPPER(x)"
        sql = f"SELECT * FROM t WHERE id IN ({values})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(replacements, {})

    def test_no_in_clause(self):
        sql = "SELECT * FROM t WHERE x = 1 AND y = 'foo'"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(replacements, {})
        self.assertEqual(simplified, sql)

    def test_multiple_in_clauses_only_large_extracted(self):
        small = ",".join(f"'s{i}'" for i in range(10))
        big = self._make_string_values(600)
        sql = f"SELECT * FROM t WHERE a IN ({small}) AND b IN ({big})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(len(replacements), 1)
        # Small IN clause must remain untouched
        self.assertIn(f"a IN ({small})", simplified)

    def test_escaped_quotes_in_values_extracted(self):
        values = ",".join(f"'it''s_{i}'" for i in range(600))
        sql = f"SELECT * FROM t WHERE name IN ({values})"
        simplified, replacements = extract_large_in_clauses(sql)
        self.assertEqual(len(replacements), 1)

    # ---- restore tests ----

    def test_restore_single_quoted_placeholder(self):
        sql = "SELECT * FROM t WHERE id IN ('__LARGE_IN_0__')"
        replacements = {"__LARGE_IN_0__": "'a','b','c'"}
        restored = restore_large_in_clauses(sql, replacements)
        self.assertEqual(restored, "SELECT * FROM t WHERE id IN ('a','b','c')")

    def test_restore_double_quoted_placeholder(self):
        # Transpiler may switch to double quotes
        sql = 'SELECT * FROM t WHERE id IN ("__LARGE_IN_0__")'
        replacements = {"__LARGE_IN_0__": "'a','b','c'"}
        restored = restore_large_in_clauses(sql, replacements)
        self.assertEqual(restored, "SELECT * FROM t WHERE id IN ('a','b','c')")

    def test_restore_empty_replacements_noop(self):
        sql = "SELECT * FROM t WHERE id IN ('x','y')"
        restored = restore_large_in_clauses(sql, {})
        self.assertEqual(restored, sql)

    # ---- round-trip tests ----

    def test_roundtrip_preserves_all_values(self):
        values = self._make_string_values(1000)
        sql = f"SELECT * FROM t WHERE id IN ({values}) AND x = 1"
        simplified, replacements = extract_large_in_clauses(sql)
        restored = restore_large_in_clauses(simplified, replacements)
        self.assertEqual(restored, sql)

    def test_roundtrip_with_transpilation(self):
        """End-to-end: extract -> transpile -> restore produces correct output."""
        import sqlglot
        from sqlglot.optimizer.qualify_columns import quote_identifiers
        from apis.utils.helpers import replace_struct_in_query, ensure_select_from_values

        values_list = [f"'6a{i:06x}'" for i in range(600)]
        values = ",".join(values_list)
        query = f"SELECT id FROM my_table WHERE id IN ({values}) AND x = 1"

        # WITH optimization
        simplified, replacements = extract_large_in_clauses(query)
        tree = sqlglot.parse_one(simplified, read="databricks", error_level=None)
        tree = quote_identifiers(tree, dialect="e6")
        tree = ensure_select_from_values(tree)
        tree = set_cte_names_case_sensitively(tree)
        out_with = tree.sql(dialect="e6", from_dialect="databricks", pretty=False)
        out_with = replace_struct_in_query(out_with)
        out_with = restore_large_in_clauses(out_with, replacements)

        # WITHOUT optimization
        tree = sqlglot.parse_one(query, read="databricks", error_level=None)
        tree = quote_identifiers(tree, dialect="e6")
        tree = ensure_select_from_values(tree)
        tree = set_cte_names_case_sensitively(tree)
        out_without = tree.sql(dialect="e6", from_dialect="databricks", pretty=False)
        out_without = replace_struct_in_query(out_without)

        # Both must contain the same values
        for val in values_list:
            self.assertIn(val, out_with)
            self.assertIn(val, out_without)

        # Non-IN parts must match
        self.assertEqual(
            out_with.split("IN")[0],
            out_without.split("IN")[0],
        )
