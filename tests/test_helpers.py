import unittest
from apis.utils.helpers import (
    normalize_unicode_spaces,
    auto_quote_reserved,
    transform_table_part,
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


class TestAutoQuoteReserved(unittest.TestCase):
    """
    Unit tests for the auto_quote_reserved(sql, dialect=E6, extra_reserved=None) helper.
    """

    def test_cte_name_is_quoted(self):
        raw = "WITH join AS (SELECT 1) SELECT * FROM join"
        expected = 'WITH "join" AS (SELECT 1) SELECT * FROM "join"'
        self.assertEqual(auto_quote_reserved(raw), expected)

    def test_from_table_is_quoted(self):
        raw = "SELECT * FROM join"
        expected = 'SELECT * FROM "join"'
        self.assertEqual(auto_quote_reserved(raw), expected)

    def test_join_table_is_quoted(self):
        raw = "SELECT o.id, j.val " "FROM orders o " "JOIN join j ON o.id = j.id"
        expected = "SELECT o.id, j.val " "FROM orders o " 'JOIN "join" j ON o.id = j.id'
        self.assertEqual(auto_quote_reserved(raw), expected)

    def test_dot_alias_is_quoted(self):
        raw = "SELECT join.col FROM join"
        expected = 'SELECT "join".col FROM "join"'
        self.assertEqual(auto_quote_reserved(raw), expected)

    def test_non_reserved_identifier_unchanged(self):
        raw = "WITH customers AS (SELECT 1) SELECT * FROM customers"
        self.assertEqual(auto_quote_reserved(raw), raw)

    def test_already_quoted_stays_quoted(self):
        raw = 'WITH "join" AS (SELECT 1) SELECT * FROM "join"'
        self.assertEqual(auto_quote_reserved(raw), raw)

    def test_extra_reserved_set(self):
        raw = "SELECT * FROM temp"
        expected = 'SELECT * FROM "temp"'
        self.assertEqual(
            auto_quote_reserved(raw, extra_reserved={"temp"}),
            expected,
        )
