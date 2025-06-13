import unittest
from apis.utils.helpers import (
    normalize_unicode_spaces,
    auto_quote_reserved,
)


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


ITEM = "condenast"  # keyword for header detection


class TestSanitizePreserveBlocks(unittest.TestCase):
    # ---------- simple cases ----------
    def test_header_strip_and_return(self):
        raw = "/* condenast::XYZ */ SELECT 1"
        sanitized, header = sanitize_preserve_blocks(raw, ITEM)
        self.assertEqual(header, "/* condenast::XYZ */")
        self.assertNotIn(header, sanitized)

    def test_preserve_existing_block_comment(self):
        raw = "SELECT 1; /* keep me */ SELECT 2;"
        sanitized, _ = sanitize_preserve_blocks(raw, ITEM)
        self.assertIn("/* keep me */", sanitized)

    def test_continuous_hyphens(self):
        raw = "--abc--def\nSELECT 1"
        first_line, _ = sanitize_preserve_blocks(raw, ITEM)[0].splitlines()[:2]
        self.assertEqual(first_line.count("/*"), 1)
        self.assertEqual(first_line.count("*/"), 1)

    def test_balancer_adds_missing_closer(self):
        raw = "/* open\nSELECT 1"
        sanitized, _ = sanitize_preserve_blocks(raw, ITEM)
        self.assertTrue(sanitized.rstrip().endswith("*/"))
        self.assertEqual(sanitized.count("/*"), sanitized.count("*/"))

    def test_balancer_removes_excess_closer(self):
        raw = "SELECT 1 */ */"
        sanitized, _ = sanitize_preserve_blocks(raw, ITEM)
        self.assertEqual(sanitized.count("/*"), sanitized.count("*/"))

    def test_stray_open_block_token(self):
        raw = "-- where a like 'ab*sb'/*  \n"
        sanitized, _ = sanitize_preserve_blocks(raw, ITEM)

        expected = "/* where a like 'ab*sb'/ * */"
        self.assertEqual(
            sanitized.strip(),
            expected,
            "stray /* inside line-comment must be neutralised to '/ *'",
        )

    def test_stray_close_block_token(self):
        raw = "-- where b like 'xy*/z'*/\n"
        sanitized, _ = sanitize_preserve_blocks(raw, ITEM)

        expected = "/* where b like 'xy* /z'* / */"
        self.assertEqual(
            sanitized.strip(),
            expected,
            "stray */ inside line-comment must be neutralised to '* /'",
        )

    def test_inner_hyphens(self):
        raw = "-- stray -- hyphens--inside--body\n"
        sanitized, _ = sanitize_preserve_blocks(raw, ITEM)

        expected = "/* stray -- hyphens--inside--body */"
        self.assertEqual(
            sanitized.strip(),
            expected,
            "inner -- sequences should stay verbatim; only leading -- is converted",
        )
