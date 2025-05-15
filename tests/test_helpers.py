import unittest
from apis.utils.helpers import normalize_unicode_spaces


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
