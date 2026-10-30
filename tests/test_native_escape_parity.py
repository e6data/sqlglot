import unittest

from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import USE_RS_TOKENIZER


@unittest.skipUnless(USE_RS_TOKENIZER, "Requires the native tokenizer build")
class TestNativeEscapeParity(unittest.TestCase):
    def test_databricks_unknown_escape_value(self):
        d = Dialect.get_or_raise("databricks")
        for native in (False, True):
            tokens = d.tokenizer(use_rs_tokenizer=native).tokenize(r"SELECT '\d'")
            self.assertEqual(tokens[-1].text, "d")

    def test_string_escape_parity(self):
        for dialect in ("databricks", "spark", "postgres"):
            for sql in (
                r"SELECT '\d+\.\w', '\q', '\%', '\_', '\\', '\n'",
                r"SELECT '\"', '\\d', 'it\'s'",
                "SELECT 'café\\q', '日本語', '\\ ' , '\\\t'",
                r"SELECT r'\d+\q', `a\q`",
                "SELECT '\\q",
                "SELECT '\\",
            ):
                with self.subTest(dialect=dialect, sql=sql):
                    d = Dialect.get_or_raise(dialect)

                    def result(native):
                        try:
                            return [
                                (t.token_type, t.text, t.start, t.end, t.line, t.col)
                                for t in d.tokenizer(use_rs_tokenizer=native).tokenize(sql)
                            ]
                        except Exception as exc:
                            return type(exc).__name__

                    self.assertEqual(result(True), result(False))
