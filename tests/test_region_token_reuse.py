import unittest
from unittest.mock import patch

import sqlglot
from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Token, USE_RS_TOKENIZER
from apis.utils.subquery_tree import transpile_multidialect, build_tree, _mask, SQNode
from apis.utils.region_tokens import RegionTokens


class TestRegionTokenReuse(unittest.TestCase):
    @unittest.skipIf(USE_RS_TOKENIZER, "Native tokens have immutable positions")
    def test_mask_does_not_copy_existing_tokens(self):
        sql = "SELECT " + ", ".join(f"x AS c{i}" for i in range(30)) + " FROM (SELECT 1) s"
        tokens = Dialect().tokenize(sql)
        root = build_tree(sql, tokens)
        retained = RegionTokens(sql, tokens)
        body, _ = _mask(root)
        original = Token.__init__
        allocations = []

        def init(token, *args, **kwargs):
            allocations.append(token)
            original(token, *args, **kwargs)

        with patch.object(Token, "__init__", init):
            actual = retained.mask(root, body)
        self.assertIsNotNone(actual)
        self.assertLessEqual(len(allocations), 6)  # Only the new placeholder.

    def test_masked_token_positions_match_fresh_tokenization(self):
        for sql in (
            'SELECT "a"\nFROM (SELECT x AS "a"\n FROM t) s WHERE "a" > 1',
            "SELECT *\rFROM (SELECT * FROM (SELECT 1) i) o",
            "SELECT *\r\nFROM (SELECT 1) a JOIN (SELECT 2) b ON 1 = 1",
            "SELECT 'Tri-State', 'it''s fine' FROM (SELECT 1) s",
            'SELECT "café" FROM (SELECT \'日本語\' AS "café") s',
        ):
            tokens = Dialect().tokenize(sql)
            root = build_tree(sql, tokens)
            retained = RegionTokens(sql, tokens)
            pending = [root]
            while pending:
                node = pending.pop()
                body, _ = _mask(node)
                actual = retained.mask(node, body)
                self.assertIsNotNone(actual)
                expected = Postgres().tokenize(body)
                fields = ("token_type", "text", "start", "end", "line", "col", "whitespace_before")
                self.assertEqual(
                    [[getattr(t, k, "") for k in fields] for t in actual],
                    [[getattr(t, k, "") for k in fields] for t in expected],
                )
                pending.extend(node.children)

    def test_lexical_differences_use_normal_tokenizer(self):
        for sql in (
            "SELECT FLOAT '1'",
            "SELECT $$value$$",
            "SELECT E'abc'",
            "SELECT B'01'",
            "SELECT X'AB'",
            "SELECT a @> b",
            "SELECT 1 /* hint */",
            "SELECT 1 -- comment",
        ):
            tokens = Dialect().tokenize(sql)
            self.assertIsNone(
                RegionTokens(sql, tokens).mask(SQNode("ROOT", sql, end=len(sql)), sql)
            )

    def test_different_keyword_tables_cannot_silently_reuse_tokens(self):
        generic = Dialect().tokenizer_class.KEYWORDS
        postgres = Postgres.tokenizer_class.KEYWORDS
        for keyword in generic.keys() | postgres.keys():
            if generic.get(keyword) == postgres.get(keyword):
                continue
            sql = f"SELECT {keyword} FROM t"
            # Comment openers and command text need not form valid standalone
            # lexical input; they are explicitly excluded by the symbol guard.
            if keyword.startswith("/*"):
                continue
            tokens = Dialect().tokenize(sql)
            with self.subTest(keyword=keyword):
                self.assertIsNone(
                    RegionTokens(sql, tokens).mask(SQNode("ROOT", sql, end=len(sql)), sql)
                )

    def test_cleanup_invalidates_tokens(self):
        import converter_api as api

        for sql in ("SELECT\t1", "SELECT 1 /* comment */"):
            tokens = Dialect().tokenize(sql)
            original = api.parse_region
            with patch.object(api, "SKIP_COMMENT", "true"), patch.object(
                api, "parse_region", wraps=original
            ) as parse:
                api._region_to_e6(sql, None, False, tokens)
            self.assertIsNone(parse.call_args.kwargs["tokens"])

    def test_ambiguous_substring_mask_does_not_reuse_tokens(self):
        sql = "SELECT '(SELECT 1)' FROM (SELECT 1) s"
        tokens = Dialect().tokenize(sql)
        root = build_tree(sql, tokens)
        body, _ = _mask(root)
        self.assertIsNone(RegionTokens(sql, tokens).mask(root, body))

    def test_databricks_fallback_matches_uncached_emitter(self):
        from apis.utils.subquery_tree import _default_emit

        queries = (
            'SELECT "x" FROM (SELECT `x` FROM `table`) s',
            "SELECT * FROM (SELECT * FROM (SELECT `x` FROM t) i) o",
            "SELECT * FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS y) b ON a.x = b.y",
        )
        for sql in queries:
            self.assertEqual(
                transpile_multidialect(sql, False),
                transpile_multidialect(sql, False, emit=_default_emit),
            )

    def test_function_arguments_do_not_allocate_speculative_identifiers(self):
        original = exp.Identifier.__init__
        allocations = []

        def init(node, **kwargs):
            allocations.append(node)
            original(node, **kwargs)

        with patch.object(exp.Identifier, "__init__", init):
            tree = sqlglot.parse_one('SELECT fn("a", "b")', read="postgres")
        self.assertEqual(len(list(tree.find_all(exp.Identifier))), 2)
        self.assertEqual(len(allocations), 2)

    def test_qualified_column_allocates_one_column(self):
        original = exp.Column.__init__
        allocations = []

        def init(node, **kwargs):
            allocations.append(node)
            original(node, **kwargs)

        with patch.object(exp.Column, "__init__", init):
            tree = sqlglot.parse_one('SELECT catalog.db."table"."column"', read="postgres")
        column = tree.expressions[0]
        self.assertEqual([part.name for part in column.parts], ["catalog", "db", "table", "column"])
        for part in column.parts:
            self.assertIs(part.parent, column)
        self.assertEqual(len(allocations), 1)

    def test_regions_do_not_retokenize_postgres(self):
        sql = 'SELECT "a" FROM (SELECT x AS "a" FROM t) s WHERE "a" > 1'
        original = Postgres.tokenize
        calls = []

        def tokenize(dialect, text, **kwargs):
            calls.append(text)
            return original(dialect, text, **kwargs)

        with patch.object(Postgres, "tokenize", tokenize):
            result = transpile_multidialect(sql, pretty=False)
        self.assertIn("SELECT", result)
        self.assertEqual(calls, [], "Discovery tokens should serve both PG regions")

    @unittest.skipUnless(USE_RS_TOKENIZER, "Native token identity regression")
    def test_native_region_keeps_original_token_objects(self):
        sql = 'SELECT "a"\nFROM (SELECT x AS "a" FROM t) s WHERE "a" > 1'
        tokens = Dialect().tokenize(sql)
        root = build_tree(sql, tokens)
        child = root.children[0]
        original_child_token = next(token for token in tokens if token.start == child.start)
        retained = RegionTokens(sql, tokens)
        body, _ = _mask(root)
        actual = retained.mask(root, body)
        self.assertIsNotNone(actual)
        self.assertIs(actual[0], tokens[0])
        child_tokens = retained.mask(child, child.raw)
        self.assertIs(child_tokens[0], original_child_token)

    @unittest.skipUnless(USE_RS_TOKENIZER, "Native relocation rollback regression")
    def test_native_incompatible_region_leaves_positions_unchanged(self):
        for sql in ("SELECT * FROM (SELECT x, FLOAT '1') s", "SELECT * FROM (SELECT x, café) s"):
            tokens = Dialect().tokenize(sql)
            root = build_tree(sql, tokens)
            fields = ("start", "end", "line", "col")
            before = [tuple(getattr(t, f) for f in fields) for t in tokens]
            child = root.children[0]
            self.assertIsNone(RegionTokens(sql, tokens).mask(child, child.raw))
            self.assertEqual(before, [tuple(getattr(t, f) for f in fields) for t in tokens])

    def test_type_literal_does_not_allocate_a_peek_literal(self):
        original = exp.Literal.__init__
        strings = []

        def init(node, **kwargs):
            if kwargs.get("is_string"):
                strings.append(kwargs.get("this"))
            original(node, **kwargs)

        with patch.object(exp.Literal, "__init__", init):
            tree = sqlglot.parse_one("SELECT DECIMAL(10, 2) '1.25'", read="postgres")
        self.assertIsInstance(tree.expressions[0], exp.Cast)
        self.assertEqual(strings, ["1.25"])
