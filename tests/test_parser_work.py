"""Work regressions for parser dispatch and the e6 AST transform pass."""
import pickle
import sys
import unittest
from enum import Enum
from unittest.mock import patch

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType
from apis.utils.helpers import apply_e6_ast_transforms


class TestParserWork(unittest.TestCase):
    def test_token_matching_avoids_python_enum_hash_dispatch(self):
        sqlglot.parse_one('SELECT 1', read='postgres')
        calls = 0
        enum_hash = Enum.__hash__.__code__

        def profile(frame, event, arg):
            nonlocal calls
            if event == 'call' and frame.f_code is enum_hash:
                if isinstance(frame.f_locals.get('self'), TokenType):
                    calls += 1

        previous = sys.getprofile()
        try:
            sys.setprofile(profile)
            tree = sqlglot.parse_one('SELECT a + 2 * b FROM t WHERE a > 1', read='postgres')
        finally:
            sys.setprofile(previous)
        self.assertIsInstance(tree.expressions[0], exp.Add)
        self.assertEqual(calls, 0, 'Token set checks should not execute Python Enum.__hash__')

    def test_token_identity_lookup_and_serialization(self):
        table = {token: token.name for token in TokenType}
        for token in TokenType:
            restored = pickle.loads(pickle.dumps(token))
            self.assertIs(restored, token)
            self.assertEqual(table[restored], token.name)
            self.assertIs(TokenType(token.value), token)
            self.assertNotEqual(token, token.value)
        self.assertEqual(len(set(TokenType)), len(TokenType))

    def test_transforms_traverse_each_node_once(self):
        tree = sqlglot.parse_one(
            'WITH Foo AS (SELECT 1), Bar AS (SELECT * FROM foo) SELECT * FROM bar',
            read='postgres',
        )
        nodes = list(tree.walk())
        visits = []
        original = exp.Expression.iter_expressions

        def children(node, *args, **kwargs):
            visits.append(node)
            yield from original(node, *args, **kwargs)

        with patch.object(exp.Expression, 'iter_expressions', children):
            result = apply_e6_ast_transforms(tree)
        self.assertIs(result, tree)
        self.assertEqual(len(visits), len(nodes))
        self.assertEqual(sorted(t.name for t in tree.find_all(exp.Table)), ['Bar', 'Foo'])

    def test_transform_preserves_values_comments_and_qualified_tables(self):
        tree = sqlglot.parse_one(
            'WITH Foo AS (VALUES (1)) SELECT * FROM foo JOIN db.foo ON TRUE',
            read='postgres',
        )
        tree.comments = ['/* embedded */']
        result = apply_e6_ast_transforms(tree)
        self.assertEqual(result.comments, ['/ * embedded * /'])
        cte = next(result.find_all(exp.CTE))
        self.assertIsInstance(cte.this, exp.Select)
        self.assertIsInstance(cte.this.args['from'].this, exp.Values)
        self.assertEqual(sorted((t.db, t.name) for t in result.find_all(exp.Table)),
                         [('', 'Foo'), ('db', 'foo')])
        for node in result.walk():
            for child in node.iter_expressions():
                self.assertIs(child.parent, node)
