"""Reuse generic discovery tokens only where PostgreSQL lexing is identical.

This is a conservative optimization, not a hybrid tokenizer. SQL containing a
lexical difference, comments, or changed preprocessing uses the normal tokenizer.
Python tokens are request-owned: each region consumes its disjoint source tokens
once, adjusting their positions in place after structural discovery is complete.
"""

from bisect import bisect_left
from functools import lru_cache
import re

from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Token, TokenType

from apis.utils.multidialect import MARKER


@lru_cache(maxsize=1)
def _different_words():
    generic = Dialect().tokenizer_class.KEYWORDS
    postgres = Dialect.get_or_raise("postgres").tokenizer_class.KEYWORDS
    return frozenset(
        keyword.split()[0] for keyword in generic.keys() | postgres.keys()
        if generic.get(keyword) != postgres.get(keyword)
    )


_UNSAFE_SYMBOLS = re.compile(r"[$~@|]|--|/\*")
_QUOTED = frozenset((TokenType.STRING, TokenType.IDENTIFIER))


class RegionTokens:
    def __init__(self, sql, tokens):
        self.sql = sql
        self.tokens = tokens
        self.starts = [token.start for token in tokens]
        self.consumed = set()

    def mask(self, node, body):
        """Return PG-compatible tokens for the exact masked body, or None.

        Preserve the current string-mask semantics. If substring replacement
        doesn't correspond to the captured span (e.g. duplicate text in a literal),
        do not reuse tokens. Offsets and error locations must describe *body*.
        """
        span = (node.start, node.end)
        if span in self.consumed or _UNSAFE_SYMBOLS.search(body):
            return None
        replacements = []
        for index, child in enumerate(node.children):
            if node.raw.find(child.raw) != child.start - node.start:
                return None
            replacements.append((child.start, child.end, f"(SELECT NULL AS {MARKER.format(index)})"))
        replacements.sort()
        pieces = []
        cursor = node.start
        for start, end, replacement in replacements:
            if start < cursor:
                return None
            pieces.extend((self.sql[cursor:start], replacement))
            cursor = end
        pieces.append(self.sql[cursor:node.end])
        if "".join(pieces) != body:
            return None
        self.consumed.add(span)

        newlines = [match.start() for match in re.finditer(r"\r(?!\n)|\n", body)]
        result = []
        different_words = _different_words()
        compatible = True

        def append(tokens, shift):
            nonlocal compatible
            if not compatible:
                return
            for token in tokens:
                if token.token_type not in _QUOTED:
                    word = token.text.upper()
                    if word in different_words or (
                        word in ("B", "E", "X")
                        and self.sql[token.end + 1:token.end + 2] == "'"
                    ):
                        compatible = False
                        return
                end = token.end + shift
                start = token.start + shift
                line = bisect_left(newlines, end) + 1
                col = end - (newlines[line - 2] if line > 1 else -1)
                whitespace = (
                    body[result[-1].end + 1 if result else 0:start]
                    if hasattr(token, "whitespace_before") else ""
                )
                if isinstance(token, Token):
                    token.start = start
                    token.end = end
                    token.line = line
                    token.col = col
                    token.whitespace_before = whitespace
                    result.append(token)
                else:
                    result.append(Token(
                        token.token_type, token.text, line, col,
                        start, end, token.comments, whitespace,
                    ))

        cursor = node.start
        shift = -node.start
        for start, end, replacement in replacements:
            append(self.tokens[bisect_left(self.starts, cursor):bisect_left(self.starts, start)], shift)
            append(Dialect().tokenize(replacement), start + shift)
            shift += len(replacement) - (end - start)
            cursor = end
        append(self.tokens[bisect_left(self.starts, cursor):bisect_left(self.starts, node.end)], shift)
        return result if compatible else None
