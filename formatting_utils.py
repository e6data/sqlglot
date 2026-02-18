"""
Formatting Utilities for SQL Transpilation

This module provides a simple wrapper to preserve original query formatting
after transpilation. It can be integrated into existing code with minimal changes.

Usage:
    from formatting_utils import preserve_formatting

    # Your existing transpilation code
    transpiled_query = tree.sql(dialect=to_sql, from_dialect=from_sql)

    # Add this one line to preserve formatting
    formatted_query = preserve_formatting(original_query, transpiled_query, from_sql, to_sql)
"""

from typing import Dict, List, Optional
from sqlglot import tokenize
from sqlglot.tokens import Token


def preserve_formatting(
    original_sql: str,
    transpiled_sql: str,
    source_dialect: Optional[str] = None,
    target_dialect: Optional[str] = None,
) -> str:
    """
    Preserve the original SQL formatting in the transpiled output.

    This function takes the original SQL (with its formatting) and the transpiled SQL,
    then reconstructs the transpiled SQL using the whitespace from the original.

    Args:
        original_sql: The original SQL query with formatting to preserve
        transpiled_sql: The transpiled SQL query (typically loses formatting)
        source_dialect: Source dialect (optional, for future use)
        target_dialect: Target dialect (optional, for future use)

    Returns:
        The transpiled SQL with original formatting preserved

    Example:
        >>> original = '''SELECT
        ...     col1,
        ...     col2
        ... FROM table1'''
        >>> transpiled = "SELECT col1, col2 FROM table1"
        >>> preserve_formatting(original, transpiled)
        'SELECT\\n    col1,\\n    col2\\nFROM table1'
    """
    if not original_sql or not transpiled_sql:
        return transpiled_sql

    # Get original tokens with whitespace
    try:
        original_tokens = list(tokenize(original_sql))
        transpiled_tokens = list(tokenize(transpiled_sql))
    except Exception:
        # If tokenization fails, return transpiled as-is
        return transpiled_sql

    # Build alignment between transpiled and original tokens
    alignment = _align_tokens(original_tokens, transpiled_tokens)

    # Reconstruct with original whitespace
    result_parts = []
    for i, token in enumerate(transpiled_tokens):
        orig_idx = alignment.get(i)

        if orig_idx is not None:
            # Use original whitespace
            ws = original_tokens[orig_idx].whitespace_before
            # For first token, strip leading whitespace to avoid extra indentation
            if i == 0:
                ws = ws.lstrip()
            result_parts.append(ws)
        else:
            # New token - use minimal spacing
            if i > 0:
                # Don't add space before commas or closing parens
                if token.token_type.name in ("COMMA", "R_PAREN", "R_BRACKET"):
                    pass  # No space
                # Don't add space after opening parens
                elif result_parts and result_parts[-1].endswith("("):
                    pass  # No space
                else:
                    result_parts.append(" ")

        # Handle token text (preserve quotes for strings)
        if token.token_type.name == "STRING":
            result_parts.append(f"'{token.text}'")
        else:
            result_parts.append(token.text)

    return "".join(result_parts)


def _align_tokens(
    original_tokens: List[Token],
    transpiled_tokens: List[Token],
) -> Dict[int, int]:
    """
    Align transpiled tokens to original tokens using position-aware matching.
    Returns dict mapping transpiled index -> original index.
    """
    alignment: Dict[int, int] = {}
    used_original: set = set()

    n_orig = len(original_tokens)
    n_trans = len(transpiled_tokens)

    # For each transpiled token, find all matching original tokens
    # Then pick the one closest in relative position
    for trans_idx, trans_token in enumerate(transpiled_tokens):
        trans_ratio = trans_idx / max(n_trans, 1)

        best_orig = None
        best_score = float("inf")

        for orig_idx, orig_token in enumerate(original_tokens):
            if orig_idx in used_original:
                continue

            # Check if tokens match (exact type and text)
            if (
                orig_token.token_type == trans_token.token_type
                and orig_token.text.upper() == trans_token.text.upper()
            ):
                # Score based on relative position difference
                orig_ratio = orig_idx / max(n_orig, 1)
                score = abs(trans_ratio - orig_ratio)

                if score < best_score:
                    best_score = score
                    best_orig = orig_idx

        if best_orig is not None:
            alignment[trans_idx] = best_orig
            used_original.add(best_orig)

    # Second pass: handle function renames (struct -> NAMED_STRUCT, etc.)
    for trans_idx, trans_token in enumerate(transpiled_tokens):
        if trans_idx in alignment:
            continue

        # Only handle function calls (VAR followed by L_PAREN)
        if trans_token.token_type.name not in ("VAR", "STRUCT"):
            continue
        if trans_idx + 1 >= n_trans:
            continue
        if transpiled_tokens[trans_idx + 1].token_type.name != "L_PAREN":
            continue

        trans_ratio = trans_idx / max(n_trans, 1)
        best_orig = None
        best_score = float("inf")

        for orig_idx, orig_token in enumerate(original_tokens):
            if orig_idx in used_original:
                continue
            if orig_token.token_type.name not in ("VAR", "STRUCT"):
                continue
            if orig_idx + 1 >= n_orig:
                continue
            if original_tokens[orig_idx + 1].token_type.name != "L_PAREN":
                continue

            orig_ratio = orig_idx / max(n_orig, 1)
            score = abs(trans_ratio - orig_ratio)

            if score < best_score:
                best_score = score
                best_orig = orig_idx

        if best_orig is not None and best_score < 0.2:
            alignment[trans_idx] = best_orig
            used_original.add(best_orig)

    # Third pass: structural tokens (parens, commas) by closest position
    structural_types = {"L_PAREN", "R_PAREN", "COMMA"}

    for trans_idx, trans_token in enumerate(transpiled_tokens):
        if trans_idx in alignment:
            continue
        if trans_token.token_type.name not in structural_types:
            continue

        trans_ratio = trans_idx / max(n_trans, 1)
        best_orig = None
        best_score = float("inf")

        for orig_idx, orig_token in enumerate(original_tokens):
            if orig_idx in used_original:
                continue
            if orig_token.token_type != trans_token.token_type:
                continue

            orig_ratio = orig_idx / max(n_orig, 1)
            score = abs(trans_ratio - orig_ratio)

            if score < best_score:
                best_score = score
                best_orig = orig_idx

        if best_orig is not None and best_score < 0.15:
            alignment[trans_idx] = best_orig
            used_original.add(best_orig)

    return alignment


# Convenience function with feature flag support
def transpile_with_formatting(
    original_sql: str,
    transpiled_sql: str,
    preserve_format: bool = True,
    source_dialect: Optional[str] = None,
    target_dialect: Optional[str] = None,
) -> str:
    """
    Wrapper that conditionally preserves formatting based on a flag.

    Args:
        original_sql: The original SQL query
        transpiled_sql: The transpiled SQL query
        preserve_format: If True, preserve original formatting. If False, return transpiled as-is.
        source_dialect: Source dialect (optional)
        target_dialect: Target dialect (optional)

    Returns:
        Formatted or unformatted transpiled SQL based on preserve_format flag
    """
    if not preserve_format:
        return transpiled_sql

    return preserve_formatting(original_sql, transpiled_sql, source_dialect, target_dialect)
