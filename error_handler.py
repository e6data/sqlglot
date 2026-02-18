"""
Error Handler for e6data Transpiler

Categorizes and formats errors for user-friendly display in the query editor.
Errors are classified into two categories:
- PARSING_FAILURE: SQL syntax errors, tokenization errors
- TRANSFORMATION_FAILURE: Unsupported features, dialect conversion errors
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
import re

from sqlglot.errors import ParseError, TokenError, UnsupportedError, OptimizeError


class ErrorCategory(str, Enum):
    PARSING_FAILURE = "PARSING_FAILURE"
    TRANSFORMATION_FAILURE = "TRANSFORMATION_FAILURE"


@dataclass
class TranspilerError:
    """Structured error response for the transpiler API."""

    category: ErrorCategory
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    highlight: Optional[str] = None
    suggestion: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        result = {
            "error_category": self.category.value,
            "message": self.message,
        }
        if self.line is not None:
            result["line"] = self.line
        if self.column is not None:
            result["column"] = self.column
        if self.highlight:
            result["highlight"] = self.highlight
        if self.suggestion:
            result["suggestion"] = self.suggestion
        return result

    def to_user_message(self) -> str:
        """Generate a clean user-facing error message."""
        parts = [f"[{self.category.value}] {self.message}"]

        if self.line is not None:
            location = f"Line {self.line}"
            if self.column is not None:
                location += f", Column {self.column}"
            parts.append(f"Location: {location}")

        if self.highlight:
            parts.append(f"Near: {self.highlight}")

        if self.suggestion:
            parts.append(f"Suggestion: {self.suggestion}")

        return " | ".join(parts)


def _extract_line_from_message(message: str) -> Optional[int]:
    """Try to extract line number from error message if not provided directly."""
    patterns = [
        r"line[:\s]+(\d+)",
        r"Line[:\s]+(\d+)",
        r"at line (\d+)",
        r":(\d+):\d+",  # file:line:col format
    ]
    for pattern in patterns:
        match = re.search(pattern, message)
        if match:
            return int(match.group(1))
    return None


def _extract_column_from_message(message: str) -> Optional[int]:
    """Try to extract column number from error message."""
    patterns = [
        r"col(?:umn)?[:\s]+(\d+)",
        r"Col(?:umn)?[:\s]+(\d+)",
        r":\d+:(\d+)",  # file:line:col format
    ]
    for pattern in patterns:
        match = re.search(pattern, message)
        if match:
            return int(match.group(1))
    return None


def _simplify_parse_error_message(message: str, highlight: str = None) -> str:
    """
    Simplify sqlglot parse error messages for end users.
    Removes internal details while keeping useful information.
    """
    # Handle "Required keyword: 'this' missing for <class ...>" pattern
    # 'this' in sqlglot refers to the required expression for a clause
    this_missing_match = re.search(
        r"Required keyword: 'this' missing for <class 'sqlglot\.expressions\.(\w+)'>",
        message
    )
    if this_missing_match:
        clause_name = this_missing_match.group(1).upper()
        return f"{clause_name} clause is incomplete"

    # Handle other "Required keyword" patterns
    keyword_missing_match = re.search(
        r"Required keyword: '(\w+)' missing for <class 'sqlglot\.expressions\.(\w+)'>",
        message
    )
    if keyword_missing_match:
        keyword = keyword_missing_match.group(1)
        clause_name = keyword_missing_match.group(2)
        return f"{clause_name} requires '{keyword}'"

    # Remove "sqlglot." prefixes and class references
    message = re.sub(r"sqlglot\.\w+\.", "", message)
    message = re.sub(r"<class '[^']+'>", "", message)

    # Clean up common verbose patterns
    simplifications = [
        (r"Expecting (\w+) but got (\w+)", r"Expected \1, found \2"),
        (r"Invalid expression / Unexpected token", "Unexpected token"),
        (r"Unexpected token\.", "Unexpected token"),
    ]

    for pattern, replacement in simplifications:
        message = re.sub(pattern, replacement, message, flags=re.IGNORECASE)

    return message.strip()


def _simplify_unsupported_error_message(message: str, from_dialect: str = None) -> str:
    """Simplify unsupported feature error messages."""
    # Remove internal class names
    message = re.sub(r"<class '[^']+'>", "", message)
    message = re.sub(r"sqlglot\.expressions\.\w+", "expression", message)

    if from_dialect:
        message = message.replace("is not supported", f"is not supported when converting from {from_dialect}")

    return message.strip()


def _extract_previous_token(start_context: str) -> Optional[str]:
    """
    Extract the last token from start_context.
    This is typically where the actual error is (e.g., 'FORM' instead of 'FROM').
    """
    if not start_context:
        return None
    tokens = start_context.strip().split()
    return tokens[-1] if tokens else None


def _get_previous_token_column(start_context: str, previous_token: str) -> Optional[int]:
    """Calculate the 1-indexed column position where the previous token starts."""
    if not start_context or not previous_token:
        return None
    # Find the last occurrence of the token
    idx = start_context.rfind(previous_token)
    if idx >= 0:
        return idx + 1  # Convert to 1-indexed
    return None


def handle_transpiler_error(
    error: Exception,
    from_dialect: str = None,
    stage: str = "unknown"
) -> TranspilerError:
    """
    Convert any exception into a structured TranspilerError.

    Args:
        error: The caught exception
        from_dialect: Source SQL dialect (e.g., 'databricks', 'spark')
        stage: Which stage the error occurred in ('parsing' or 'transformation')

    Returns:
        TranspilerError with categorized and formatted error information
    """
    message = str(error)
    line = None
    column = None
    highlight = None

    # Determine category and extract details based on error type
    if isinstance(error, (ParseError, TokenError)):
        category = ErrorCategory.PARSING_FAILURE

        # ParseError has structured error info
        if isinstance(error, ParseError) and error.errors:
            err_info = error.errors[0]
            line = err_info.get("line")
            description = err_info.get("description", "")
            sqlglot_highlight = err_info.get("highlight")
            start_context = err_info.get("start_context", "")

            # Simplify the message
            message = _simplify_parse_error_message(description if description else str(error), sqlglot_highlight)

            # Decide whether to use previous token or sqlglot's highlight
            # For "Unexpected token" errors, the actual problem is usually the previous token
            # For other errors (like incomplete clauses), use sqlglot's highlight
            if "unexpected token" in description.lower() or "invalid expression" in description.lower():
                previous_token = _extract_previous_token(start_context)
                if previous_token:
                    highlight = previous_token
                    column = _get_previous_token_column(start_context, previous_token)
                else:
                    highlight = sqlglot_highlight
                    column = err_info.get("col")
            else:
                # Use sqlglot's highlight for other error types
                highlight = sqlglot_highlight
                column = err_info.get("col")
        else:
            message = _simplify_parse_error_message(str(error))
            line = _extract_line_from_message(message)
            column = _extract_column_from_message(message)

    elif isinstance(error, UnsupportedError):
        category = ErrorCategory.TRANSFORMATION_FAILURE
        message = _simplify_unsupported_error_message(message, from_dialect)
        line = _extract_line_from_message(message)

    elif isinstance(error, OptimizeError):
        category = ErrorCategory.TRANSFORMATION_FAILURE
        message = f"Query optimization failed: {message}"
        line = _extract_line_from_message(message)

    else:
        # For unknown exceptions, categorize based on the stage hint
        if stage == "parsing":
            category = ErrorCategory.PARSING_FAILURE
        else:
            category = ErrorCategory.TRANSFORMATION_FAILURE

        # Try to extract location info from generic messages
        line = _extract_line_from_message(message)
        column = _extract_column_from_message(message)

    return TranspilerError(
        category=category,
        message=message,
        line=line,
        column=column,
        highlight=highlight,
        suggestion=None,
    )


def format_error_response(
    error: Exception,
    from_dialect: str = None,
    stage: str = "unknown",
) -> str:
    """
    Format an exception into a simple string error message for API response.

    Args:
        error: The caught exception
        from_dialect: Source SQL dialect
        stage: Which stage the error occurred in

    Returns:
        Formatted error string like:
        "Parsing_Error: Invalid expression / Unexpected token. Line 1, Col: 18. around \"FORM\""
    """
    transpiler_error = handle_transpiler_error(error, from_dialect, stage)

    # Build the error string
    category_str = transpiler_error.category.value.replace("_FAILURE", "_Error")

    parts = [f"{category_str}: {transpiler_error.message}."]

    if transpiler_error.line is not None:
        location = f"Line {transpiler_error.line}"
        if transpiler_error.column is not None:
            location += f", Col: {transpiler_error.column}"
        parts.append(f"{location}.")

    if transpiler_error.highlight:
        parts.append(f'around "{transpiler_error.highlight}"')

    return " ".join(parts)


def format_error_message(
    error: Exception,
    from_dialect: str = None,
    stage: str = "unknown"
) -> str:
    """
    Format an exception into a user-friendly string message.

    Args:
        error: The caught exception
        from_dialect: Source SQL dialect
        stage: Which stage the error occurred in

    Returns:
        Formatted error string for display
    """
    transpiler_error = handle_transpiler_error(error, from_dialect, stage)
    return transpiler_error.to_user_message()
