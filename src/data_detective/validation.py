"""Validation utilities for sanitizing SQL identifiers and file paths."""

from __future__ import annotations

import re


# Identifiers must be alphanumeric (including underscores), with optional dots,
# hyphens, and spaces — all of which DuckDB handles when double-quoted.
# This blocks SQL injection characters like ; ' " -- /* etc.
_SAFE_IDENTIFIER_RE = re.compile(r"^[\w][\w\s\-.]*$", re.UNICODE)

# Additionally block SQL comment sequences that the base regex allows.
_DANGEROUS_IDENTIFIER_RE = re.compile(r"--|/\*|\*/")

# Paths may contain drive letters, slashes, dots, hyphens, spaces, and globs.
# Block sequences that could escape a single-quoted SQL string literal.
# We allow /* since globs like ./data/*.parquet are legitimate.
_DANGEROUS_PATH_CHARS_RE = re.compile(r"--")


def validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is safe to use as a SQL identifier.

    Allows alphanumeric characters, underscores, hyphens, spaces, and dots.
    Rejects anything that could be used for SQL injection (semicolons,
    quotes, comment sequences, etc.).

    Args:
        value: The identifier string to validate.
        label: A human-readable label for error messages (e.g. 'source name', 'table').

    Returns:
        The validated identifier string (stripped of leading/trailing whitespace).

    Raises:
        ValueError: If the identifier contains unsafe characters.
    """
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"Invalid {label}: must not be empty.")
    if not _SAFE_IDENTIFIER_RE.match(stripped):
        raise ValueError(
            f"Invalid {label}: '{stripped}' contains disallowed characters. "
            f"Only letters, digits, underscores, hyphens, spaces, and dots are allowed."
        )
    if _DANGEROUS_IDENTIFIER_RE.search(stripped):
        raise ValueError(
            f"Invalid {label}: '{stripped}' contains disallowed character sequences."
        )
    return stripped


def validate_path(value: str, label: str = "path") -> str:
    """Validate that a file path is safe to interpolate into a SQL string literal.

    Blocks characters/sequences that could break out of a single-quoted SQL string:
    semicolons, quotes, double-dash comments, C-style comments.

    Args:
        value: The path string to validate.
        label: A human-readable label for error messages.

    Returns:
        The validated path string.

    Raises:
        ValueError: If the path contains dangerous characters.
    """
    if not value.strip():
        raise ValueError(f"Invalid {label}: must not be empty.")
    if "'" in value or '"' in value or ";" in value:
        raise ValueError(
            f"Invalid {label}: '{value}' contains disallowed characters (quotes or semicolons)."
        )
    if _DANGEROUS_PATH_CHARS_RE.search(value):
        raise ValueError(
            f"Invalid {label}: '{value}' contains disallowed character sequences."
        )
    return value
