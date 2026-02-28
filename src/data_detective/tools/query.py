"""Query tools — query, list_tables, get_sample, get_schema."""

from __future__ import annotations

from typing import Any

from data_detective.sources.registry import SourceRegistry
from data_detective.validation import validate_identifier


def list_tables(registry: SourceRegistry) -> dict[str, Any]:
    """List all tables and views across all connected sources with their source type and path."""
    tables = registry.list_all_tables()
    return {"tables": tables, "count": len(tables)}


def get_schema(registry: SourceRegistry, table: str, source: str | None = None) -> dict[str, Any]:
    """Get the column names, types, and nullability for a table.

    Args:
        table: The table name to describe.
        source: (Optional) The source alias, required if the table name is ambiguous.

    Returns:
        Schema information with column details.
    """
    columns = registry.get_schema(table, source)
    return {"table": table, "columns": columns, "column_count": len(columns)}


def query(registry: SourceRegistry, sql: str, limit: int = 1000) -> dict[str, Any]:
    """Execute a SQL query against any connected data source.

    Uses DuckDB SQL syntax. Tables from SQLite sources are qualified as "source_name"."table_name".
    Parquet/CSV sources are available directly by their alias.

    Args:
        sql: The SQL query to execute.
        limit: Maximum rows to return (default 1000). A safety LIMIT is auto-appended if missing.

    Returns:
        Query results as a list of row dicts, plus row count and column names.
    """
    rows = registry.query(sql, limit=limit)
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows, "row_count": len(rows)}


def get_sample(registry: SourceRegistry, table: str, n: int = 10, source: str | None = None) -> dict[str, Any]:
    """Get a random sample of rows from a table.

    Args:
        table: The table to sample from.
        n: Number of rows to return (default 10).
        source: (Optional) Source alias for SQLite tables.

    Returns:
        Sample rows with column names.
    """
    table = validate_identifier(table, "table")
    if source:
        source = validate_identifier(source, "source")
        qualified = f'"{source}"."{ table}"'
    else:
        qualified = f'"{table}"'

    rows = registry.query(f"SELECT * FROM {qualified} USING SAMPLE {n}")
    columns = list(rows[0].keys()) if rows else []
    return {"table": table, "columns": columns, "rows": rows, "row_count": len(rows)}
