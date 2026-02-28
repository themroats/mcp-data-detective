"""
Data Detective — main server entry point.

Registers all tools with the MCP framework and handles the server lifecycle.
"""

from __future__ import annotations

import functools
import json
import logging
import os
from typing import Any, Callable

from mcp.server.fastmcp import FastMCP

from data_detective import DEFAULT_QUERY_LIMIT
from data_detective.sources.registry import SourceRegistry
from data_detective.tools import connect as connect_tools
from data_detective.tools import export as export_tools
from data_detective.tools import profile as profile_tools
from data_detective.tools import quality as quality_tools
from data_detective.tools import query as query_tools

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("data-detective")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json(result: Any) -> str:
    """Serialize a tool result to JSON."""
    return json.dumps(result, indent=2, default=str)


def _error(exc: Exception) -> str:
    """Return a clean JSON error payload instead of a raw traceback."""
    return json.dumps({"error": type(exc).__name__, "message": str(exc)}, indent=2)


def _tool_handler(fn: Callable[..., Any]) -> Callable[..., str]:
    """Decorator that wraps a tool function with JSON serialization and error handling."""
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> str:
        try:
            return _json(fn(*args, **kwargs))
        except Exception as exc:
            logger.exception("%s failed", fn.__name__)
            return _error(exc)
    return wrapper

# ---------------------------------------------------------------------------
# Server & shared state
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "data-detective",
    instructions=(
        "An MCP server that gives AI assistants the ability to connect to, "
        "query, profile, and monitor data sources — turning any LLM into an "
        "interactive data engineering copilot."
    ),
)

registry = SourceRegistry()

# ---------------------------------------------------------------------------
# Connection tools
# ---------------------------------------------------------------------------


@mcp.tool()
@_tool_handler
def connect_source(name: str, source_type: str, path: str) -> str:
    """Connect a data source (SQLite DB, Parquet file, or CSV).

    Args:
        name: A friendly alias for this source (e.g. 'sales', 'events').
        source_type: One of 'sqlite', 'parquet', 'csv'.
        path: File path. Supports globs for parquet/csv (e.g. './data/*.parquet').
    """
    return connect_tools.connect_source(registry, name, source_type, path)


@mcp.tool()
@_tool_handler
def disconnect_source(name: str) -> str:
    """Disconnect a previously registered data source.

    Args:
        name: The alias of the source to disconnect.
    """
    return connect_tools.disconnect_source(registry, name)


@mcp.tool()
@_tool_handler
def list_sources() -> str:
    """List all connected data sources and their tables."""
    return connect_tools.list_sources(registry)


# ---------------------------------------------------------------------------
# Query tools
# ---------------------------------------------------------------------------


@mcp.tool()
@_tool_handler
def list_tables() -> str:
    """List all tables and views across all connected sources."""
    return query_tools.list_tables(registry)


@mcp.tool()
@_tool_handler
def get_table_schema(table: str, source: str | None = None) -> str:
    """Get column names, types, and nullability for a table.

    Args:
        table: The table name to describe.
        source: (Optional) Source alias, needed for SQLite tables.
    """
    return query_tools.get_schema(registry, table, source)


@mcp.tool()
@_tool_handler
def run_query(sql: str, limit: int = DEFAULT_QUERY_LIMIT) -> str:
    """Execute a SQL query against any connected data source.

    Uses DuckDB SQL. SQLite tables: "source_name"."table_name". Parquet/CSV: use the alias directly.

    Args:
        sql: The SQL query to execute.
        limit: Max rows to return (default 1000).
    """
    return query_tools.query(registry, sql, limit=limit)


@mcp.tool()
@_tool_handler
def get_sample(table: str, n: int = 10, source: str | None = None) -> str:
    """Get a random sample of rows from a table.

    Args:
        table: The table to sample from.
        n: Number of rows (default 10).
        source: (Optional) Source alias for SQLite tables.
    """
    return query_tools.get_sample(registry, table, n, source)


# ---------------------------------------------------------------------------
# Profiling tools
# ---------------------------------------------------------------------------


@mcp.tool()
@_tool_handler
def profile_table(table: str, source: str | None = None) -> str:
    """Generate a detailed profile of a table: row count, null rates, distributions, and per-column stats.

    Args:
        table: The table to profile.
        source: (Optional) Source alias for SQLite tables.
    """
    return profile_tools.profile_table(registry, table, source)


@mcp.tool()
@_tool_handler
def summarize() -> str:
    """High-level summary of all connected data: source count, table count, total rows, and column names."""
    return profile_tools.summarize(registry)


# ---------------------------------------------------------------------------
# Quality tools
# ---------------------------------------------------------------------------


@mcp.tool()
@_tool_handler
def detect_quality_issues(table: str, source: str | None = None) -> str:
    """Scan a table for data quality issues: duplicates, high null rates, constant columns, unexpected negatives.

    Args:
        table: The table to scan.
        source: (Optional) Source alias for SQLite tables.
    """
    return quality_tools.detect_quality_issues(registry, table, source)


@mcp.tool()
@_tool_handler
def detect_anomalies(
    table: str,
    column: str,
    time_column: str | None = None,
    source: str | None = None,
    z_threshold: float = 3.0,
) -> str:
    """Detect statistical anomalies in a numeric column using z-scores.

    Optionally aggregates by day if a time_column is provided to spot unusual trends.

    Args:
        table: The table to analyze.
        column: The numeric column to check.
        time_column: (Optional) Timestamp column to aggregate by day.
        source: (Optional) Source alias for SQLite tables.
        z_threshold: Z-score threshold (default 3.0).
    """
    return quality_tools.detect_anomalies(
        registry, table, column, time_column, source, z_threshold
    )


@mcp.tool()
@_tool_handler
def compare_schemas(
    table_a: str,
    table_b: str,
    source_a: str | None = None,
    source_b: str | None = None,
) -> str:
    """Compare schemas of two tables and report added, removed, or type-changed columns.

    Args:
        table_a: First table name.
        table_b: Second table name.
        source_a: (Optional) Source alias for first table.
        source_b: (Optional) Source alias for second table.
    """
    return quality_tools.compare_schemas(registry, table_a, table_b, source_a, source_b)


# ---------------------------------------------------------------------------
# Export tools
# ---------------------------------------------------------------------------


@mcp.tool()
@_tool_handler
def export_data(sql: str, output_path: str, format: str = "parquet") -> str:
    """Export SQL query results to a Parquet or CSV file.

    Args:
        sql: The SQL query whose results to export.
        output_path: File path for the output file.
        format: 'parquet' or 'csv' (default 'parquet').
    """
    return export_tools.export_data(registry, sql, output_path, format)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run Data Detective."""
    logger.info("Starting Data Detective...")
    mcp.run()


if __name__ == "__main__":
    main()
