"""
Data Detective — main server entry point.

Registers all tools with the MCP framework and handles the server lifecycle.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mcp.server.fastmcp import FastMCP

from data_detective.sources.registry import SourceRegistry
from data_detective.tools import connect as connect_tools
from data_detective.tools import export as export_tools
from data_detective.tools import profile as profile_tools
from data_detective.tools import quality as quality_tools
from data_detective.tools import query as query_tools

logging.basicConfig(level=logging.INFO)
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
def connect_source(name: str, source_type: str, path: str) -> str:
    """Connect a data source (SQLite DB, Parquet file, or CSV).

    Args:
        name: A friendly alias for this source (e.g. 'sales', 'events').
        source_type: One of 'sqlite', 'parquet', 'csv'.
        path: File path. Supports globs for parquet/csv (e.g. './data/*.parquet').
    """
    try:
        return _json(connect_tools.connect_source(registry, name, source_type, path))
    except Exception as exc:
        logger.exception("connect_source failed")
        return _error(exc)


@mcp.tool()
def disconnect_source(name: str) -> str:
    """Disconnect a previously registered data source.

    Args:
        name: The alias of the source to disconnect.
    """
    try:
        return _json(connect_tools.disconnect_source(registry, name))
    except Exception as exc:
        logger.exception("disconnect_source failed")
        return _error(exc)


@mcp.tool()
def list_sources() -> str:
    """List all connected data sources and their tables."""
    try:
        return _json(connect_tools.list_sources(registry))
    except Exception as exc:
        logger.exception("list_sources failed")
        return _error(exc)


# ---------------------------------------------------------------------------
# Query tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_tables() -> str:
    """List all tables and views across all connected sources."""
    try:
        return _json(query_tools.list_tables(registry))
    except Exception as exc:
        logger.exception("list_tables failed")
        return _error(exc)


@mcp.tool()
def get_table_schema(table: str, source: str | None = None) -> str:
    """Get column names, types, and nullability for a table.

    Args:
        table: The table name to describe.
        source: (Optional) Source alias, needed for SQLite tables.
    """
    try:
        return _json(query_tools.get_schema(registry, table, source))
    except Exception as exc:
        logger.exception("get_table_schema failed")
        return _error(exc)


@mcp.tool()
def run_query(sql: str, limit: int = 1000) -> str:
    """Execute a SQL query against any connected data source.

    Uses DuckDB SQL. SQLite tables: "source_name"."table_name". Parquet/CSV: use the alias directly.

    Args:
        sql: The SQL query to execute.
        limit: Max rows to return (default 1000).
    """
    try:
        return _json(query_tools.query(registry, sql, limit=limit))
    except Exception as exc:
        logger.exception("run_query failed")
        return _error(exc)


@mcp.tool()
def get_sample(table: str, n: int = 10, source: str | None = None) -> str:
    """Get a random sample of rows from a table.

    Args:
        table: The table to sample from.
        n: Number of rows (default 10).
        source: (Optional) Source alias for SQLite tables.
    """
    try:
        return _json(query_tools.get_sample(registry, table, n, source))
    except Exception as exc:
        logger.exception("get_sample failed")
        return _error(exc)


# ---------------------------------------------------------------------------
# Profiling tools
# ---------------------------------------------------------------------------


@mcp.tool()
def profile_table(table: str, source: str | None = None) -> str:
    """Generate a detailed profile of a table: row count, null rates, distributions, and per-column stats.

    Args:
        table: The table to profile.
        source: (Optional) Source alias for SQLite tables.
    """
    try:
        return _json(profile_tools.profile_table(registry, table, source))
    except Exception as exc:
        logger.exception("profile_table failed")
        return _error(exc)


@mcp.tool()
def summarize() -> str:
    """High-level summary of all connected data: source count, table count, total rows, and column names."""
    try:
        return _json(profile_tools.summarize(registry))
    except Exception as exc:
        logger.exception("summarize failed")
        return _error(exc)


# ---------------------------------------------------------------------------
# Quality tools
# ---------------------------------------------------------------------------


@mcp.tool()
def detect_quality_issues(table: str, source: str | None = None) -> str:
    """Scan a table for data quality issues: duplicates, high null rates, constant columns, unexpected negatives.

    Args:
        table: The table to scan.
        source: (Optional) Source alias for SQLite tables.
    """
    try:
        return _json(quality_tools.detect_quality_issues(registry, table, source))
    except Exception as exc:
        logger.exception("detect_quality_issues failed")
        return _error(exc)


@mcp.tool()
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
    try:
        return _json(
            quality_tools.detect_anomalies(
                registry, table, column, time_column, source, z_threshold
            )
        )
    except Exception as exc:
        logger.exception("detect_anomalies failed")
        return _error(exc)


@mcp.tool()
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
    try:
        return _json(
            quality_tools.compare_schemas(registry, table_a, table_b, source_a, source_b)
        )
    except Exception as exc:
        logger.exception("compare_schemas failed")
        return _error(exc)


# ---------------------------------------------------------------------------
# Export tools
# ---------------------------------------------------------------------------


@mcp.tool()
def export_data(sql: str, output_path: str, format: str = "parquet") -> str:
    """Export SQL query results to a Parquet or CSV file.

    Args:
        sql: The SQL query whose results to export.
        output_path: File path for the output file.
        format: 'parquet' or 'csv' (default 'parquet').
    """
    try:
        return _json(export_tools.export_data(registry, sql, output_path, format))
    except Exception as exc:
        logger.exception("export_data failed")
        return _error(exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run Data Detective."""
    logger.info("Starting Data Detective...")
    mcp.run()


if __name__ == "__main__":
    main()
