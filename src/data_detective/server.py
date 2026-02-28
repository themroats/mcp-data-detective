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

from mcp.server.fastmcp import FastMCP, Image

from data_detective import DEFAULT_QUERY_LIMIT
from data_detective.sources.registry import SourceRegistry
from data_detective.tools import connect as connect_tools
from data_detective.tools import export as export_tools
from data_detective.tools import profile as profile_tools
from data_detective.tools import quality as quality_tools
from data_detective.tools import chart as chart_tools
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
# Chart tools
# ---------------------------------------------------------------------------


@mcp.tool()
def create_chart(
    sql: str,
    chart_type: str,
    x: str,
    y: str,
    title: str | None = None,
    color: str | None = None,
    output_path: str | None = None,
):
    """Run a SQL query and render the results as a chart image.

    The chart recipe is held in memory so you can save it later with save_recipe.

    Args:
        sql: SQL query that produces the data to chart.
        chart_type: One of 'bar', 'line', 'scatter', 'histogram'.
        x: Column name for the x-axis.
        y: Column name for the y-axis.
        title: Optional chart title.
        color: Optional column name to group/color by.
        output_path: Optional file path for the chart image.
    """
    try:
        result = chart_tools.create_chart(
            registry, sql, chart_type, x, y, title, color, output_path
        )
        return [Image(path=result["chart_path"]), _json(result)]
    except Exception as exc:
        logger.exception("%s failed", "create_chart")
        return [_error(exc)]


@mcp.tool()
@_tool_handler
def save_recipe(
    name: str | None = None,
    output_dir: str | None = None,
    index: int = -1,
) -> str:
    """Persist the most recent chart recipe to disk as a JSON file.

    Args:
        name: Optional name for the recipe file.
        output_dir: Directory to save to (default: ./charts/).
        index: Recipe index in history (-1 = most recent).
    """
    return chart_tools.save_recipe(name, output_dir, index)


@mcp.tool()
def replay_chart(
    recipe_path: str,
    output_path: str | None = None,
):
    """Regenerate a chart from a previously saved recipe file.

    Re-runs the original SQL against the connected sources and re-renders the chart.

    Args:
        recipe_path: Path to a .recipe.json file.
        output_path: Optional override path for the chart image.
    """
    try:
        result = chart_tools.replay_chart(registry, recipe_path, output_path)
        return [Image(path=result["chart_path"]), _json(result)]
    except Exception as exc:
        logger.exception("%s failed", "replay_chart")
        return [_error(exc)]


@mcp.tool()
@_tool_handler
def export_insight(
    recipe_path: str | None = None,
    export_format: str = "script",
    output_path: str | None = None,
    index: int = -1,
) -> str:
    """Export a chart recipe as a standalone Python script, SQL file, or Jupyter notebook.

    Can use either a saved recipe file or whichever recipe is held in memory.

    Args:
        recipe_path: Path to a .recipe.json. If omitted, uses the in-memory recipe.
        export_format: One of 'script', 'sql', 'notebook'.
        output_path: Optional output file path.
        index: Recipe index in history if using in-memory recipe (-1 = most recent).
    """
    return chart_tools.export_insight(recipe_path, export_format, output_path, index)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run Data Detective."""
    logger.info("Starting Data Detective...")
    mcp.run()


if __name__ == "__main__":
    main()
