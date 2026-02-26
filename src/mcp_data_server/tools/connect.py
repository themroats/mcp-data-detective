"""Connection tools — connect_source, disconnect_source, list_sources."""

from __future__ import annotations

from typing import Any

from mcp_data_server.sources.registry import SourceRegistry


def connect_source(registry: SourceRegistry, name: str, source_type: str, path: str) -> dict[str, Any]:
    """Connect a data source (SQLite DB, Parquet file, or CSV).

    Args:
        name: A friendly alias for this source (e.g. 'sales', 'events').
        source_type: One of 'sqlite', 'parquet', 'csv'.
        path: File path. Supports globs for parquet/csv (e.g. './data/*.parquet').

    Returns:
        Connection confirmation with discovered tables.
    """
    source = registry.connect(name, source_type, path)
    return {
        "status": "connected",
        "name": source.name,
        "type": source.source_type.value,
        "path": source.path,
        "tables": source.tables,
        "table_count": len(source.tables),
    }


def disconnect_source(registry: SourceRegistry, name: str) -> dict[str, str]:
    """Disconnect a previously registered data source.

    Args:
        name: The alias of the source to disconnect.
    """
    registry.disconnect(name)
    return {"status": "disconnected", "name": name}


def list_sources(registry: SourceRegistry) -> dict[str, Any]:
    """List all connected data sources and their tables."""
    sources = registry.sources
    result = []
    total_tables = 0
    for src in sources.values():
        total_tables += len(src.tables)
        result.append(
            {
                "name": src.name,
                "type": src.source_type.value,
                "path": src.path,
                "tables": src.tables,
            }
        )
    return {"sources": result, "total_sources": len(result), "total_tables": total_tables}
