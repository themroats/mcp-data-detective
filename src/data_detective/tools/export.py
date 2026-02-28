"""Export tools — export_data."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from data_detective.sources.registry import SourceRegistry


def export_data(
    registry: SourceRegistry,
    sql: str,
    output_path: str,
    format: str = "parquet",
) -> dict[str, Any]:
    """Export the result of a SQL query to a Parquet or CSV file.

    Args:
        sql: The SQL query whose results to export.
        output_path: File path for the exported file.
        format: Output format — 'parquet' or 'csv' (default 'parquet').

    Returns:
        Export confirmation with file path and row count.
    """
    fmt = format.lower()
    if fmt not in ("parquet", "csv"):
        raise ValueError(f"Unsupported format '{fmt}'. Use 'parquet' or 'csv'.")

    resolved = str(Path(output_path).resolve())
    Path(resolved).parent.mkdir(parents=True, exist_ok=True)

    trimmed = sql.strip().rstrip(";")

    if fmt == "parquet":
        registry.execute_raw(
            f"COPY ({trimmed}) TO '{resolved}' (FORMAT PARQUET)"
        )
    else:
        registry.execute_raw(
            f"COPY ({trimmed}) TO '{resolved}' (FORMAT CSV, HEADER)"
        )

    # Get row count of what we exported
    row_count = registry.execute_raw(f"SELECT COUNT(*) FROM ({trimmed})").fetchone()[0]
    file_size = Path(resolved).stat().st_size

    return {
        "status": "exported",
        "path": resolved,
        "format": fmt,
        "row_count": row_count,
        "file_size_bytes": file_size,
    }
