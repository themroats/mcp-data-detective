"""Profiling tools — profile_table, summarize."""

from __future__ import annotations

from typing import Any

from data_detective.sources.registry import SourceRegistry
from data_detective.validation import validate_identifier


def profile_table(
    registry: SourceRegistry,
    table: str,
    source: str | None = None,
) -> dict[str, Any]:
    """Generate a detailed profile of a table: row count, column stats, null rates, and unique counts.

    Args:
        table: The table to profile.
        source: (Optional) Source alias for SQLite tables.

    Returns:
        A profile report with per-column statistics.
    """
    table = validate_identifier(table, "table")
    if source:
        source = validate_identifier(source, "source")
    qualified = f'"{source}"."{table}"' if source else f'"{table}"'

    # Row count
    row_count = registry.execute_raw(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]

    # Column metadata
    schema = registry.get_schema(table, source)

    column_profiles = []
    for col_info in schema:
        col = col_info["column"]
        col_type = col_info["type"]
        safe_col = f'"{col}"'

        stats: dict[str, Any] = {
            "column": col,
            "type": col_type,
        }

        # Null count/rate
        null_count = registry.execute_raw(
            f"SELECT COUNT(*) FROM {qualified} WHERE {safe_col} IS NULL"
        ).fetchone()[0]
        stats["null_count"] = null_count
        stats["null_rate"] = round(null_count / row_count, 4) if row_count > 0 else 0

        # Distinct count
        distinct = registry.execute_raw(
            f"SELECT COUNT(DISTINCT {safe_col}) FROM {qualified}"
        ).fetchone()[0]
        stats["distinct_count"] = distinct
        stats["unique_rate"] = round(distinct / row_count, 4) if row_count > 0 else 0

        # Numeric stats
        type_lower = col_type.lower()
        is_numeric = any(
            t in type_lower
            for t in ["int", "float", "double", "decimal", "numeric", "bigint", "smallint", "real"]
        )

        if is_numeric:
            agg = registry.execute_raw(
                f"SELECT MIN({safe_col}), MAX({safe_col}), "
                f"AVG({safe_col}), MEDIAN({safe_col}), STDDEV({safe_col}) "
                f"FROM {qualified}"
            ).fetchone()
            stats["min"] = _safe_num(agg[0])
            stats["max"] = _safe_num(agg[1])
            stats["mean"] = _safe_num(agg[2])
            stats["median"] = _safe_num(agg[3])
            stats["stddev"] = _safe_num(agg[4])

        # Top values for low-cardinality columns
        if distinct <= 20 and row_count > 0:
            top_vals = registry.execute_raw(
                f"SELECT {safe_col} AS val, COUNT(*) AS cnt FROM {qualified} "
                f"GROUP BY {safe_col} ORDER BY cnt DESC LIMIT 10"
            ).fetchall()
            stats["top_values"] = [{"value": str(v[0]), "count": v[1]} for v in top_vals]

        column_profiles.append(stats)

    return {
        "table": table,
        "row_count": row_count,
        "column_count": len(schema),
        "columns": column_profiles,
    }


def summarize(registry: SourceRegistry) -> dict[str, Any]:
    """Generate a high-level summary of all connected data: source count, table count, total rows, and schemas.

    Returns:
        A summary dict with per-source breakdowns.
    """
    sources_summary = []
    total_rows = 0

    for src in registry.sources.values():
        src_info: dict[str, Any] = {
            "name": src.name,
            "type": src.source_type.value,
            "tables": [],
        }
        for table in src.tables:
            qualified = (
                f'"{src.name}"."{table}"'
                if src.source_type.value == "sqlite"
                else f'"{table}"'
            )
            try:
                row_count = registry.execute_raw(
                    f"SELECT COUNT(*) FROM {qualified}"
                ).fetchone()[0]
            except Exception:
                row_count = -1

            schema = registry.get_schema(table, src.name)
            src_info["tables"].append(
                {
                    "name": table,
                    "row_count": row_count,
                    "column_count": len(schema),
                    "columns": [c["column"] for c in schema],
                }
            )
            if row_count > 0:
                total_rows += row_count

        sources_summary.append(src_info)

    return {
        "total_sources": len(sources_summary),
        "total_tables": sum(len(s["tables"]) for s in sources_summary),
        "total_rows": total_rows,
        "sources": sources_summary,
    }


def _safe_num(value: Any) -> Any:
    """Convert numeric value to a JSON-safe type."""
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return str(value)
