"""Data quality tools — detect_quality_issues, detect_anomalies, compare_schemas."""

from __future__ import annotations

from typing import Any

from data_detective.sources.registry import SourceRegistry


def detect_quality_issues(
    registry: SourceRegistry,
    table: str,
    source: str | None = None,
) -> dict[str, Any]:
    """Scan a table for common data quality issues.

    Checks for:
    - High null rates (>5%)
    - Duplicate rows
    - Columns with a single constant value
    - Negative values in likely-positive columns (price, amount, quantity, count)
    - Potential type mismatches (future expansion)

    Args:
        table: The table to scan.
        source: (Optional) Source alias for SQLite tables.

    Returns:
        A list of detected issues with severity and details.
    """
    qualified = f'"{source}"."{table}"' if source else f'"{table}"'
    schema = registry.get_schema(table, source)
    row_count = registry.execute_raw(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]

    if row_count == 0:
        return {"table": table, "row_count": 0, "issues": [], "issue_count": 0}

    issues: list[dict[str, Any]] = []

    # --- Check for exact duplicate rows ---
    col_list = ", ".join(f'"{c["column"]}"' for c in schema)
    dup_count = registry.execute_raw(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT {col_list}, COUNT(*) AS cnt FROM {qualified} "
        f"  GROUP BY {col_list} HAVING cnt > 1"
        f")"
    ).fetchone()[0]
    if dup_count > 0:
        issues.append(
            {
                "type": "duplicates",
                "severity": "high" if dup_count > row_count * 0.01 else "medium",
                "message": f"Found {dup_count} groups of exact duplicate rows",
                "duplicate_groups": dup_count,
            }
        )

    # --- Check for semantic duplicates (excluding ID-like columns) ---
    id_suffixes = ("_id", "id")
    non_id_cols = [
        c["column"] for c in schema if not c["column"].lower().endswith(id_suffixes)
    ]
    if non_id_cols and len(non_id_cols) < len(schema):
        non_id_list = ", ".join(f'"{c}"' for c in non_id_cols)
        sem_dup_count = registry.execute_raw(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT {non_id_list}, COUNT(*) AS cnt FROM {qualified} "
            f"  GROUP BY {non_id_list} HAVING cnt > 1"
            f")"
        ).fetchone()[0]
        if sem_dup_count > 0 and sem_dup_count != dup_count:
            issues.append(
                {
                    "type": "semantic_duplicates",
                    "severity": "high" if sem_dup_count > row_count * 0.01 else "medium",
                    "message": (
                        f"Found {sem_dup_count} groups of rows with identical "
                        f"values (excluding ID columns)"
                    ),
                    "duplicate_groups": sem_dup_count,
                    "columns_checked": non_id_cols,
                }
            )

    for col_info in schema:
        col = col_info["column"]
        col_type = col_info["type"].lower()
        safe_col = f'"{col}"'

        # --- High null rate ---
        null_count = registry.execute_raw(
            f"SELECT COUNT(*) FROM {qualified} WHERE {safe_col} IS NULL"
        ).fetchone()[0]
        null_rate = null_count / row_count
        if null_rate > 0.05:
            issues.append(
                {
                    "type": "high_null_rate",
                    "severity": "high" if null_rate > 0.3 else "medium",
                    "column": col,
                    "message": f"Column '{col}' has {null_rate:.1%} null values ({null_count:,} rows)",
                    "null_rate": round(null_rate, 4),
                    "null_count": null_count,
                }
            )

        # --- Constant columns ---
        distinct = registry.execute_raw(
            f"SELECT COUNT(DISTINCT {safe_col}) FROM {qualified}"
        ).fetchone()[0]
        if distinct == 1 and row_count > 1:
            issues.append(
                {
                    "type": "constant_column",
                    "severity": "low",
                    "column": col,
                    "message": f"Column '{col}' has a single constant value across all {row_count:,} rows",
                }
            )

        # --- Negative values in likely-positive columns ---
        is_numeric = any(
            t in col_type for t in ["int", "float", "double", "decimal", "numeric", "real"]
        )
        positive_keywords = ["price", "amount", "total", "quantity", "qty", "count", "cost", "revenue", "fee"]
        likely_positive = any(kw in col.lower() for kw in positive_keywords)

        if is_numeric and likely_positive:
            neg_count = registry.execute_raw(
                f"SELECT COUNT(*) FROM {qualified} WHERE {safe_col} < 0"
            ).fetchone()[0]
            if neg_count > 0:
                issues.append(
                    {
                        "type": "unexpected_negatives",
                        "severity": "high",
                        "column": col,
                        "message": f"Column '{col}' has {neg_count:,} negative values (expected positive)",
                        "negative_count": neg_count,
                    }
                )

    return {
        "table": table,
        "row_count": row_count,
        "issues": issues,
        "issue_count": len(issues),
    }


def detect_anomalies(
    registry: SourceRegistry,
    table: str,
    column: str,
    time_column: str | None = None,
    source: str | None = None,
    z_threshold: float = 3.0,
) -> dict[str, Any]:
    """Detect statistical anomalies in a numeric column.

    Uses z-score method to identify outlier values. If a time_column is provided,
    anomalies are detected in time-aggregated (daily) data to spot unusual trends.

    Args:
        table: The table to analyze.
        column: The numeric column to check for anomalies.
        time_column: (Optional) A timestamp/date column to aggregate by day.
        source: (Optional) Source alias for SQLite tables.
        z_threshold: Z-score threshold for anomaly detection (default 3.0).

    Returns:
        Detected anomalies with z-scores and context.
    """
    qualified = f'"{source}"."{table}"' if source else f'"{table}"'
    safe_col = f'"{column}"'

    if time_column:
        # Time-series anomaly detection: aggregate by day, then find outlier days
        safe_time = f'"{time_column}"'
        agg_query = (
            f"SELECT CAST({safe_time} AS DATE) AS day, "
            f"SUM({safe_col}) AS daily_value, COUNT(*) AS daily_count "
            f"FROM {qualified} WHERE {safe_col} IS NOT NULL "
            f"GROUP BY day ORDER BY day"
        )
        rows = registry.query(agg_query, limit=10000)

        if len(rows) < 3:
            return {"table": table, "column": column, "anomalies": [], "message": "Not enough data points for anomaly detection"}

        values = [r["daily_value"] for r in rows]
        mean = sum(values) / len(values)
        stddev = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5

        anomalies = []
        if stddev > 0:
            for row in rows:
                z = (row["daily_value"] - mean) / stddev
                if abs(z) >= z_threshold:
                    anomalies.append(
                        {
                            "day": str(row["day"]),
                            "value": row["daily_value"],
                            "count": row["daily_count"],
                            "z_score": round(z, 2),
                            "direction": "above" if z > 0 else "below",
                        }
                    )

        return {
            "table": table,
            "column": column,
            "time_column": time_column,
            "method": "z_score_daily_aggregation",
            "z_threshold": z_threshold,
            "stats": {"mean": round(mean, 2), "stddev": round(stddev, 2), "days": len(rows)},
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        }
    else:
        # Row-level anomaly detection
        stats = registry.execute_raw(
            f"SELECT AVG({safe_col}), STDDEV({safe_col}), COUNT(*) "
            f"FROM {qualified} WHERE {safe_col} IS NOT NULL"
        ).fetchone()
        mean, stddev, count = float(stats[0] or 0), float(stats[1] or 0), stats[2]

        anomalies = []
        if stddev > 0:
            outliers = registry.query(
                f"SELECT *, ({safe_col} - {mean}) / {stddev} AS z_score "
                f"FROM {qualified} "
                f"WHERE {safe_col} IS NOT NULL "
                f"AND ABS(({safe_col} - {mean}) / {stddev}) >= {z_threshold} "
                f"ORDER BY ABS(({safe_col} - {mean}) / {stddev}) DESC",
                limit=100,
            )
            anomalies = outliers

        return {
            "table": table,
            "column": column,
            "method": "z_score_row_level",
            "z_threshold": z_threshold,
            "stats": {"mean": round(mean, 2), "stddev": round(stddev, 2), "count": count},
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        }


def compare_schemas(
    registry: SourceRegistry,
    table_a: str,
    table_b: str,
    source_a: str | None = None,
    source_b: str | None = None,
) -> dict[str, Any]:
    """Compare schemas of two tables and report differences.

    Useful for detecting schema drift between environments, versions, or duplicate tables.

    Args:
        table_a: First table name.
        table_b: Second table name.
        source_a: (Optional) Source alias for first table.
        source_b: (Optional) Source alias for second table.

    Returns:
        Schema diff with added, removed, and type-changed columns.
    """
    schema_a = {c["column"]: c["type"] for c in registry.get_schema(table_a, source_a)}
    schema_b = {c["column"]: c["type"] for c in registry.get_schema(table_b, source_b)}

    cols_a = set(schema_a.keys())
    cols_b = set(schema_b.keys())

    added = [{"column": c, "type": schema_b[c]} for c in sorted(cols_b - cols_a)]
    removed = [{"column": c, "type": schema_a[c]} for c in sorted(cols_a - cols_b)]
    type_changes = [
        {"column": c, "type_a": schema_a[c], "type_b": schema_b[c]}
        for c in sorted(cols_a & cols_b)
        if schema_a[c] != schema_b[c]
    ]

    is_identical = len(added) == 0 and len(removed) == 0 and len(type_changes) == 0

    return {
        "table_a": f"{source_a}.{table_a}" if source_a else table_a,
        "table_b": f"{source_b}.{table_b}" if source_b else table_b,
        "identical": is_identical,
        "columns_added_in_b": added,
        "columns_removed_in_b": removed,
        "type_changes": type_changes,
        "diff_count": len(added) + len(removed) + len(type_changes),
    }
