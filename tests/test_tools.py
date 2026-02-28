"""Tests for the source registry and core query functionality."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_detective.sources.registry import SourceRegistry


@pytest.fixture
def registry() -> SourceRegistry:
    return SourceRegistry()


@pytest.fixture
def sample_sqlite(tmp_path: Path) -> Path:
    """Create a small SQLite DB for testing."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")
    conn.executemany(
        "INSERT INTO users VALUES (?, ?, ?)",
        [
            (1, "Alice", "alice@test.com"),
            (2, "Bob", None),
            (3, "Charlie", "charlie@test.com"),
            (4, "Alice", "alice@test.com"),  # duplicate of row 1
        ],
    )
    conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)")
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [
            (1, 1, 99.99),
            (2, 2, -10.50),  # negative amount
            (3, 1, 250.00),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """Create a small Parquet file for testing."""
    table = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10.0, 20.0, 30.0, 1000.0, 25.0],  # 1000 is an outlier
            "category": ["A", "B", "A", "B", "A"],
        }
    )
    path = tmp_path / "test.parquet"
    pq.write_table(table, str(path))
    return path


class TestSourceRegistry:
    def test_connect_sqlite(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        source = registry.connect("testdb", "sqlite", str(sample_sqlite))
        assert source.name == "testdb"
        assert "users" in source.tables
        assert "orders" in source.tables

    def test_connect_parquet(self, registry: SourceRegistry, sample_parquet: Path) -> None:
        source = registry.connect("testpq", "parquet", str(sample_parquet))
        assert source.name == "testpq"
        assert len(source.tables) == 1

    def test_query_sqlite(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        registry.connect("db", "sqlite", str(sample_sqlite))
        rows = registry.query('SELECT * FROM "db"."users"')
        assert len(rows) == 4
        assert rows[0]["name"] == "Alice"

    def test_query_parquet(self, registry: SourceRegistry, sample_parquet: Path) -> None:
        registry.connect("pq", "parquet", str(sample_parquet))
        rows = registry.query('SELECT * FROM "pq"')
        assert len(rows) == 5

    def test_disconnect(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        registry.connect("db", "sqlite", str(sample_sqlite))
        registry.disconnect("db")
        assert "db" not in registry.sources

    def test_duplicate_connect_raises(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="already registered"):
            registry.connect("db", "sqlite", str(sample_sqlite))

    def test_list_all_tables(self, registry: SourceRegistry, sample_sqlite: Path, sample_parquet: Path) -> None:
        registry.connect("db", "sqlite", str(sample_sqlite))
        registry.connect("pq", "parquet", str(sample_parquet))
        tables = registry.list_all_tables()
        assert len(tables) == 3  # users, orders from sqlite + pq view

    def test_get_schema(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        registry.connect("db", "sqlite", str(sample_sqlite))
        schema = registry.get_schema("users", "db")
        col_names = [c["column"] for c in schema]
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names


class TestQualityTools:
    def test_detect_quality_issues(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.quality import detect_quality_issues

        registry.connect("db", "sqlite", str(sample_sqlite))
        result = detect_quality_issues(registry, "users", source="db")
        assert result["issue_count"] > 0
        issue_types = [i["type"] for i in result["issues"]]
        # Rows 1 & 4 share name+email but differ in id → semantic duplicate
        assert "semantic_duplicates" in issue_types or "duplicates" in issue_types
        assert "high_null_rate" in issue_types  # 25% null emails

    def test_detect_anomalies(self, registry: SourceRegistry, sample_parquet: Path) -> None:
        from data_detective.tools.quality import detect_anomalies

        registry.connect("pq", "parquet", str(sample_parquet))
        # Use a lower z_threshold for this small dataset (5 rows)
        result = detect_anomalies(registry, "pq", "value", z_threshold=1.5)
        # 1000.0 should be detected as an outlier
        assert result["anomaly_count"] >= 1

    def test_compare_schemas(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.quality import compare_schemas

        registry.connect("db", "sqlite", str(sample_sqlite))
        result = compare_schemas(registry, "users", "orders", source_a="db", source_b="db")
        assert not result["identical"]
        assert result["diff_count"] > 0


class TestProfileTools:
    def test_profile_table(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.profile import profile_table

        registry.connect("db", "sqlite", str(sample_sqlite))
        result = profile_table(registry, "users", source="db")
        assert result["row_count"] == 4
        assert result["column_count"] == 3
        email_col = next(c for c in result["columns"] if c["column"] == "email")
        assert email_col["null_count"] == 1

    def test_summarize(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.profile import summarize

        registry.connect("db", "sqlite", str(sample_sqlite))
        result = summarize(registry)
        assert result["total_sources"] == 1
        assert result["total_rows"] == 7  # 4 users + 3 orders


class TestExportTools:
    def test_export_parquet(self, registry: SourceRegistry, sample_sqlite: Path, tmp_path: Path) -> None:
        from data_detective.tools.export import export_data

        registry.connect("db", "sqlite", str(sample_sqlite))
        out = tmp_path / "export.parquet"
        result = export_data(registry, 'SELECT * FROM "db"."users"', str(out), "parquet")
        assert result["status"] == "exported"
        assert result["row_count"] == 4
        assert Path(result["path"]).exists()

    def test_export_csv(self, registry: SourceRegistry, sample_sqlite: Path, tmp_path: Path) -> None:
        from data_detective.tools.export import export_data

        registry.connect("db", "sqlite", str(sample_sqlite))
        out = tmp_path / "export.csv"
        result = export_data(registry, 'SELECT * FROM "db"."orders"', str(out), "csv")
        assert result["status"] == "exported"
        assert result["row_count"] == 3
