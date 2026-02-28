"""Tests for identifier and path validation."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from data_detective.validation import validate_identifier, validate_path
from data_detective.sources.registry import SourceRegistry


# ---------------------------------------------------------------------------
# validate_identifier
# ---------------------------------------------------------------------------


class TestValidateIdentifier:
    def test_simple_name(self) -> None:
        assert validate_identifier("users") == "users"

    def test_name_with_underscore(self) -> None:
        assert validate_identifier("my_table") == "my_table"

    def test_name_with_hyphen(self) -> None:
        assert validate_identifier("my-source") == "my-source"

    def test_name_with_dot(self) -> None:
        assert validate_identifier("schema.table") == "schema.table"

    def test_name_with_spaces(self) -> None:
        assert validate_identifier("my table") == "my table"

    def test_strips_whitespace(self) -> None:
        assert validate_identifier("  users  ") == "users"

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            validate_identifier("")

    def test_rejects_whitespace_only(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            validate_identifier("   ")

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_identifier("users; DROP TABLE users")

    def test_rejects_single_quote(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_identifier("users' OR '1'='1")

    def test_rejects_double_quote(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_identifier('users" --')

    def test_rejects_double_dash_comment(self) -> None:
        with pytest.raises(ValueError, match="disallowed"):
            validate_identifier("users --comment")

    def test_rejects_c_style_comment(self) -> None:
        with pytest.raises(ValueError, match="disallowed"):
            validate_identifier("users /*comment*/")

    def test_rejects_parentheses(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_identifier("users()")

    def test_custom_label_in_error(self) -> None:
        with pytest.raises(ValueError, match="source name"):
            validate_identifier("bad;input", label="source name")


# ---------------------------------------------------------------------------
# validate_path
# ---------------------------------------------------------------------------


class TestValidatePath:
    def test_simple_path(self) -> None:
        assert validate_path("./data/file.parquet") == "./data/file.parquet"

    def test_absolute_unix_path(self) -> None:
        assert validate_path("/home/user/data.csv") == "/home/user/data.csv"

    def test_windows_path(self) -> None:
        assert validate_path("C:\\Users\\data\\file.db") == "C:\\Users\\data\\file.db"

    def test_glob_path(self) -> None:
        assert validate_path("./data/*.parquet") == "./data/*.parquet"

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            validate_path("")

    def test_rejects_single_quote(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_path("data'; DROP TABLE users--")

    def test_rejects_double_quote(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_path('data" OR 1=1')

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            validate_path("data; malicious")

    def test_custom_label_in_error(self) -> None:
        with pytest.raises(ValueError, match="output path"):
            validate_path("bad'path", label="output path")


# ---------------------------------------------------------------------------
# Integration: validation blocks injection at the registry level
# ---------------------------------------------------------------------------


class TestValidationIntegration:
    @pytest.fixture
    def registry(self) -> SourceRegistry:
        return SourceRegistry()

    @pytest.fixture
    def sample_sqlite(self, tmp_path: Path) -> Path:
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO users VALUES (1, 'Alice')")
        conn.commit()
        conn.close()
        return db_path

    def test_connect_rejects_malicious_name(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            registry.connect("db'; DROP TABLE users--", "sqlite", str(sample_sqlite))

    def test_connect_rejects_semicolon_name(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            registry.connect("db; evil", "sqlite", str(sample_sqlite))

    def test_get_schema_rejects_malicious_table(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            registry.get_schema("users'; DROP TABLE users--", "db")

    def test_disconnect_rejects_malicious_name(self, registry: SourceRegistry) -> None:
        with pytest.raises(ValueError, match="disallowed characters"):
            registry.disconnect("db'; DROP TABLE users--")

    def test_quality_rejects_malicious_table(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.quality import detect_quality_issues

        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            detect_quality_issues(registry, "users'; DROP TABLE users--", source="db")

    def test_quality_rejects_malicious_source(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.quality import detect_quality_issues

        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            detect_quality_issues(registry, "users", source="db'; evil")

    def test_anomaly_rejects_malicious_column(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.quality import detect_anomalies

        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            detect_anomalies(registry, "users", "id'; DROP TABLE users--", source="db")

    def test_profile_rejects_malicious_table(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.profile import profile_table

        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            profile_table(registry, "users'; DROP TABLE users--", source="db")

    def test_sample_rejects_malicious_table(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.query import get_sample

        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            get_sample(registry, "users'; DROP TABLE users--", source="db")

    def test_export_rejects_malicious_path(self, registry: SourceRegistry, sample_sqlite: Path) -> None:
        from data_detective.tools.export import export_data

        registry.connect("db", "sqlite", str(sample_sqlite))
        with pytest.raises(ValueError, match="disallowed characters"):
            export_data(registry, "SELECT 1", "out'; DROP TABLE users--.parquet")
