"""
Source registry — manages connections to SQLite, Parquet, and CSV data sources.

All querying is done through DuckDB, which can natively read SQLite databases,
Parquet files, and CSVs. This gives us a single SQL dialect across all source types.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb

from data_detective import DEFAULT_QUERY_LIMIT
from data_detective.validation import validate_identifier, validate_path


class SourceType(str, Enum):
    SQLITE = "sqlite"
    PARQUET = "parquet"
    CSV = "csv"


@dataclass
class DataSource:
    """A registered data source."""

    name: str
    source_type: SourceType
    path: str
    tables: list[str] = field(default_factory=list)


class SourceRegistry:
    """Manages data source connections through a shared DuckDB instance."""

    def __init__(self) -> None:
        self._conn = duckdb.connect(":memory:")
        self._sources: dict[str, DataSource] = {}
        # Install and load extensions we may need
        self._conn.execute("INSTALL sqlite; LOAD sqlite;")

    @property
    def sources(self) -> dict[str, DataSource]:
        return dict(self._sources)

    def connect(self, name: str, source_type: str, path: str) -> DataSource:
        """Register and connect a new data source.

        Args:
            name: A friendly alias for this source.
            source_type: One of 'sqlite', 'parquet', 'csv'.
            path: File path (supports globs for parquet/csv, e.g. './data/*.parquet').

        Returns:
            The registered DataSource with discovered tables.

        Raises:
            ValueError: If the name is already registered or the type is unknown.
            FileNotFoundError: If the path does not exist.
        """
        name = validate_identifier(name, "source name")

        if name in self._sources:
            raise ValueError(f"Source '{name}' is already registered. Disconnect it first.")

        stype = SourceType(source_type.lower())
        resolved = str(Path(path).resolve())
        validate_path(resolved, "source path")

        # Validate path exists (for globs, check the parent dir)
        if "*" not in resolved and not os.path.exists(resolved):
            raise FileNotFoundError(f"Path not found: {resolved}")

        tables: list[str] = []

        if stype == SourceType.SQLITE:
            self._conn.execute(f"ATTACH '{resolved}' AS \"{name}\" (TYPE sqlite, READ_ONLY)")
            rows = self._conn.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{name}'"
            ).fetchall()
            tables = [r[0] for r in rows]

        elif stype == SourceType.PARQUET:
            view_name = f"{name}"
            self._conn.execute(
                f'CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM read_parquet(\'{resolved}\')'
            )
            tables = [view_name]

        elif stype == SourceType.CSV:
            view_name = f"{name}"
            self._conn.execute(
                f'CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM read_csv_auto(\'{resolved}\')'
            )
            tables = [view_name]

        source = DataSource(name=name, source_type=stype, path=resolved, tables=tables)
        self._sources[name] = source
        return source

    def disconnect(self, name: str) -> None:
        """Remove a registered data source."""
        name = validate_identifier(name, "source name")
        if name not in self._sources:
            raise ValueError(f"Source '{name}' is not registered.")

        source = self._sources[name]
        if source.source_type == SourceType.SQLITE:
            self._conn.execute(f'DETACH "{name}"')
        else:
            for table in source.tables:
                self._conn.execute(f'DROP VIEW IF EXISTS "{table}"')

        del self._sources[name]

    def query(self, sql: str, limit: int = DEFAULT_QUERY_LIMIT) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as list of dicts.

        A LIMIT clause is appended if not already present to prevent runaway queries.
        """
        trimmed = sql.strip().rstrip(";")
        if "limit" not in trimmed.lower().split("--")[0].split("/*")[0]:
            trimmed = f"{trimmed} LIMIT {limit}"

        result = self._conn.execute(trimmed)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def execute_raw(self, sql: str) -> duckdb.DuckDBPyConnection:
        """Execute raw SQL and return the connection cursor for advanced use."""
        return self._conn.execute(sql)

    def list_all_tables(self) -> list[dict[str, str]]:
        """List all tables/views across all connected sources."""
        tables = []
        for source in self._sources.values():
            for table in source.tables:
                tables.append(
                    {
                        "source": source.name,
                        "table": table,
                        "type": source.source_type.value,
                        "path": source.path,
                    }
                )
        return tables

    def get_schema(self, table: str, source_name: str | None = None) -> list[dict[str, str]]:
        """Get the column schema for a table.

        For SQLite sources, use 'source_name.table_name' qualification.
        """
        table = validate_identifier(table, "table")
        if source_name:
            source_name = validate_identifier(source_name, "source name")
        if source_name and source_name in self._sources:
            src = self._sources[source_name]
            if src.source_type == SourceType.SQLITE:
                qualified = f'"{source_name}"."{table}"'
            else:
                qualified = f'"{table}"'
        else:
            qualified = f'"{table}"'

        rows = self._conn.execute(f"DESCRIBE {qualified}").fetchall()
        return [{"column": r[0], "type": r[1], "nullable": r[2]} for r in rows]
