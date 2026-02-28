"""MCP Data Server — an MCP server for data exploration, profiling, and quality monitoring."""

__version__ = "0.1.0"

# Substrings used to identify numeric column types from DuckDB's type system.
# Shared across profiling and quality tools to keep detection consistent.
NUMERIC_TYPE_FRAGMENTS = (
    "int", "float", "double", "decimal", "numeric",
    "bigint", "smallint", "tinyint", "hugeint", "ubigint", "real",
)
