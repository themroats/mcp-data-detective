# mcp-data-server

An MCP server that gives AI assistants the ability to connect to, query, profile, and monitor data sources — turning any LLM into an interactive data engineering copilot.

Point it at any SQLite database, Parquet file, or CSV and let your AI assistant explore schemas, run queries, profile columns, detect data quality issues, and export clean datasets — all through natural conversation.

## Features

| Tool | Description |
|---|---|
| `connect_source` | Register a SQLite, Parquet, or CSV data source |
| `disconnect_source` | Remove a connected data source |
| `list_sources` | List all connected sources and their tables |
| `list_tables` | List all tables across all sources |
| `get_table_schema` | Describe columns, types, and nullability |
| `run_query` | Execute SQL against any connected source (DuckDB) |
| `get_sample` | Random sample of N rows from a table |
| `profile_table` | Column-level stats: null rates, distributions, min/max/mean/median |
| `detect_quality_issues` | Find duplicates, high null rates, constant columns, unexpected negatives |
| `detect_anomalies` | Z-score based anomaly detection on numeric columns |
| `compare_schemas` | Diff schemas between two tables |
| `summarize` | High-level summary across all connected data |
| `export_data` | Export query results to Parquet or CSV |

## Architecture

```
┌──────────────────────────────────────────────┐
│              AI Assistant (LLM)              │
│         Claude, GPT, Copilot, etc.           │
└──────────────────┬───────────────────────────┘
                   │  MCP Protocol
┌──────────────────▼───────────────────────────┐
│            mcp-data-server                   │
│                                              │
│  ┌─────────────────────────────────────────┐ │
│  │             MCP Tools Layer             │ │
│  │  connect · query · profile · quality ·  │ │
│  │  export · anomalies · schemas           │ │
│  └──────────────────┬──────────────────────┘ │
│                     │                        │
│  ┌──────────────────▼──────────────────────┐ │
│  │          Source Registry                │ │
│  │     Manages connections & routing       │ │
│  └──────────────────┬──────────────────────┘ │
│                     │                        │
│  ┌──────────────────▼──────────────────────┐ │
│  │          DuckDB Query Engine            │ │
│  │   Single SQL dialect across all sources │ │
│  └──────┬───────────┬──────────────┬───────┘ │
│         │           │              │         │
│    ┌────▼───┐ ┌─────▼────┐ ┌──────▼───┐     │
│    │ SQLite │ │ Parquet  │ │   CSV    │     │
│    └────────┘ └──────────┘ └──────────┘     │
└──────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+

### Install

```bash
# Clone the repository
git clone https://github.com/themroats/mcp-data-server.git
cd mcp-data-server

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows

# Install the package
pip install -e ".[dev]"
```

### Generate Demo Data

The project includes a synthetic e-commerce data generator with intentional quality issues for demo purposes:

```bash
mcp-data-seed
```

This creates a `data/` directory with:
- **4 Parquet files**: `customers.parquet`, `products.parquet`, `orders.parquet`, `events.parquet`
- **1 SQLite database**: `ecommerce.db` (same data)
- **Intentional quality issues**: null emails, negative prices, duplicate orders, anomalous revenue dips, future timestamps

Options:
```bash
mcp-data-seed --rows 50000          # More data (default: 10,000 orders)
mcp-data-seed --output ./my-data    # Custom output directory
mcp-data-seed --format parquet      # Parquet only (or sqlite, both)
```

### Configure with Your AI Client

Add to your MCP client configuration (e.g. Claude Desktop `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "data-server": {
      "command": "mcp-data-server"
    }
  }
}
```

Or with VS Code / GitHub Copilot, add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "data-server": {
      "command": "mcp-data-server"
    }
  }
}
```

## Example Conversation

Once connected, you can have conversations like:

> **You:** Connect to the demo SQLite database at `./data/ecommerce.db`
>
> **Assistant:** Connected source "ecommerce" — found 4 tables: customers, orders, products, events (60,150+ total rows).
>
> **You:** Profile all the tables — what's the health of this data?
>
> **Assistant:** Here's what I found across the 4 tables...
> - `customers`: 8.2% of emails are NULL
> - `products`: 6 products have negative prices
> - `orders`: 150 duplicate order groups detected
> - `events`: 248 events have timestamps in the future
>
> **You:** Show me the monthly revenue trend. Are there any anomalies?
>
> **Assistant:** Revenue is steady except for **March 2024**, which shows a 40% drop in total revenue compared to the average — flagged as an anomaly (z-score: -3.2).
>
> **You:** Export a cleaned version of orders without duplicates as Parquet
>
> **Assistant:** Exported 9,850 rows to `./clean_orders.parquet` (duplicate rows removed).

## Using Your Own Data

You can connect any SQLite database, Parquet file, or CSV:

```
Connect to my sales database at ./sales.db as "sales" (sqlite)
Connect to ./logs/*.parquet as "logs" (parquet)
Connect to ./report.csv as "report" (csv)
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest -v
```

## Tech Stack

- **[MCP (Model Context Protocol)](https://modelcontextprotocol.io/)** — Standard protocol for AI↔tool communication
- **[DuckDB](https://duckdb.org/)** — In-process analytical SQL engine (reads SQLite, Parquet, CSV natively)
- **[FastMCP](https://github.com/jlowin/fastmcp)** — Python framework for building MCP servers
- **[PyArrow](https://arrow.apache.org/docs/python/)** — Parquet read/write
- **[Faker](https://faker.readthedocs.io/)** — Synthetic data generation

## Project Structure

```
mcp-data-server/
├── src/mcp_data_server/
│   ├── server.py              # MCP server entry point, tool registration
│   ├── sources/
│   │   └── registry.py        # DuckDB-backed source management
│   ├── tools/
│   │   ├── connect.py         # Connect/disconnect/list sources
│   │   ├── query.py           # SQL query, list tables, get sample
│   │   ├── profile.py         # Table profiling, summarize
│   │   ├── quality.py         # Quality issues, anomalies, schema diff
│   │   └── export.py          # Export to Parquet/CSV
│   └── seed/
│       └── generator.py       # Synthetic e-commerce data generator
├── tests/
│   └── test_tools.py          # Registry, quality, profile, export tests
├── data/                      # Generated demo data (gitignored)
├── pyproject.toml
└── README.md
```

## License

MIT
