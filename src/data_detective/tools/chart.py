"""Chart tools — create_chart, save_recipe, replay_chart, export_insight."""

from __future__ import annotations

import json
import textwrap
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from data_detective.sources.registry import SourceRegistry
from data_detective.validation import validate_path

try:
    import matplotlib

    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError:
    plt = None  # type: ignore[assignment]
    mdates = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Chart configuration
# ---------------------------------------------------------------------------

SUPPORTED_CHART_TYPES = ("bar", "line", "scatter", "histogram")
DEFAULT_CHART_DIR = "./charts"
DEFAULT_FIGSIZE = (12, 6)


@dataclass
class ChartRecipe:
    """Everything needed to reproduce a chart."""

    name: str
    created: str
    sql: str
    chart_type: str
    x: str
    y: str
    title: str | None = None
    color: str | None = None
    sources: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChartRecipe:
        return cls(**data)


class RecipeStore:
    """In-memory store for recent chart recipes."""

    def __init__(self, max_history: int = 20) -> None:
        self._history: list[ChartRecipe] = []
        self._max_history = max_history

    def push(self, recipe: ChartRecipe) -> None:
        self._history.append(recipe)
        if len(self._history) > self._max_history:
            self._history.pop(0)

    @property
    def last(self) -> ChartRecipe | None:
        return self._history[-1] if self._history else None

    def get(self, index: int = -1) -> ChartRecipe | None:
        try:
            return self._history[index]
        except IndexError:
            return None

    @property
    def count(self) -> int:
        return len(self._history)


# Module-level store shared across tool calls
recipe_store = RecipeStore()

# ---------------------------------------------------------------------------
# Core chart logic
# ---------------------------------------------------------------------------


def _check_matplotlib() -> None:
    if plt is None:
        raise ImportError(
            "matplotlib is required for chart tools. Install with: pip install matplotlib"
        )


def _capture_sources(registry: SourceRegistry) -> list[dict[str, str]]:
    """Snapshot the currently connected sources for the recipe."""
    return [
        {"name": src.name, "type": src.source_type.value, "path": src.path}
        for src in registry.sources.values()
    ]


def _render_chart(
    rows: list[dict[str, Any]],
    chart_type: str,
    x: str,
    y: str,
    title: str | None,
    color: str | None,
    output_path: Path,
) -> Path:
    """Render a chart to disk and return the file path."""
    _check_matplotlib()

    x_vals = [r[x] for r in rows]
    y_vals = [r[y] for r in rows]

    fig, ax = plt.subplots(figsize=DEFAULT_FIGSIZE)

    if chart_type == "bar":
        if color and color in rows[0]:
            groups: dict[str, tuple[list, list]] = {}
            for r in rows:
                g = str(r[color])
                if g not in groups:
                    groups[g] = ([], [])
                groups[g][0].append(r[x])
                groups[g][1].append(r[y])
            for label, (gx, gy) in groups.items():
                ax.bar(gx, gy, label=label, alpha=0.8)
            ax.legend()
        else:
            ax.bar(range(len(x_vals)), y_vals, tick_label=[str(v) for v in x_vals], alpha=0.8)
    elif chart_type == "line":
        if color and color in rows[0]:
            groups = {}
            for r in rows:
                g = str(r[color])
                if g not in groups:
                    groups[g] = ([], [])
                groups[g][0].append(r[x])
                groups[g][1].append(r[y])
            for label, (gx, gy) in groups.items():
                ax.plot(gx, gy, label=label, marker="o", markersize=3)
            ax.legend()
        else:
            ax.plot(x_vals, y_vals, marker="o", markersize=3)
    elif chart_type == "scatter":
        ax.scatter(x_vals, y_vals, alpha=0.6)
    elif chart_type == "histogram":
        ax.hist(y_vals, bins="auto", alpha=0.8, edgecolor="black")
        ax.set_xlabel(y)
        ax.set_ylabel("Frequency")

    if chart_type != "histogram":
        ax.set_xlabel(x)
        ax.set_ylabel(y)

    # Rotate x labels if there are many values
    if len(x_vals) > 10:
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    ax.set_title(title or f"{y} by {x}")
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    return output_path


def _auto_chart_path(title: str | None, chart_type: str) -> Path:
    """Generate an auto-named path in the default chart directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = (title or chart_type).lower().replace(" ", "_")[:40]
    # Remove non-alphanumeric characters except underscores
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    return Path(DEFAULT_CHART_DIR) / f"{slug}_{timestamp}.png"


# ---------------------------------------------------------------------------
# Tool functions
# ---------------------------------------------------------------------------


def create_chart(
    registry: SourceRegistry,
    sql: str,
    chart_type: str,
    x: str,
    y: str,
    title: str | None = None,
    color: str | None = None,
    output_path: str | None = None,
) -> dict[str, Any]:
    """Run a SQL query and render the results as a chart.

    The chart is saved to disk and a recipe is held in memory for later saving.

    Args:
        registry: The source registry to query against.
        sql: SQL query that produces the data to chart.
        chart_type: One of 'bar', 'line', 'scatter', 'histogram'.
        x: Column name for the x-axis.
        y: Column name for the y-axis.
        title: Optional chart title.
        color: Optional column to group/color by.
        output_path: Optional file path for the chart image.

    Returns:
        Chart path and recipe info.
    """
    _check_matplotlib()

    ct = chart_type.lower()
    if ct not in SUPPORTED_CHART_TYPES:
        raise ValueError(
            f"Unsupported chart type '{ct}'. Use one of: {', '.join(SUPPORTED_CHART_TYPES)}"
        )

    rows = registry.query(sql, limit=10000)
    if not rows:
        raise ValueError("Query returned no rows — nothing to chart.")

    if x not in rows[0]:
        raise ValueError(f"Column '{x}' not found in query results. Available: {list(rows[0].keys())}")
    if y not in rows[0] and ct != "histogram":
        raise ValueError(f"Column '{y}' not found in query results. Available: {list(rows[0].keys())}")

    dest = Path(output_path) if output_path else _auto_chart_path(title, ct)
    if output_path:
        validate_path(output_path, "output_path")

    chart_path = _render_chart(rows, ct, x, y, title, color, dest)

    # Build recipe and hold in memory
    recipe = ChartRecipe(
        name=title or f"{y}_by_{x}",
        created=datetime.now().isoformat(),
        sql=sql,
        chart_type=ct,
        x=x,
        y=y,
        title=title,
        color=color,
        sources=_capture_sources(registry),
    )
    recipe_store.push(recipe)

    return {
        "chart_path": str(chart_path),
        "chart_type": ct,
        "rows_plotted": len(rows),
        "recipe_held": True,
        "recipe_index": recipe_store.count - 1,
    }


def save_recipe(
    name: str | None = None,
    output_dir: str | None = None,
    index: int = -1,
) -> dict[str, Any]:
    """Persist a held recipe to disk as a JSON file.

    Args:
        name: Optional name for the recipe file. Defaults to the recipe's name.
        output_dir: Directory to save to. Defaults to ./charts/.
        index: Recipe index in history (-1 = most recent).

    Returns:
        Path to the saved recipe file.
    """
    recipe = recipe_store.get(index)
    if recipe is None:
        raise ValueError("No recipe found. Run create_chart first.")

    slug = (name or recipe.name).lower().replace(" ", "_")[:40]
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    out_dir = Path(output_dir or DEFAULT_CHART_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    recipe_path = out_dir / f"{slug}.recipe.json"

    recipe_path.write_text(json.dumps(recipe.to_dict(), indent=2, default=str))

    return {
        "recipe_path": str(recipe_path),
        "name": recipe.name,
        "status": "saved",
    }


def replay_chart(
    registry: SourceRegistry,
    recipe_path: str,
    output_path: str | None = None,
) -> dict[str, Any]:
    """Regenerate a chart from a saved recipe file.

    Args:
        registry: The source registry to query against.
        recipe_path: Path to a .recipe.json file.
        output_path: Optional override path for the re-rendered chart.

    Returns:
        New chart path and info.
    """
    validate_path(recipe_path, "recipe_path")
    rp = Path(recipe_path)
    if not rp.exists():
        raise FileNotFoundError(f"Recipe file not found: {recipe_path}")

    data = json.loads(rp.read_text())
    recipe = ChartRecipe.from_dict(data)

    return create_chart(
        registry,
        sql=recipe.sql,
        chart_type=recipe.chart_type,
        x=recipe.x,
        y=recipe.y,
        title=recipe.title,
        color=recipe.color,
        output_path=output_path,
    )


def export_insight(
    recipe_path: str | None = None,
    export_format: str = "script",
    output_path: str | None = None,
    index: int = -1,
) -> dict[str, Any]:
    """Export a recipe as a standalone script, SQL file, or notebook.

    Can use either a saved recipe file or a held in-memory recipe.

    Args:
        recipe_path: Path to a .recipe.json file. If None, uses in-memory recipe.
        export_format: One of 'script', 'sql', 'notebook'.
        output_path: Optional output file path.
        index: Recipe index in history if using in-memory recipe (-1 = most recent).

    Returns:
        Path to the exported file.
    """
    # Load recipe from file or memory
    if recipe_path:
        validate_path(recipe_path, "recipe_path")
        rp = Path(recipe_path)
        if not rp.exists():
            raise FileNotFoundError(f"Recipe file not found: {recipe_path}")
        recipe = ChartRecipe.from_dict(json.loads(rp.read_text()))
    else:
        recipe = recipe_store.get(index)
        if recipe is None:
            raise ValueError("No recipe found. Run create_chart or provide a recipe_path.")

    fmt = export_format.lower()
    if fmt not in ("script", "sql", "notebook"):
        raise ValueError(f"Unsupported format '{fmt}'. Use 'script', 'sql', or 'notebook'.")

    slug = recipe.name.lower().replace(" ", "_")[:40]
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    out_dir = Path(DEFAULT_CHART_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    if fmt == "sql":
        dest = Path(output_path) if output_path else out_dir / f"{slug}.sql"
        content = f"-- {recipe.title or recipe.name}\n-- Generated by Data Detective\n\n{recipe.sql}\n"
        dest.write_text(content)
        return {"export_path": str(dest), "format": "sql"}

    if fmt == "script":
        dest = Path(output_path) if output_path else out_dir / f"{slug}.py"
        content = _generate_script(recipe)
        dest.write_text(content)
        return {"export_path": str(dest), "format": "script"}

    if fmt == "notebook":
        dest = Path(output_path) if output_path else out_dir / f"{slug}.ipynb"
        content = _generate_notebook(recipe)
        dest.write_text(content)
        return {"export_path": str(dest), "format": "notebook"}

    # unreachable due to validation above, but satisfies type checker
    raise ValueError(f"Unsupported format: {fmt}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Export templates
# ---------------------------------------------------------------------------


def _generate_script(recipe: ChartRecipe) -> str:
    """Generate a standalone Python script from a recipe."""
    source_setup = ""
    for src in recipe.sources:
        if src["type"] == "sqlite":
            source_setup += f'conn.execute("ATTACH \'{src["path"]}\' AS \\"{src["name"]}\\" (TYPE sqlite)")\n'
        elif src["type"] == "parquet":
            source_setup += (
                f'conn.execute("CREATE OR REPLACE VIEW \\"{src["name"]}\\" '
                f"AS SELECT * FROM read_parquet('{src['path']}')\")\n"
            )
        elif src["type"] == "csv":
            source_setup += (
                f'conn.execute("CREATE OR REPLACE VIEW \\"{src["name"]}\\" '
                f"AS SELECT * FROM read_csv_auto('{src['path']}')\")\n"
            )

    color_code = ""
    if recipe.color:
        color_code = f"""
    # Group by color column
    groups = {{}}
    for _, row in df.iterrows():
        g = str(row["{recipe.color}"])
        if g not in groups:
            groups[g] = ([], [])
        groups[g][0].append(row["{recipe.x}"])
        groups[g][1].append(row["{recipe.y}"])
    for label, (gx, gy) in groups.items():
        ax.{recipe.chart_type}(gx, gy, label=label, alpha=0.8)
    ax.legend()"""
    else:
        if recipe.chart_type == "bar":
            color_code = f'    ax.bar(df["{recipe.x}"], df["{recipe.y}"], alpha=0.8)'
        elif recipe.chart_type == "line":
            color_code = f'    ax.plot(df["{recipe.x}"], df["{recipe.y}"], marker="o", markersize=3)'
        elif recipe.chart_type == "scatter":
            color_code = f'    ax.scatter(df["{recipe.x}"], df["{recipe.y}"], alpha=0.6)'
        elif recipe.chart_type == "histogram":
            color_code = f'    ax.hist(df["{recipe.y}"], bins="auto", alpha=0.8, edgecolor="black")'

    return textwrap.dedent(f"""\
        #!/usr/bin/env python3
        \"""
        {recipe.title or recipe.name}

        Auto-generated by Data Detective on {recipe.created}.
        Re-run this script to regenerate the chart from live data.
        \"""

        import duckdb
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        # Connect to data sources
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL sqlite; LOAD sqlite;")
        {source_setup}
        # Run query
        sql = \"""{recipe.sql}\"""
        df = conn.execute(sql).fetchdf()
        conn.close()

        # Render chart
        fig, ax = plt.subplots(figsize=(12, 6))
        {color_code}
        ax.set_xlabel("{recipe.x}")
        ax.set_ylabel("{recipe.y}")
        ax.set_title("{recipe.title or recipe.name}")
        fig.tight_layout()
        fig.savefig("{recipe.name.lower().replace(' ', '_')}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        print("Chart saved!")
    """)


def _generate_notebook(recipe: ChartRecipe) -> str:
    """Generate a Jupyter notebook JSON string from a recipe."""
    source_setup = ""
    for src in recipe.sources:
        if src["type"] == "sqlite":
            source_setup += f'conn.execute("ATTACH \\\'{src["path"]}\\\' AS \\\\\\"{src["name"]}\\\\\\" (TYPE sqlite)")\\n'
        elif src["type"] == "parquet":
            source_setup += (
                f'conn.execute("CREATE OR REPLACE VIEW \\\\\\"{src["name"]}\\\\\\" '
                f"AS SELECT * FROM read_parquet(\\\'{src['path']}\\\')\\\")\n"
            )
        elif src["type"] == "csv":
            source_setup += (
                f'conn.execute("CREATE OR REPLACE VIEW \\\\\\"{src["name"]}\\\\\\" '
                f"AS SELECT * FROM read_csv_auto(\\\'{src['path']}\\\')\\\")\n"
            )

    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# {recipe.title or recipe.name}\n",
                f"\n",
                f"Auto-generated by Data Detective on {recipe.created}.\n",
            ],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "source": [
                "import duckdb\n",
                "import matplotlib.pyplot as plt\n",
                "\n",
                "conn = duckdb.connect(':memory:')\n",
                'conn.execute("INSTALL sqlite; LOAD sqlite;")\n',
            ],
            "outputs": [],
            "execution_count": None,
        },
        {
            "cell_type": "code",
            "metadata": {},
            "source": [
                f'sql = """{recipe.sql}"""\n',
                "df = conn.execute(sql).fetchdf()\n",
                "df.head()\n",
            ],
            "outputs": [],
            "execution_count": None,
        },
        {
            "cell_type": "code",
            "metadata": {},
            "source": [
                "fig, ax = plt.subplots(figsize=(12, 6))\n",
                f'ax.{recipe.chart_type}(df["{recipe.x}"], df["{recipe.y}"], alpha=0.8)\n'
                if recipe.chart_type in ("bar", "scatter")
                else f'ax.plot(df["{recipe.x}"], df["{recipe.y}"], marker="o")\n',
                f'ax.set_xlabel("{recipe.x}")\n',
                f'ax.set_ylabel("{recipe.y}")\n',
                f'ax.set_title("{recipe.title or recipe.name}")\n',
                "fig.tight_layout()\n",
                "plt.show()\n",
            ],
            "outputs": [],
            "execution_count": None,
        },
    ]

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.10.0"},
        },
        "cells": cells,
    }
    return json.dumps(notebook, indent=2)
