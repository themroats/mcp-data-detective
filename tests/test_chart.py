"""Tests for chart tools — create_chart, save_recipe, replay_chart, export_insight."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from data_detective.sources.registry import SourceRegistry
from data_detective.tools.chart import (
    ChartRecipe,
    RecipeStore,
    SUPPORTED_CHART_TYPES,
    create_chart,
    export_insight,
    recipe_store,
    replay_chart,
    save_recipe,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry() -> SourceRegistry:
    return SourceRegistry()


@pytest.fixture
def sample_sqlite(tmp_path: Path) -> Path:
    db_path = tmp_path / "chart_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE sales (id INTEGER, month TEXT, revenue REAL, region TEXT)"
    )
    conn.executemany(
        "INSERT INTO sales VALUES (?, ?, ?, ?)",
        [
            (1, "Jan", 100.0, "North"),
            (2, "Feb", 200.0, "North"),
            (3, "Mar", 150.0, "South"),
            (4, "Apr", 300.0, "South"),
            (5, "May", 250.0, "North"),
            (6, "Jun", 400.0, "South"),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def connected(registry: SourceRegistry, sample_sqlite: Path) -> SourceRegistry:
    registry.connect("shop", "sqlite", str(sample_sqlite))
    return registry


@pytest.fixture(autouse=True)
def _clear_store():
    """Reset the global recipe store between tests."""
    recipe_store._history.clear()
    yield
    recipe_store._history.clear()


# ---------------------------------------------------------------------------
# ChartRecipe dataclass
# ---------------------------------------------------------------------------


class TestChartRecipe:
    def test_to_dict_round_trip(self):
        recipe = ChartRecipe(
            name="test",
            created="2024-01-01",
            sql="SELECT 1",
            chart_type="bar",
            x="a",
            y="b",
        )
        d = recipe.to_dict()
        restored = ChartRecipe.from_dict(d)
        assert restored.name == "test"
        assert restored.chart_type == "bar"
        assert restored.sources == []

    def test_optional_fields_default(self):
        recipe = ChartRecipe(
            name="t", created="now", sql="SELECT 1", chart_type="line", x="x", y="y"
        )
        assert recipe.title is None
        assert recipe.color is None
        assert recipe.sources == []


# ---------------------------------------------------------------------------
# RecipeStore
# ---------------------------------------------------------------------------


class TestRecipeStore:
    def test_push_and_last(self):
        store = RecipeStore()
        r = ChartRecipe("a", "now", "SELECT 1", "bar", "x", "y")
        store.push(r)
        assert store.last is r
        assert store.count == 1

    def test_max_history(self):
        store = RecipeStore(max_history=3)
        for i in range(5):
            store.push(ChartRecipe(f"r{i}", "now", "SELECT 1", "bar", "x", "y"))
        assert store.count == 3
        assert store.last.name == "r4"
        assert store.get(0).name == "r2"  # oldest surviving

    def test_get_empty(self):
        store = RecipeStore()
        assert store.get() is None
        assert store.last is None


# ---------------------------------------------------------------------------
# create_chart
# ---------------------------------------------------------------------------


class TestCreateChart:
    def test_bar_chart(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "bar.png"
        result = create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Monthly Revenue",
            output_path=str(out),
        )
        assert out.exists()
        assert result["chart_type"] == "bar"
        assert result["rows_plotted"] == 6
        assert result["recipe_held"] is True

    def test_line_chart(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "line.png"
        result = create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="line",
            x="month",
            y="revenue",
            output_path=str(out),
        )
        assert out.exists()
        assert result["chart_type"] == "line"

    def test_scatter_chart(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "scatter.png"
        result = create_chart(
            connected,
            sql='SELECT id, revenue FROM "shop"."sales"',
            chart_type="scatter",
            x="id",
            y="revenue",
            output_path=str(out),
        )
        assert out.exists()
        assert result["chart_type"] == "scatter"

    def test_histogram_chart(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "hist.png"
        result = create_chart(
            connected,
            sql='SELECT revenue FROM "shop"."sales"',
            chart_type="histogram",
            x="revenue",
            y="revenue",
            output_path=str(out),
        )
        assert out.exists()
        assert result["chart_type"] == "histogram"

    def test_color_grouping(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "grouped.png"
        result = create_chart(
            connected,
            sql='SELECT month, revenue, region FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            color="region",
            output_path=str(out),
        )
        assert out.exists()
        assert result["rows_plotted"] == 6

    def test_line_color_grouping(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "line_grouped.png"
        result = create_chart(
            connected,
            sql='SELECT month, revenue, region FROM "shop"."sales"',
            chart_type="line",
            x="month",
            y="revenue",
            color="region",
            output_path=str(out),
        )
        assert out.exists()

    def test_unsupported_chart_type(self, connected: SourceRegistry):
        with pytest.raises(ValueError, match="Unsupported chart type"):
            create_chart(
                connected,
                sql='SELECT month, revenue FROM "shop"."sales"',
                chart_type="pie",
                x="month",
                y="revenue",
            )

    def test_empty_query(self, connected: SourceRegistry):
        with pytest.raises(ValueError, match="no rows"):
            create_chart(
                connected,
                sql='SELECT month, revenue FROM "shop"."sales" WHERE 1=0',
                chart_type="bar",
                x="month",
                y="revenue",
            )

    def test_missing_x_column(self, connected: SourceRegistry):
        with pytest.raises(ValueError, match="Column 'bad_col'"):
            create_chart(
                connected,
                sql='SELECT month, revenue FROM "shop"."sales"',
                chart_type="bar",
                x="bad_col",
                y="revenue",
            )

    def test_missing_y_column(self, connected: SourceRegistry):
        with pytest.raises(ValueError, match="Column 'bad_y'"):
            create_chart(
                connected,
                sql='SELECT month, revenue FROM "shop"."sales"',
                chart_type="bar",
                x="month",
                y="bad_y",
            )

    def test_recipe_held_in_memory(self, connected: SourceRegistry, tmp_path: Path):
        out = tmp_path / "mem.png"
        create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Test Recipe",
            output_path=str(out),
        )
        assert recipe_store.last is not None
        assert recipe_store.last.name == "Test Recipe"
        assert recipe_store.last.chart_type == "bar"

    def test_auto_path(self, connected: SourceRegistry):
        """create_chart with no output_path auto-generates a file under ./charts/."""
        result = create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Auto Path Test",
        )
        assert "charts" in result["chart_path"]
        # Cleanup the auto-generated file
        Path(result["chart_path"]).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# save_recipe
# ---------------------------------------------------------------------------


class TestSaveRecipe:
    def test_save(self, connected: SourceRegistry, tmp_path: Path):
        create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Save Test",
            output_path=str(tmp_path / "chart.png"),
        )
        result = save_recipe(name="save_test", output_dir=str(tmp_path))
        rp = Path(result["recipe_path"])
        assert rp.exists()
        assert rp.suffix == ".json"

        data = json.loads(rp.read_text())
        assert data["chart_type"] == "bar"
        assert data["sql"] == 'SELECT month, revenue FROM "shop"."sales"'

    def test_save_no_recipe(self):
        with pytest.raises(ValueError, match="No recipe found"):
            save_recipe()

    def test_save_uses_recipe_name(self, connected: SourceRegistry, tmp_path: Path):
        create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="line",
            x="month",
            y="revenue",
            title="My Chart",
            output_path=str(tmp_path / "c.png"),
        )
        result = save_recipe(output_dir=str(tmp_path))
        assert "my_chart" in Path(result["recipe_path"]).stem


# ---------------------------------------------------------------------------
# replay_chart
# ---------------------------------------------------------------------------


class TestReplayChart:
    def test_replay(self, connected: SourceRegistry, tmp_path: Path):
        # Create and save
        create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Replay Test",
            output_path=str(tmp_path / "original.png"),
        )
        save_result = save_recipe(name="replay", output_dir=str(tmp_path))

        # Replay
        replay_out = tmp_path / "replayed.png"
        result = replay_chart(
            connected,
            recipe_path=save_result["recipe_path"],
            output_path=str(replay_out),
        )
        assert replay_out.exists()
        assert result["chart_type"] == "bar"
        assert result["rows_plotted"] == 6

    def test_replay_missing_file(self, connected: SourceRegistry):
        with pytest.raises(FileNotFoundError):
            replay_chart(connected, recipe_path="nonexistent.recipe.json")


# ---------------------------------------------------------------------------
# export_insight
# ---------------------------------------------------------------------------


class TestExportInsight:
    def _create_recipe(self, connected: SourceRegistry, tmp_path: Path) -> str:
        """Helper: create+save a recipe, return recipe_path."""
        create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Export Test",
            output_path=str(tmp_path / "export.png"),
        )
        r = save_recipe(name="export_test", output_dir=str(tmp_path))
        return r["recipe_path"]

    def test_export_sql(self, connected: SourceRegistry, tmp_path: Path):
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "export.sql"
        result = export_insight(recipe_path=rp, export_format="sql", output_path=str(out))
        assert out.exists()
        content = out.read_text()
        assert "SELECT" in content
        assert result["format"] == "sql"

    def test_export_script(self, connected: SourceRegistry, tmp_path: Path):
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "export.py"
        result = export_insight(recipe_path=rp, export_format="script", output_path=str(out))
        assert out.exists()
        content = out.read_text()
        assert "import duckdb" in content
        assert "matplotlib" in content
        assert result["format"] == "script"

    def test_export_script_indentation(self, connected: SourceRegistry, tmp_path: Path):
        """Verify that the generated script has no broken indentation."""
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "indent_check.py"
        export_insight(recipe_path=rp, export_format="script", output_path=str(out))
        content = out.read_text()
        # No line should start with unexpected whitespace at the top level
        for i, line in enumerate(content.splitlines(), 1):
            stripped = line.lstrip()
            # Skip blank lines
            if not stripped:
                continue
            indent = len(line) - len(stripped)
            # Top-level statements should be at indent 0
            # Indented code (inside for/if) should be multiples of 4
            assert indent % 4 == 0, f"Line {i} has {indent}-space indent: {line!r}"

    def test_export_script_output_path_in_charts_dir(
        self, connected: SourceRegistry, tmp_path: Path
    ):
        """Verify the generated script saves charts to charts/ not the root directory."""
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "path_check.py"
        export_insight(recipe_path=rp, export_format="script", output_path=str(out))
        content = out.read_text()
        assert 'Path("charts")' in content
        assert "mkdir" in content

    def test_export_script_has_shebang(self, connected: SourceRegistry, tmp_path: Path):
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "shebang_check.py"
        export_insight(recipe_path=rp, export_format="script", output_path=str(out))
        content = out.read_text()
        assert content.startswith("#!/usr/bin/env python3")

    def test_export_script_is_valid_python(self, connected: SourceRegistry, tmp_path: Path):
        """Verify the generated script compiles without syntax errors."""
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "syntax_check.py"
        export_insight(recipe_path=rp, export_format="script", output_path=str(out))
        content = out.read_text()
        compile(content, str(out), "exec")  # raises SyntaxError if invalid

    def test_export_script_with_color(self, connected: SourceRegistry, tmp_path: Path):
        """Verify color-grouped script also has valid indentation and compiles."""
        create_chart(
            connected,
            sql='SELECT month, revenue, region FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            color="region",
            title="Color Script",
            output_path=str(tmp_path / "color.png"),
        )
        save_recipe(name="color_script", output_dir=str(tmp_path))
        rp = str(tmp_path / "color_script.recipe.json")
        out = tmp_path / "color_check.py"
        export_insight(recipe_path=rp, export_format="script", output_path=str(out))
        content = out.read_text()
        compile(content, str(out), "exec")
        assert "groups" in content
        assert 'Path("charts")' in content

    def test_export_notebook(self, connected: SourceRegistry, tmp_path: Path):
        rp = self._create_recipe(connected, tmp_path)
        out = tmp_path / "export.ipynb"
        result = export_insight(
            recipe_path=rp, export_format="notebook", output_path=str(out)
        )
        assert out.exists()
        nb = json.loads(out.read_text())
        assert nb["nbformat"] == 4
        assert len(nb["cells"]) >= 3
        assert result["format"] == "notebook"

    def test_export_from_memory(self, connected: SourceRegistry, tmp_path: Path):
        create_chart(
            connected,
            sql='SELECT month, revenue FROM "shop"."sales"',
            chart_type="bar",
            x="month",
            y="revenue",
            title="Mem Export",
            output_path=str(tmp_path / "mem.png"),
        )
        out = tmp_path / "mem.sql"
        result = export_insight(export_format="sql", output_path=str(out))
        assert out.exists()
        assert "SELECT" in out.read_text()

    def test_export_invalid_format(self, connected: SourceRegistry, tmp_path: Path):
        self._create_recipe(connected, tmp_path)
        with pytest.raises(ValueError, match="Unsupported format"):
            export_insight(export_format="pdf")

    def test_export_no_recipe(self):
        with pytest.raises(ValueError, match="No recipe found"):
            export_insight(export_format="sql")


# ---------------------------------------------------------------------------
# Supported chart types constant
# ---------------------------------------------------------------------------


class TestConstants:
    def test_supported_types(self):
        assert "bar" in SUPPORTED_CHART_TYPES
        assert "line" in SUPPORTED_CHART_TYPES
        assert "scatter" in SUPPORTED_CHART_TYPES
        assert "histogram" in SUPPORTED_CHART_TYPES
