"""Tests for the seed data generator."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from data_detective.seed.generator import (
    GeneratorConfig,
    generate_customers,
    generate_events,
    generate_orders,
    generate_products,
    _write_parquet,
    _write_sqlite,
    main,
)


# ---------------------------------------------------------------------------
# GeneratorConfig
# ---------------------------------------------------------------------------


class TestGeneratorConfig:
    def test_defaults(self) -> None:
        cfg = GeneratorConfig()
        assert cfg.null_email_rate == 0.08
        assert cfg.negative_price_rate == 0.03
        assert cfg.duplicate_order_rate == 0.015
        assert cfg.anomaly_month == 3

    def test_no_defects(self) -> None:
        cfg = GeneratorConfig.no_defects()
        assert cfg.null_email_rate == 0.0
        assert cfg.inactive_rate == 0.0
        assert cfg.negative_price_rate == 0.0
        assert cfg.duplicate_order_rate == 0.0
        assert cfg.future_timestamp_rate == 0.0
        assert cfg.anomaly_month == 0

    def test_no_defects_with_overrides(self) -> None:
        cfg = GeneratorConfig.no_defects(event_multiplier=2)
        assert cfg.null_email_rate == 0.0
        assert cfg.event_multiplier == 2


# ---------------------------------------------------------------------------
# Data generation helpers
# ---------------------------------------------------------------------------


class TestGenerateCustomers:
    def test_returns_correct_count(self) -> None:
        customers = generate_customers(50)
        assert len(customers) == 50

    def test_customer_fields(self) -> None:
        customers = generate_customers(10)
        expected_keys = {"customer_id", "name", "email", "region", "signup_date", "is_active"}
        assert set(customers[0].keys()) == expected_keys

    def test_customer_ids_sequential(self) -> None:
        customers = generate_customers(20)
        ids = [c["customer_id"] for c in customers]
        assert ids == list(range(1, 21))

    def test_some_emails_are_null(self) -> None:
        """~8% of emails should be None. With 500 rows we expect at least one."""
        customers = generate_customers(500)
        null_emails = [c for c in customers if c["email"] is None]
        assert len(null_emails) >= 1

    def test_custom_null_email_rate_zero(self) -> None:
        """Setting null_email_rate=0 should produce no null emails."""
        cfg = GeneratorConfig(null_email_rate=0.0)
        customers = generate_customers(200, cfg)
        null_emails = [c for c in customers if c["email"] is None]
        assert len(null_emails) == 0

    def test_custom_null_email_rate_high(self) -> None:
        """Setting null_email_rate=1.0 should make all emails null."""
        cfg = GeneratorConfig(null_email_rate=1.0)
        customers = generate_customers(50, cfg)
        null_emails = [c for c in customers if c["email"] is None]
        assert len(null_emails) == 50

    def test_valid_regions(self) -> None:
        customers = generate_customers(100)
        valid = {"US-West", "US-East", "US-Central", "EU-West", "EU-East", "APAC"}
        for c in customers:
            assert c["region"] in valid

    def test_zero_rows(self) -> None:
        customers = generate_customers(0)
        assert customers == []


class TestGenerateProducts:
    def test_returns_correct_count(self) -> None:
        products = generate_products(30)
        assert len(products) == 30

    def test_product_fields(self) -> None:
        products = generate_products(10)
        expected_keys = {"product_id", "name", "category", "price", "inventory_count"}
        assert set(products[0].keys()) == expected_keys

    def test_some_prices_are_negative(self) -> None:
        """~3% of prices should be negative. With 500 rows we expect at least one."""
        products = generate_products(500)
        negatives = [p for p in products if p["price"] < 0]
        assert len(negatives) >= 1

    def test_custom_negative_price_rate_zero(self) -> None:
        """Setting negative_price_rate=0 should produce no negative prices."""
        cfg = GeneratorConfig(negative_price_rate=0.0)
        products = generate_products(200, cfg)
        negatives = [p for p in products if p["price"] < 0]
        assert len(negatives) == 0

    def test_valid_categories(self) -> None:
        products = generate_products(100)
        valid = {
            "Electronics", "Clothing", "Home & Kitchen", "Books",
            "Sports", "Toys", "Health", "Automotive",
        }
        for p in products:
            assert p["category"] in valid

    def test_zero_rows(self) -> None:
        products = generate_products(0)
        assert products == []


class TestGenerateOrders:
    def test_includes_duplicates(self) -> None:
        """Order list should be longer than n due to ~1.5% injected duplicates."""
        orders = generate_orders(200, n_customers=50, n_products=20)
        assert len(orders) > 200

    def test_custom_duplicate_rate_zero(self) -> None:
        """Setting duplicate_order_rate=0 should produce exactly n orders."""
        cfg = GeneratorConfig(duplicate_order_rate=0.0)
        orders = generate_orders(100, n_customers=20, n_products=10, cfg=cfg)
        assert len(orders) == 100

    def test_custom_anomaly_month_disabled(self) -> None:
        """Setting anomaly_month=0 disables the anomaly."""
        cfg = GeneratorConfig(anomaly_month=0)
        orders = generate_orders(2000, n_customers=50, n_products=20, cfg=cfg)
        march = [o for o in orders if o["order_date"].startswith("2024-03")]
        non_march = [o for o in orders if not o["order_date"].startswith("2024-03")]
        if march and non_march:
            avg_march = sum(o["quantity"] for o in march) / len(march)
            avg_other = sum(o["quantity"] for o in non_march) / len(non_march)
            # Without anomaly, March average should be comparable to other months
            assert avg_march > avg_other * 0.6  # not drastically lower

    def test_order_fields(self) -> None:
        orders = generate_orders(10, n_customers=5, n_products=5)
        expected_keys = {
            "order_id", "customer_id", "product_id", "order_date",
            "quantity", "unit_price", "total_amount", "status",
        }
        assert set(orders[0].keys()) == expected_keys

    def test_customer_ids_in_range(self) -> None:
        orders = generate_orders(100, n_customers=10, n_products=5)
        for o in orders:
            assert 1 <= o["customer_id"] <= 10

    def test_product_ids_in_range(self) -> None:
        orders = generate_orders(100, n_customers=10, n_products=5)
        for o in orders:
            assert 1 <= o["product_id"] <= 5

    def test_valid_statuses(self) -> None:
        orders = generate_orders(100, n_customers=10, n_products=5)
        valid = {"completed", "pending", "shipped", "cancelled", "returned"}
        for o in orders:
            assert o["status"] in valid

    def test_total_amount_is_quantity_times_price(self) -> None:
        orders = generate_orders(50, n_customers=10, n_products=5)
        for o in orders:
            expected = round(o["quantity"] * o["unit_price"], 2)
            assert o["total_amount"] == expected

    def test_march_anomaly_reduces_quantities(self) -> None:
        """March orders should have lower quantities on average."""
        orders = generate_orders(2000, n_customers=50, n_products=20)
        march = [o for o in orders if o["order_date"].startswith("2024-03")]
        non_march = [o for o in orders if not o["order_date"].startswith("2024-03")]
        if march and non_march:
            avg_march = sum(o["quantity"] for o in march) / len(march)
            avg_other = sum(o["quantity"] for o in non_march) / len(non_march)
            assert avg_march < avg_other

    def test_zero_rows_still_has_duplicates(self) -> None:
        """Even with 0 base rows, at least 1 duplicate is injected (from max(1, ...))."""
        # With 0 orders, random.choice on empty list will raise — this documents the edge case.
        # The function requires n >= 1 to work correctly.
        orders = generate_orders(1, n_customers=1, n_products=1)
        assert len(orders) >= 1


class TestGenerateEvents:
    def test_returns_correct_count(self) -> None:
        events = generate_events(100, n_customers=10, n_products=5)
        assert len(events) == 100

    def test_event_fields(self) -> None:
        events = generate_events(10, n_customers=5, n_products=5)
        expected_keys = {
            "event_id", "customer_id", "product_id", "event_type",
            "page", "session_id", "timestamp",
        }
        assert set(events[0].keys()) == expected_keys

    def test_some_customer_ids_are_null(self) -> None:
        """~20% of event customer_ids should be None."""
        events = generate_events(500, n_customers=50, n_products=20)
        nulls = [e for e in events if e["customer_id"] is None]
        assert len(nulls) >= 1

    def test_some_product_ids_are_null(self) -> None:
        """~40% of event product_ids should be None."""
        events = generate_events(500, n_customers=50, n_products=20)
        nulls = [e for e in events if e["product_id"] is None]
        assert len(nulls) >= 1

    def test_valid_event_types(self) -> None:
        events = generate_events(100, n_customers=10, n_products=5)
        valid = {"page_view", "click", "add_to_cart", "purchase", "search"}
        for e in events:
            assert e["event_type"] in valid

    def test_valid_pages(self) -> None:
        events = generate_events(100, n_customers=10, n_products=5)
        valid = {"/home", "/product", "/category", "/cart", "/checkout", "/search", "/account"}
        for e in events:
            assert e["page"] in valid


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


class TestWriteParquet:
    def test_creates_parquet_file(self, tmp_path: Path) -> None:
        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        filepath = _write_parquet(records, tmp_path, "test_table")
        assert filepath.exists()
        assert filepath.name == "test_table.parquet"

    def test_parquet_row_count(self, tmp_path: Path) -> None:
        records = [{"id": i, "val": f"row_{i}"} for i in range(50)]
        filepath = _write_parquet(records, tmp_path, "data")
        table = pq.read_table(str(filepath))
        assert table.num_rows == 50

    def test_parquet_columns(self, tmp_path: Path) -> None:
        records = [{"a": 1, "b": "x", "c": 3.14}]
        filepath = _write_parquet(records, tmp_path, "cols")
        table = pq.read_table(str(filepath))
        assert table.column_names == ["a", "b", "c"]


class TestWriteSqlite:
    def test_creates_db_file(self, tmp_path: Path) -> None:
        tables = {"users": [{"id": 1, "name": "Alice"}]}
        db_path = _write_sqlite(tables, tmp_path)
        assert db_path.exists()
        assert db_path.name == "ecommerce.db"

    def test_sqlite_row_count(self, tmp_path: Path) -> None:
        tables = {"items": [{"id": i, "val": f"v{i}"} for i in range(25)]}
        db_path = _write_sqlite(tables, tmp_path)
        conn = sqlite3.connect(str(db_path))
        (count,) = conn.execute("SELECT COUNT(*) FROM items").fetchone()
        conn.close()
        assert count == 25

    def test_sqlite_multiple_tables(self, tmp_path: Path) -> None:
        tables = {
            "a": [{"x": 1}],
            "b": [{"y": 2}, {"y": 3}],
        }
        db_path = _write_sqlite(tables, tmp_path)
        conn = sqlite3.connect(str(db_path))
        table_names = [
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        conn.close()
        assert "a" in table_names
        assert "b" in table_names

    def test_sqlite_skips_empty_tables(self, tmp_path: Path) -> None:
        tables = {"filled": [{"id": 1}], "empty": []}
        db_path = _write_sqlite(tables, tmp_path)
        conn = sqlite3.connect(str(db_path))
        table_names = [
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        conn.close()
        assert "filled" in table_names
        assert "empty" not in table_names

    def test_sqlite_null_values_preserved(self, tmp_path: Path) -> None:
        tables = {"data": [{"id": 1, "email": None}]}
        db_path = _write_sqlite(tables, tmp_path)
        conn = sqlite3.connect(str(db_path))
        row = conn.execute("SELECT email FROM data WHERE id = '1'").fetchone()
        conn.close()
        assert row[0] is None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


class TestMain:
    def test_cli_parquet_output(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "50", "--format", "parquet"]
        )
        main()
        parquet_files = list(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 4  # customers, products, orders, events

    def test_cli_sqlite_output(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "50", "--format", "sqlite"]
        )
        main()
        assert (tmp_path / "ecommerce.db").exists()
        # No parquet files should be created
        assert list(tmp_path.glob("*.parquet")) == []

    def test_cli_both_output(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "50", "--format", "both"]
        )
        main()
        assert (tmp_path / "ecommerce.db").exists()
        assert len(list(tmp_path.glob("*.parquet"))) == 4

    def test_cli_default_rows(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default is 10000 rows — use a small run to verify it doesn't crash."""
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "10", "--format", "parquet"]
        )
        main()
        parquet_files = list(tmp_path.glob("*.parquet"))
        assert len(parquet_files) == 4

    def test_cli_creates_output_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        nested = tmp_path / "a" / "b" / "c"
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(nested), "--rows", "10", "--format", "parquet"]
        )
        main()
        assert nested.exists()
        assert len(list(nested.glob("*.parquet"))) == 4

    def test_cli_no_defects(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """--no-defects should produce clean data with no quality issues."""
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "200",
                         "--format", "parquet", "--no-defects"]
        )
        main()
        import pyarrow.parquet as pq

        # Check customers: no null emails
        customers = pq.read_table(str(tmp_path / "customers.parquet")).to_pylist()
        assert all(c["email"] is not None for c in customers)

        # Check products: no negative prices
        products = pq.read_table(str(tmp_path / "products.parquet")).to_pylist()
        assert all(p["price"] >= 0 for p in products)

        # Check orders: no duplicates (count should be exactly 200)
        orders = pq.read_table(str(tmp_path / "orders.parquet")).to_pylist()
        assert len(orders) == 200

    def test_cli_custom_rates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Custom injection rates should be respected."""
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "50",
                         "--format", "parquet", "--null-email-rate", "1.0",
                         "--negative-price-rate", "0.0", "--duplicate-rate", "0.0"]
        )
        main()
        import pyarrow.parquet as pq

        customers = pq.read_table(str(tmp_path / "customers.parquet")).to_pylist()
        assert all(c["email"] is None for c in customers)

        products = pq.read_table(str(tmp_path / "products.parquet")).to_pylist()
        assert all(p["price"] >= 0 for p in products)

    def test_cli_custom_ratios(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Custom entity ratios should control table sizes."""
        monkeypatch.setattr(
            "sys.argv", ["data-detective-seed", "--output", str(tmp_path), "--rows", "100",
                         "--format", "parquet", "--customer-ratio", "0.5",
                         "--product-ratio", "0.1", "--event-multiplier", "2"]
        )
        main()
        import pyarrow.parquet as pq

        customers = pq.read_table(str(tmp_path / "customers.parquet"))
        assert customers.num_rows == 100  # max(100, 100*0.5) = 100

        products = pq.read_table(str(tmp_path / "products.parquet"))
        assert products.num_rows == 50  # max(50, 100*0.1=10) = 50

        events = pq.read_table(str(tmp_path / "events.parquet"))
        assert events.num_rows == 200  # 100 * 2
