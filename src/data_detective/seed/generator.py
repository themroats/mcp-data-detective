"""
Synthetic e-commerce data generator.

Generates realistic-looking data with intentional quality issues for demo purposes:
- Duplicate orders
- Null emails in customers
- Negative product prices
- Anomalous revenue spikes/dips
- Future-dated timestamps

Usage:
    data-detective-seed                         # Default: outputs to ./data/
    data-detective-seed --output ./my-data      # Custom output directory
    data-detective-seed --rows 50000            # Control row count
    data-detective-seed --format sqlite         # Output as SQLite DB instead of Parquet
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

try:
    from faker import Faker
except ImportError:
    print("Error: faker is required. Install with: pip install faker")
    sys.exit(1)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Data generation helpers
# ---------------------------------------------------------------------------

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books",
    "Sports", "Toys", "Health", "Automotive",
]

STATUS_OPTIONS = ["completed", "pending", "shipped", "cancelled", "returned"]


def generate_customers(n: int) -> list[dict[str, Any]]:
    """Generate customer records. ~8% will have null emails."""
    customers = []
    regions = ["US-West", "US-East", "US-Central", "EU-West", "EU-East", "APAC"]

    for i in range(1, n + 1):
        email = fake.email() if random.random() > 0.08 else None  # 8% null emails
        customers.append(
            {
                "customer_id": i,
                "name": fake.name(),
                "email": email,
                "region": random.choice(regions),
                "signup_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
                "is_active": random.random() > 0.15,
            }
        )
    return customers


def generate_products(n: int) -> list[dict[str, Any]]:
    """Generate product records. ~3% will have negative prices (data quality bug)."""
    products = []
    for i in range(1, n + 1):
        price = round(random.uniform(5, 500), 2)
        # Inject ~3% negative prices
        if random.random() < 0.03:
            price = -abs(price)

        products.append(
            {
                "product_id": i,
                "name": fake.catch_phrase(),
                "category": random.choice(PRODUCT_CATEGORIES),
                "price": price,
                "inventory_count": random.randint(0, 1000),
            }
        )
    return products


def generate_orders(
    n: int, n_customers: int, n_products: int
) -> list[dict[str, Any]]:
    """Generate order records with intentional duplicates and an anomalous revenue spike."""
    orders: list[dict[str, Any]] = []
    base_date = datetime(2024, 1, 1)
    anomaly_month = 3  # March gets a 40% revenue drop

    for i in range(1, n + 1):
        order_date = base_date + timedelta(days=random.randint(0, 545))
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5, 500), 2)

        # Create anomaly: March orders have much lower quantities
        if order_date.month == anomaly_month:
            quantity = max(1, quantity // 3)

        total_amount = round(quantity * unit_price, 2)

        orders.append(
            {
                "order_id": i,
                "customer_id": random.randint(1, n_customers),
                "product_id": random.randint(1, n_products),
                "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "status": random.choice(STATUS_OPTIONS),
            }
        )

    # Inject ~1.5% duplicate orders (same data, different order_id)
    n_dups = max(1, int(n * 0.015))
    for _ in range(n_dups):
        src = random.choice(orders)
        dup = dict(src)
        dup["order_id"] = len(orders) + 1
        orders.append(dup)

    return orders


def generate_events(
    n: int, n_customers: int, n_products: int
) -> list[dict[str, Any]]:
    """Generate web event records. ~0.5% will have timestamps in the future."""
    event_types = ["page_view", "click", "add_to_cart", "purchase", "search"]
    pages = ["/home", "/product", "/category", "/cart", "/checkout", "/search", "/account"]
    events = []
    now = datetime.now()

    for i in range(1, n + 1):
        ts = fake.date_time_between(start_date="-1y", end_date="now")
        # Inject ~0.5% future timestamps
        if random.random() < 0.005:
            ts = now + timedelta(days=random.randint(1, 365))

        events.append(
            {
                "event_id": i,
                "customer_id": random.randint(1, n_customers) if random.random() > 0.2 else None,
                "product_id": random.randint(1, n_products) if random.random() > 0.4 else None,
                "event_type": random.choice(event_types),
                "page": random.choice(pages),
                "session_id": fake.uuid4(),
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return events


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def _write_parquet(records: list[dict], path: Path, name: str) -> Path:
    if pa is None or pq is None:
        raise ImportError("pyarrow is required for Parquet output. pip install pyarrow")
    table = pa.Table.from_pylist(records)
    filepath = path / f"{name}.parquet"
    pq.write_table(table, str(filepath))
    return filepath


def _write_sqlite(tables: dict[str, list[dict]], path: Path) -> Path:
    db_path = path / "ecommerce.db"
    conn = sqlite3.connect(str(db_path))

    for table_name, records in tables.items():
        if not records:
            continue
        cols = list(records[0].keys())
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
        placeholders = ", ".join("?" for _ in cols)
        conn.executemany(
            f'INSERT INTO "{table_name}" VALUES ({placeholders})',
            [tuple(str(r.get(c, "")) if r.get(c) is not None else None for c in cols) for r in records],
        )

    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce demo data.")
    parser.add_argument("--output", "-o", default="./data", help="Output directory (default: ./data)")
    parser.add_argument("--rows", "-n", type=int, default=10000, help="Base row count for orders table (default: 10000)")
    parser.add_argument("--format", "-f", choices=["parquet", "sqlite", "both"], default="both", help="Output format (default: both)")
    args = parser.parse_args()

    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    n_orders = args.rows
    n_customers = max(100, n_orders // 10)
    n_products = max(50, n_orders // 50)
    n_events = n_orders * 5

    print(f"Generating synthetic e-commerce data...")
    print(f"  Customers: {n_customers:,}")
    print(f"  Products:  {n_products:,}")
    print(f"  Orders:    {n_orders:,} (+ ~{int(n_orders * 0.015):,} duplicates)")
    print(f"  Events:    {n_events:,}")
    print()

    customers = generate_customers(n_customers)
    products = generate_products(n_products)
    orders = generate_orders(n_orders, n_customers, n_products)
    events = generate_events(n_events, n_customers, n_products)

    tables = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "events": events,
    }

    if args.format in ("parquet", "both"):
        print(f"Writing Parquet files to {output_dir}/")
        for name, records in tables.items():
            fp = _write_parquet(records, output_dir, name)
            print(f"  {fp.name} ({len(records):,} rows)")

    if args.format in ("sqlite", "both"):
        print(f"Writing SQLite database to {output_dir}/")
        db_path = _write_sqlite(tables, output_dir)
        print(f"  {db_path.name} ({sum(len(r) for r in tables.values()):,} total rows)")

    print()
    print("Done! Intentional quality issues injected:")
    print("  - ~8% of customer emails are NULL")
    print("  - ~3% of product prices are negative")
    print("  - ~1.5% of orders are duplicates")
    print("  - March orders have anomalously low quantities (revenue dip)")
    print("  - ~0.5% of event timestamps are in the future")


if __name__ == "__main__":
    main()
