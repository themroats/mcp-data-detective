"""
Synthetic e-commerce data generator.

Generates realistic-looking data with intentional quality issues for demo purposes:
- Duplicate orders
- Null emails in customers
- Negative product prices
- Anomalous revenue spikes/dips
- Future-dated timestamps

Usage:
    data-detective-seed                              # Default: outputs to ./data/
    data-detective-seed --output ./my-data            # Custom output directory
    data-detective-seed --rows 50000                  # Control row count
    data-detective-seed --format sqlite               # Output as SQLite DB instead of Parquet
    data-detective-seed --null-email-rate 0.2          # 20% null emails
    data-detective-seed --no-defects                   # Generate clean data (no quality issues)
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import sys
from dataclasses import dataclass
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
# Generation config
# ---------------------------------------------------------------------------


@dataclass
class GeneratorConfig:
    """Configuration for the synthetic data generator.

    All injection rates and entity ratios are overridable via CLI flags.
    Use :meth:`no_defects` to get a config with all quality issues disabled.
    """

    # Injection rates for intentional quality issues
    null_email_rate: float = 0.08         # fraction of customers with missing email
    inactive_rate: float = 0.15           # fraction of inactive customers
    negative_price_rate: float = 0.03     # fraction of products with negative price
    duplicate_order_rate: float = 0.015   # fraction of orders duplicated
    future_timestamp_rate: float = 0.005  # fraction of events with future timestamps

    # Anomaly configuration
    anomaly_month: int = 3                # month with depressed order quantities (0 = disabled)

    # Entity ratios (relative to --rows)
    customer_ratio: float = 0.1           # customers = rows * ratio (min 100)
    product_ratio: float = 0.02           # products  = rows * ratio (min 50)
    event_multiplier: int = 5             # events    = rows * multiplier

    # Value ranges
    price_min: float = 5.0
    price_max: float = 500.0
    quantity_min: int = 1
    quantity_max: int = 10
    inventory_min: int = 0
    inventory_max: int = 1000
    order_date_span_days: int = 545
    signup_lookback: str = "-3y"

    @classmethod
    def no_defects(cls, **overrides: Any) -> GeneratorConfig:
        """Return a config with all quality-issue injection rates set to zero."""
        return cls(
            null_email_rate=0.0,
            inactive_rate=0.0,
            negative_price_rate=0.0,
            duplicate_order_rate=0.0,
            future_timestamp_rate=0.0,
            anomaly_month=0,
            **overrides,
        )


# ---------------------------------------------------------------------------
# Data generation helpers
# ---------------------------------------------------------------------------

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books",
    "Sports", "Toys", "Health", "Automotive",
]

STATUS_OPTIONS = ["completed", "pending", "shipped", "cancelled", "returned"]


def generate_customers(n: int, cfg: GeneratorConfig | None = None) -> list[dict[str, Any]]:
    """Generate customer records."""
    cfg = cfg or GeneratorConfig()
    customers = []
    regions = ["US-West", "US-East", "US-Central", "EU-West", "EU-East", "APAC"]

    for i in range(1, n + 1):
        email = fake.email() if random.random() > cfg.null_email_rate else None
        customers.append(
            {
                "customer_id": i,
                "name": fake.name(),
                "email": email,
                "region": random.choice(regions),
                "signup_date": fake.date_between(start_date=cfg.signup_lookback, end_date="today").isoformat(),
                "is_active": random.random() > cfg.inactive_rate,
            }
        )
    return customers


def generate_products(n: int, cfg: GeneratorConfig | None = None) -> list[dict[str, Any]]:
    """Generate product records."""
    cfg = cfg or GeneratorConfig()
    products = []
    for i in range(1, n + 1):
        price = round(random.uniform(cfg.price_min, cfg.price_max), 2)
        if random.random() < cfg.negative_price_rate:
            price = -abs(price)

        products.append(
            {
                "product_id": i,
                "name": fake.catch_phrase(),
                "category": random.choice(PRODUCT_CATEGORIES),
                "price": price,
                "inventory_count": random.randint(cfg.inventory_min, cfg.inventory_max),
            }
        )
    return products


def generate_orders(
    n: int, n_customers: int, n_products: int, cfg: GeneratorConfig | None = None,
) -> list[dict[str, Any]]:
    """Generate order records."""
    cfg = cfg or GeneratorConfig()
    orders: list[dict[str, Any]] = []
    base_date = datetime(2024, 1, 1)

    for i in range(1, n + 1):
        order_date = base_date + timedelta(days=random.randint(0, cfg.order_date_span_days))
        quantity = random.randint(cfg.quantity_min, cfg.quantity_max)
        unit_price = round(random.uniform(cfg.price_min, cfg.price_max), 2)

        # Create anomaly: target month orders have much lower quantities
        if cfg.anomaly_month and order_date.month == cfg.anomaly_month:
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

    # Inject duplicate orders (same data, different order_id)
    rate = cfg.duplicate_order_rate
    n_dups = max(1, int(n * rate)) if rate > 0 else 0
    for _ in range(n_dups):
        src = random.choice(orders)
        dup = dict(src)
        dup["order_id"] = len(orders) + 1
        orders.append(dup)

    return orders


def generate_events(
    n: int, n_customers: int, n_products: int, cfg: GeneratorConfig | None = None,
) -> list[dict[str, Any]]:
    """Generate web event records."""
    cfg = cfg or GeneratorConfig()
    event_types = ["page_view", "click", "add_to_cart", "purchase", "search"]
    pages = ["/home", "/product", "/category", "/cart", "/checkout", "/search", "/account"]
    events = []
    now = datetime.now()

    for i in range(1, n + 1):
        ts = fake.date_time_between(start_date="-1y", end_date="now")
        if random.random() < cfg.future_timestamp_rate:
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
    _defaults = GeneratorConfig()

    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce demo data.")
    parser.add_argument("--output", "-o", default="./data", help="Output directory (default: ./data)")
    parser.add_argument("--rows", "-n", type=int, default=10000, help="Base row count for orders table (default: 10000)")
    parser.add_argument("--format", "-f", choices=["parquet", "sqlite", "both"], default="both", help="Output format (default: both)")

    # Quality issue injection rates
    parser.add_argument("--null-email-rate", type=float, default=_defaults.null_email_rate,
                        help=f"Fraction of customers with NULL emails (default: {_defaults.null_email_rate})")
    parser.add_argument("--negative-price-rate", type=float, default=_defaults.negative_price_rate,
                        help=f"Fraction of products with negative prices (default: {_defaults.negative_price_rate})")
    parser.add_argument("--duplicate-rate", type=float, default=_defaults.duplicate_order_rate,
                        help=f"Fraction of duplicate orders (default: {_defaults.duplicate_order_rate})")
    parser.add_argument("--future-ts-rate", type=float, default=_defaults.future_timestamp_rate,
                        help=f"Fraction of events with future timestamps (default: {_defaults.future_timestamp_rate})")
    parser.add_argument("--anomaly-month", type=int, default=_defaults.anomaly_month,
                        choices=range(1, 13), metavar="{1..12}",
                        help=f"Month with anomalous low quantities (default: {_defaults.anomaly_month})")

    # Entity ratios
    parser.add_argument("--customer-ratio", type=float, default=_defaults.customer_ratio,
                        help=f"Customer count as fraction of orders (default: {_defaults.customer_ratio})")
    parser.add_argument("--product-ratio", type=float, default=_defaults.product_ratio,
                        help=f"Product count as fraction of orders (default: {_defaults.product_ratio})")
    parser.add_argument("--event-multiplier", type=int, default=_defaults.event_multiplier,
                        help=f"Events per order row (default: {_defaults.event_multiplier})")

    # Convenience flag
    parser.add_argument("--no-defects", action="store_true",
                        help="Disable all intentional quality issues (sets all injection rates to 0)")

    args = parser.parse_args()

    # Build config from CLI args
    if args.no_defects:
        cfg = GeneratorConfig.no_defects(
            customer_ratio=args.customer_ratio,
            product_ratio=args.product_ratio,
            event_multiplier=args.event_multiplier,
        )
    else:
        cfg = GeneratorConfig(
            null_email_rate=args.null_email_rate,
            negative_price_rate=args.negative_price_rate,
            duplicate_order_rate=args.duplicate_rate,
            future_timestamp_rate=args.future_ts_rate,
            anomaly_month=args.anomaly_month,
            customer_ratio=args.customer_ratio,
            product_ratio=args.product_ratio,
            event_multiplier=args.event_multiplier,
        )

    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    n_orders = args.rows
    n_customers = max(100, int(n_orders * cfg.customer_ratio))
    n_products = max(50, int(n_orders * cfg.product_ratio))
    n_events = n_orders * cfg.event_multiplier

    dup_estimate = int(n_orders * cfg.duplicate_order_rate)
    print(f"Generating synthetic e-commerce data...")
    print(f"  Customers: {n_customers:,}")
    print(f"  Products:  {n_products:,}")
    print(f"  Orders:    {n_orders:,} (+ ~{dup_estimate:,} duplicates)")
    print(f"  Events:    {n_events:,}")
    if args.no_defects:
        print(f"  Defects:   DISABLED")
    print()

    customers = generate_customers(n_customers, cfg)
    products = generate_products(n_products, cfg)
    orders = generate_orders(n_orders, n_customers, n_products, cfg)
    events = generate_events(n_events, n_customers, n_products, cfg)

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
    print("Done!")
    if not args.no_defects:
        print("Intentional quality issues injected:")
        if cfg.null_email_rate > 0:
            print(f"  - ~{cfg.null_email_rate:.0%} of customer emails are NULL")
        if cfg.negative_price_rate > 0:
            print(f"  - ~{cfg.negative_price_rate:.0%} of product prices are negative")
        if cfg.duplicate_order_rate > 0:
            print(f"  - ~{cfg.duplicate_order_rate:.1%} of orders are duplicates")
        if cfg.anomaly_month:
            print(f"  - Month {cfg.anomaly_month} orders have anomalously low quantities")
        if cfg.future_timestamp_rate > 0:
            print(f"  - ~{cfg.future_timestamp_rate:.1%} of event timestamps are in the future")
    else:
        print("No defects injected (--no-defects).")


if __name__ == "__main__":
    main()
