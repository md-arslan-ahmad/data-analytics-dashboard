"""
analyze.py — Part 2: Data Merging & Analysis
Merges cleaned datasets and derives business insights.
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_PROCESSED_DIR = Path(__file__).parent / "data" / "processed"
DEFAULT_RAW_DIR = Path(__file__).parent / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV with FileNotFoundError and EmptyDataError handling."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError as exc:
        raise pd.errors.EmptyDataError(f"File is empty: {path}") from exc
    if df.empty:
        raise pd.errors.EmptyDataError(f"No data rows in: {path}")
    logger.info("Loaded %d rows from %s", len(df), path.name)
    return df


# ---------------------------------------------------------------------------
# 2.1 Merging
# ---------------------------------------------------------------------------

def build_full_data(
    customers_clean: pd.DataFrame,
    orders_clean: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge cleaned datasets:
      1. Left-join orders onto customers on customer_id  → orders_with_customers
      2. Left-join products onto that on product/product_name → full_data
    """
    orders_with_customers = pd.merge(
        orders_clean,
        customers_clean,
        on="customer_id",
        how="left",
        suffixes=("_order", "_customer"),
    )

    orders_no_customer = orders_with_customers["name"].isna().sum()
    logger.info("Orders with no matching customer: %d", orders_no_customer)
    print(f"\n  ⚠ Orders with no matching customer : {orders_no_customer}")

    full_data = pd.merge(
        orders_with_customers,
        products,
        left_on="product",
        right_on="product_name",
        how="left",
    )

    orders_no_product = full_data["product_id"].isna().sum()
    logger.info("Orders with no matching product: %d", orders_no_product)
    print(f"  ⚠ Orders with no matching product  : {orders_no_product}\n")

    return full_data


# ---------------------------------------------------------------------------
# 2.2 Analysis Tasks
# ---------------------------------------------------------------------------

def monthly_revenue(full_data: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    """Task 1 — Monthly revenue trend (completed orders)."""
    completed = full_data[full_data["status"] == "completed"].copy()
    trend = (
        completed.groupby("order_year_month", as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "total_revenue"})
        .sort_values("order_year_month")
    )
    path = out_dir / "monthly_revenue.csv"
    trend.to_csv(path, index=False)
    logger.info("Saved monthly_revenue.csv (%d rows)", len(trend))
    return trend


def top_customers(full_data: pd.DataFrame, out_dir: Path, reference_date: pd.Timestamp) -> pd.DataFrame:
    """Task 2 — Top 10 customers by total spend (completed orders) + churn flag."""
    completed = full_data[full_data["status"] == "completed"].copy()

    spend = (
        completed.groupby("customer_id", as_index=False)
        .agg(
            name=("name", "first"),
            region=("region", "first"),
            total_spend=("amount", "sum"),
            last_order_date=("order_date", "max"),
        )
        .sort_values("total_spend", ascending=False)
        .head(10)
    )

    cutoff = reference_date - pd.Timedelta(days=90)
    spend["last_order_date"] = pd.to_datetime(spend["last_order_date"])
    spend["churned"] = spend["last_order_date"] < cutoff

    path = out_dir / "top_customers.csv"
    spend.to_csv(path, index=False)
    logger.info("Saved top_customers.csv (%d rows)", len(spend))
    return spend


def category_performance(full_data: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    """Task 3 — Revenue, avg order value, order count by product category."""
    completed = full_data[full_data["status"] == "completed"].copy()
    perf = (
        completed.groupby("category", as_index=False)
        .agg(
            total_revenue=("amount", "sum"),
            avg_order_value=("amount", "mean"),
            order_count=("order_id", "count"),
        )
        .round(2)
        .sort_values("total_revenue", ascending=False)
    )
    path = out_dir / "category_performance.csv"
    perf.to_csv(path, index=False)
    logger.info("Saved category_performance.csv (%d rows)", len(perf))
    return perf


def regional_analysis(full_data: pd.DataFrame, customers_clean: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    """Task 4 — Per-region: customer count, order count, revenue, avg revenue per customer."""
    completed = full_data[full_data["status"] == "completed"].copy()

    order_stats = (
        completed.groupby("region", as_index=False)
        .agg(
            order_count=("order_id", "count"),
            total_revenue=("amount", "sum"),
        )
    )

    customer_counts = (
        customers_clean.groupby("region", as_index=False)
        .agg(customer_count=("customer_id", "nunique"))
    )

    regional = pd.merge(customer_counts, order_stats, on="region", how="left")
    regional["total_revenue"] = regional["total_revenue"].fillna(0)
    regional["order_count"] = regional["order_count"].fillna(0).astype(int)
    regional["avg_revenue_per_customer"] = (
        regional["total_revenue"] / regional["customer_count"]
    ).round(2)

    path = out_dir / "regional_analysis.csv"
    regional.to_csv(path, index=False)
    logger.info("Saved regional_analysis.csv (%d rows)", len(regional))
    return regional


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Merge and analyze cleaned data.")
    parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    return parser.parse_args()


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    customers_clean = load_csv(args.processed_dir / "customers_clean.csv")
    orders_clean = load_csv(args.processed_dir / "orders_clean.csv")
    products = load_csv(args.raw_dir / "products.csv")

    # Parse dates after loading
    orders_clean["order_date"] = pd.to_datetime(orders_clean["order_date"], errors="coerce")

    full_data = build_full_data(customers_clean, orders_clean, products)

    reference_date = orders_clean["order_date"].max()
    logger.info("Reference date for churn calculation: %s", reference_date.date())

    monthly_revenue(full_data, args.out_dir)
    top_customers(full_data, args.out_dir, reference_date)
    category_performance(full_data, args.out_dir)
    regional_analysis(full_data, customers_clean, args.out_dir)

    print("✅  Analysis complete. Output CSVs saved to:", args.out_dir)


if __name__ == "__main__":
    main()
