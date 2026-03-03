"""
clean_data.py — Part 1: Data Cleaning
Cleans customers.csv and orders.csv, outputs cleaned CSVs and a report.
"""

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Config (override via CLI flags)
# ---------------------------------------------------------------------------
DEFAULT_RAW_DIR = Path(__file__).parent / "data" / "raw"
DEFAULT_OUT_DIR = Path(__file__).parent / "data" / "processed"

STATUS_MAP = {
    "done": "completed",
    "complete": "completed",
    "COMPLETED": "completed",
    "finished": "completed",
    "canceled": "cancelled",
    "CANCELLED": "cancelled",
    "cancel": "cancelled",
    "refund": "refunded",
    "REFUNDED": "refunded",
    "PENDING": "pending",
    "Pending": "pending",
    "in progress": "pending",
}
VALID_STATUSES = {"completed", "pending", "cancelled", "refunded"}

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV file with error handling."""
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


def null_counts(df: pd.DataFrame) -> dict:
    """Return per-column null counts as a dict."""
    return df.isnull().sum().to_dict()


def is_valid_email(email) -> bool:
    """Return True if email is non-null and contains '@' and at least one '.'."""
    if pd.isna(email) or not isinstance(email, str) or email.strip() == "":
        return False
    return "@" in email and "." in email.split("@")[-1]


def parse_date_multi(val) -> pd.Timestamp:
    """Try parsing a date value with multiple format strings."""
    if pd.isna(val):
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y"):
        try:
            return pd.to_datetime(val, format=fmt)
        except (ValueError, TypeError):
            pass
    logger.warning("Could not parse date value: %r — replacing with NaT", val)
    return pd.NaT


def normalize_status(val) -> str:
    """Map status variants to controlled vocabulary; leave unknown as-is."""
    if pd.isna(val):
        return "pending"
    val_str = str(val).strip()
    normalized = STATUS_MAP.get(val_str, val_str.lower())
    if normalized not in VALID_STATUSES:
        logger.warning("Unknown order status %r — keeping as-is", val_str)
        return val_str
    return normalized


# ---------------------------------------------------------------------------
# Part 1.1 — customers.csv
# ---------------------------------------------------------------------------

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all cleaning rules to the customers DataFrame."""
    before_rows = len(df)
    before_nulls = null_counts(df)

    # Strip whitespace from string columns
    for col in ("name", "region"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Parse signup_date → datetime
    df["signup_date"] = df["signup_date"].apply(
        lambda v: pd.to_datetime(v, format="%Y-%m-%d", errors="coerce")
        if not pd.isna(v)
        else pd.NaT
    )

    # Remove duplicates: keep the row with the most recent signup_date
    df = df.sort_values("signup_date", ascending=False, na_position="last")
    dup_mask = df.duplicated(subset=["customer_id"], keep="first")
    dups_removed = dup_mask.sum()
    df = df[~dup_mask].copy()
    logger.info("Removed %d duplicate customer rows", dups_removed)

    # Standardize email to lowercase; flag invalid/missing
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["email"] = df["email"].replace({"nan": np.nan, "": np.nan})
    df["is_valid_email"] = df["email"].apply(is_valid_email)

    # Fill missing region
    df["region"] = df["region"].replace({"": np.nan, "nan": np.nan})
    df["region"] = df["region"].fillna("Unknown")

    after_nulls = null_counts(df)
    report = {
        "file": "customers",
        "rows_before": before_rows,
        "rows_after": len(df),
        "duplicates_removed": dups_removed,
        "nulls_before": before_nulls,
        "nulls_after": after_nulls,
    }
    return df.reset_index(drop=True), report


# ---------------------------------------------------------------------------
# Part 1.2 — orders.csv
# ---------------------------------------------------------------------------

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all cleaning rules to the orders DataFrame."""
    before_rows = len(df)
    before_nulls = null_counts(df)

    # Drop unrecoverable rows (both customer_id and order_id are null)
    unrecoverable = df["customer_id"].isna() & df["order_id"].isna()
    df = df[~unrecoverable].copy()
    logger.info("Dropped %d unrecoverable rows", unrecoverable.sum())

    # Parse order_date with multi-format parser
    df["order_date"] = df["order_date"].apply(parse_date_multi)

    # Fill missing amount with per-product median
    median_by_product = df.groupby("product")["amount"].transform("median")
    df["amount"] = df["amount"].fillna(median_by_product)

    # Normalize status
    df["status"] = df["status"].apply(normalize_status)

    # Derived column: order_year_month
    df["order_year_month"] = df["order_date"].dt.to_period("M").astype(str)

    after_nulls = null_counts(df)
    report = {
        "file": "orders",
        "rows_before": before_rows,
        "rows_after": len(df),
        "unrecoverable_dropped": int(unrecoverable.sum()),
        "nulls_before": before_nulls,
        "nulls_after": after_nulls,
    }
    return df.reset_index(drop=True), report


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(report: dict) -> None:
    """Print a human-readable cleaning report to stdout."""
    print("\n" + "=" * 60)
    print(f"  CLEANING REPORT — {report['file'].upper()}")
    print("=" * 60)
    print(f"  Rows before : {report['rows_before']}")
    print(f"  Rows after  : {report['rows_after']}")
    if "duplicates_removed" in report:
        print(f"  Duplicates removed : {report['duplicates_removed']}")
    if "unrecoverable_dropped" in report:
        print(f"  Unrecoverable rows dropped : {report['unrecoverable_dropped']}")
    print("\n  Null counts per column:")
    all_cols = set(report["nulls_before"]) | set(report["nulls_after"])
    for col in sorted(all_cols):
        before = report["nulls_before"].get(col, 0)
        after = report["nulls_after"].get(col, 0)
        print(f"    {col:<25} before={before}  after={after}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Clean raw CSV datasets.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR,
                        help="Directory containing raw CSV files")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                        help="Directory to write cleaned CSVs")
    return parser.parse_args()


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # --- Customers ---
    customers_raw = load_csv(args.raw_dir / "customers.csv")
    customers_clean, cust_report = clean_customers(customers_raw)
    out_path = args.out_dir / "customers_clean.csv"
    customers_clean.to_csv(out_path, index=False)
    logger.info("Saved cleaned customers → %s", out_path)
    print_report(cust_report)

    # --- Orders ---
    orders_raw = load_csv(args.raw_dir / "orders.csv")
    orders_clean, ord_report = clean_orders(orders_raw)
    out_path = args.out_dir / "orders_clean.csv"
    orders_clean.to_csv(out_path, index=False)
    logger.info("Saved cleaned orders → %s", out_path)
    print_report(ord_report)

    print("✅  Data cleaning complete.")


if __name__ == "__main__":
    main()
