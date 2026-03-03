"""
tests/test_cleaning.py — Unit tests for data-cleaning functions (Bonus)
Run with: pytest tests/ -v
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from clean_data import (
    is_valid_email,
    normalize_status,
    parse_date_multi,
    clean_customers,
    clean_orders,
)


# ---------------------------------------------------------------------------
# Test: is_valid_email
# ---------------------------------------------------------------------------

class TestIsValidEmail:
    def test_valid_email(self):
        assert is_valid_email("user@example.com") is True

    def test_missing_at(self):
        assert is_valid_email("userwithnoat.com") is False

    def test_missing_dot_in_domain(self):
        assert is_valid_email("user@nodot") is False

    def test_empty_string(self):
        assert is_valid_email("") is False

    def test_none_value(self):
        assert is_valid_email(None) is False

    def test_nan_value(self):
        assert is_valid_email(float("nan")) is False

    def test_uppercase_valid(self):
        # is_valid_email is case-insensitive structurally
        assert is_valid_email("USER@EXAMPLE.COM") is True


# ---------------------------------------------------------------------------
# Test: parse_date_multi
# ---------------------------------------------------------------------------

class TestParseDateMulti:
    def test_iso_format(self):
        result = parse_date_multi("2024-05-15")
        assert result == pd.Timestamp("2024-05-15")

    def test_dd_mm_yyyy(self):
        result = parse_date_multi("15/05/2024")
        assert result == pd.Timestamp("2024-05-15")

    def test_mm_dd_yyyy(self):
        result = parse_date_multi("05-15-2024")
        assert result == pd.Timestamp("2024-05-15")

    def test_unparseable_returns_nat(self):
        result = parse_date_multi("not-a-date")
        assert pd.isna(result)

    def test_none_returns_nat(self):
        result = parse_date_multi(None)
        assert pd.isna(result)


# ---------------------------------------------------------------------------
# Test: normalize_status
# ---------------------------------------------------------------------------

class TestNormalizeStatus:
    def test_done_to_completed(self):
        assert normalize_status("done") == "completed"

    def test_canceled_to_cancelled(self):
        assert normalize_status("canceled") == "cancelled"

    def test_already_valid(self):
        assert normalize_status("pending") == "pending"

    def test_uppercase_completed(self):
        assert normalize_status("COMPLETED") == "completed"

    def test_null_defaults_to_pending(self):
        assert normalize_status(None) == "pending"

    def test_nan_defaults_to_pending(self):
        assert normalize_status(float("nan")) == "pending"


# ---------------------------------------------------------------------------
# Test: clean_customers integration
# ---------------------------------------------------------------------------

class TestCleanCustomers:
    def _sample_df(self):
        return pd.DataFrame({
            "customer_id": ["C001", "C002", "C001", "C003"],
            "name":        ["  Alice  ", "Bob", "Alice", "Charlie"],
            "email":       ["alice@example.com", "bademail", "alice@example.com", None],
            "region":      ["North", "South", None, ""],
            "signup_date": ["2023-01-10", "2023-02-01", "2022-12-01", "2023-03-05"],
        })

    def test_duplicate_removal_keeps_latest(self):
        df, _ = clean_customers(self._sample_df())
        assert df[df["customer_id"] == "C001"].shape[0] == 1
        assert df[df["customer_id"] == "C001"].iloc[0]["signup_date"] == pd.Timestamp("2023-01-10")

    def test_email_lowercased(self):
        df = pd.DataFrame({
            "customer_id": ["C001"],
            "name":        ["Alice"],
            "email":       ["ALICE@EXAMPLE.COM"],
            "region":      ["North"],
            "signup_date": ["2023-01-10"],
        })
        result, _ = clean_customers(df)
        assert result.iloc[0]["email"] == "alice@example.com"

    def test_missing_region_filled(self):
        df, _ = clean_customers(self._sample_df())
        assert (df["region"] == "Unknown").any() or df["region"].notna().all()

    def test_is_valid_email_column_created(self):
        df, _ = clean_customers(self._sample_df())
        assert "is_valid_email" in df.columns

    def test_whitespace_stripped_from_name(self):
        df, _ = clean_customers(self._sample_df())
        assert not df["name"].str.startswith(" ").any()
        assert not df["name"].str.endswith(" ").any()


# ---------------------------------------------------------------------------
# Test: clean_orders integration
# ---------------------------------------------------------------------------

class TestCleanOrders:
    def _sample_df(self):
        return pd.DataFrame({
            "order_id":   ["O001", "O002", None, "O003"],
            "customer_id":["C001", None,   None, "C002"],
            "product":    ["Laptop", "Mouse", "Keyboard", "Laptop"],
            "amount":     [50000.0, None, 800.0, 75000.0],
            "order_date": ["2023-05-10", "15/06/2023", "07-20-2023", "2023-08-01"],
            "status":     ["done", "Pending", "canceled", "completed"],
        })

    def test_unrecoverable_rows_dropped(self):
        df, report = clean_orders(self._sample_df())
        assert report["unrecoverable_dropped"] == 1
        assert len(df) == 3

    def test_status_normalized(self):
        df, _ = clean_orders(self._sample_df())
        assert set(df["status"].unique()).issubset({"completed", "pending", "cancelled", "refunded"})

    def test_order_year_month_added(self):
        df, _ = clean_orders(self._sample_df())
        assert "order_year_month" in df.columns

    def test_amount_nulls_filled(self):
        df, _ = clean_orders(self._sample_df())
        assert df["amount"].isna().sum() == 0

    def test_dates_parsed(self):
        df, _ = clean_orders(self._sample_df())
        assert pd.api.types.is_datetime64_any_dtype(df["order_date"])
