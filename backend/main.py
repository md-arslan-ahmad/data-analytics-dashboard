"""
backend/main.py — FastAPI REST API
Serves analysis CSV outputs as JSON endpoints.
"""

import pathlib

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Data Analytics Dashboard API", version="1.0.0")

# CORS — allow frontend on any local port
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DATA = pathlib.Path(__file__).parent.parent / "data" / "processed"


def read_csv_or_404(filename: str) -> pd.DataFrame:
    """Load a processed CSV; raise HTTP 404 if not found."""
    path = DATA / filename
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Data file '{filename}' not found. Run analyze.py first.",
        )
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=404, detail=f"Data file '{filename}' is empty.")
    return df


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/api/revenue")
def get_revenue():
    """Monthly revenue trend (completed orders)."""
    df = read_csv_or_404("monthly_revenue.csv")
    return df.to_dict(orient="records")


@app.get("/api/top-customers")
def get_top_customers():
    """Top 10 customers by total spend with churn flag."""
    df = read_csv_or_404("top_customers.csv")
    # Ensure boolean serialisation is correct
    if "churned" in df.columns:
        df["churned"] = df["churned"].astype(bool)
    return df.to_dict(orient="records")


@app.get("/api/categories")
def get_categories():
    """Category performance: revenue, avg order value, order count."""
    df = read_csv_or_404("category_performance.csv")
    return df.to_dict(orient="records")


@app.get("/api/regions")
def get_regions():
    """Regional analysis: customer count, orders, revenue."""
    df = read_csv_or_404("regional_analysis.csv")
    return df.to_dict(orient="records")
