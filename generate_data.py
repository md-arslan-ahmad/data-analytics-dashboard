"""Script to generate realistic sample CSV data for the assignment."""
import pandas as pd
import numpy as np
import random
from pathlib import Path

random.seed(42)
np.random.seed(42)

RAW = Path(__file__).parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

# --- customers.csv ---
regions = ["North", "South", "East", "West", "Unknown"]
names = [
    "Aarav Sharma", "Priya Singh", "Rahul Gupta", "Ananya Patel", "Vikram Joshi",
    "Sneha Reddy", "Arjun Nair", "Kavya Menon", "Rohan Verma", "Divya Iyer",
    "Amit Kumar", "Pooja Desai", "Suresh Pillai", "Neha Chopra", "Karan Malhotra",
    "Riya Shah", "Manish Tiwari", "Shreya Bose", "Aditya Pandey", "Sunita Rao",
]
emails = [
    "aarav@example.com", "priya@example.com", "rahul@example.com", "ananya@example.com",
    "vikram@example.com", "sneha@example.com", "arjun@example.com", "kavya@example.com",
    "rohan@example.com", "divya@example.com", "amit@example.com", "pooja@example.com",
    "suresh@example.com", "neha@example.com", "karan@example.com", "riya@example.com",
    "manish@example.com", "shreya@example.com", "aditya@example.com", "sunita@example.com",
]
# inject some bad emails
emails[3] = "ananya-no-at-sign"
emails[7] = "kavya@nodot"
emails[12] = ""  # missing

customers_data = []
for i in range(20):
    customers_data.append({
        "customer_id": f"C{i+1:03d}",
        "name": f"  {names[i]}  " if i % 5 == 0 else names[i],
        "email": emails[i],
        "region": random.choice(regions) if i != 15 else "",
        "signup_date": pd.Timestamp("2022-01-01") + pd.Timedelta(days=random.randint(0, 700)),
    })
# Add duplicates
customers_data.append({
    "customer_id": "C001",
    "name": "Aarav Sharma",
    "email": "aarav@example.com",
    "region": "North",
    "signup_date": pd.Timestamp("2021-06-01"),
})
customers_data.append({
    "customer_id": "C005",
    "name": "Vikram Joshi",
    "email": "VIKRAM@Example.COM",
    "region": "West",
    "signup_date": pd.Timestamp("2022-09-15"),
})

pd.DataFrame(customers_data).to_csv(RAW / "customers.csv", index=False)
print("customers.csv generated")

# --- products.csv ---
products = [
    ("P001", "Laptop", "Electronics", 75000),
    ("P002", "Mouse", "Electronics", 800),
    ("P003", "Keyboard", "Electronics", 2500),
    ("P004", "Desk Chair", "Furniture", 12000),
    ("P005", "Monitor", "Electronics", 18000),
    ("P006", "Notebook", "Stationery", 150),
    ("P007", "Pen Set", "Stationery", 250),
    ("P008", "Standing Desk", "Furniture", 25000),
    ("P009", "Headphones", "Electronics", 5000),
    ("P010", "Webcam", "Electronics", 3500),
    ("P011", "Desk Lamp", "Furniture", 1800),
    ("P012", "USB Hub", "Electronics", 1200),
]
pd.DataFrame(products, columns=["product_id", "product_name", "category", "unit_price"]).to_csv(
    RAW / "products.csv", index=False
)
print("products.csv generated")

# --- orders.csv ---
product_names = [p[1] for p in products]
statuses = ["completed", "pending", "cancelled", "refunded", "done", "canceled", "COMPLETED", "Pending"]
date_formats = [
    lambda d: d.strftime("%Y-%m-%d"),
    lambda d: d.strftime("%d/%m/%Y"),
    lambda d: d.strftime("%m-%d-%Y"),
]
customer_ids = [f"C{i+1:03d}" for i in range(20)]

orders_data = []
for i in range(120):
    cid = random.choice(customer_ids)
    prod = random.choice(product_names)
    amt = round(random.uniform(500, 80000), 2)
    odate = pd.Timestamp("2023-01-01") + pd.Timedelta(days=random.randint(0, 540))
    fmt = random.choice(date_formats)
    status = random.choice(statuses)
    # inject nulls
    if i % 20 == 0:
        amt = None
    if i % 25 == 0:
        cid = None
    orders_data.append({
        "order_id": f"O{i+1:04d}",
        "customer_id": cid,
        "product": prod,
        "amount": amt,
        "order_date": fmt(odate),
        "status": status,
    })
# one fully unrecoverable row
orders_data.append({"order_id": None, "customer_id": None, "product": "Laptop", "amount": 1000, "order_date": "2023-06-01", "status": "completed"})

pd.DataFrame(orders_data).to_csv(RAW / "orders.csv", index=False)
print("orders.csv generated")
