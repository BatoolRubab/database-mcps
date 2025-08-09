import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import json

# Load .env variables
load_dotenv()

# Establish connection
conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=os.getenv("PG_PORT", "5432"),
    dbname=os.getenv("PG_DB", "mcp_postgres"),
    user=os.getenv("PG_USER", "mcp_user"),
    password=os.getenv("PG_PASSWORD", "")
)
cursor = conn.cursor()

# Load CSV
df = pd.read_csv("data.csv", encoding="ISO-8859-1")

# Clean column names
df.columns = df.columns.str.strip().str.lower()

# Drop rows missing customer ID
df = df.dropna(subset=["customerid"])
df["customerid"] = df["customerid"].astype(int)

# Insert Products
print("Inserting products...")
products = df[["stockcode", "description", "unitprice"]].drop_duplicates()
products = products.rename(columns={"stockcode": "code", "description": "name", "unitprice": "price"})

for _, row in products.iterrows():
    try:
        cursor.execute(
            "INSERT INTO products (code, name, price) VALUES (%s, %s, %s) ON CONFLICT (code) DO NOTHING",
            (row["code"], row["name"], row["price"])
        )
    except Exception as e:
        print(f"Product insert error: {e}")
conn.commit()

# Insert Users
print(" Inserting users...")
users = df[["customerid", "country"]].drop_duplicates()
users = users.rename(columns={"customerid": "user_id"})

for _, row in users.iterrows():
    try:
        cursor.execute(
            "INSERT INTO users (user_id, country) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING",
            (row["user_id"], row["country"])
        )
    except Exception as e:
        print(f"User insert error: {e}")
conn.commit()

# Insert Orders
print("Inserting orders")
for invoice, group in df.groupby("invoiceno"):
    try:
        items = group[["stockcode", "quantity"]].to_dict(orient="records")
        total = float((group["quantity"] * group["unitprice"]).sum())  # 👈 Fix here
        customer_id = int(group["customerid"].iloc[0])
        items_json = json.dumps(items)

        cursor.execute(
            "INSERT INTO orders (invoice, customer_id, items, total) VALUES (%s, %s, %s, %s) ON CONFLICT (invoice) DO NOTHING",
            (invoice, customer_id, items_json, total)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Order insert error for invoice {invoice}: {e}")
        break 


# Finish 
cursor.close()
conn.close()
print(" Data loaded into PostgreSQL successfully.")

