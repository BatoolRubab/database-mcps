import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
SERVER = os.getenv("SQL_SERVER", "localhost")
DATABASE = os.getenv("SQL_DATABASE", "mcp_demo")
USERNAME = os.getenv("SQL_USERNAME", "mcp_user")
PASSWORD = os.getenv("SQL_PASSWORD", "your_password_here") 

# Load CSV data
df = pd.read_csv("data.csv", encoding="ISO-8859-1")

# Clean column names (strip spaces, lowercase)
df.columns = df.columns.str.strip().str.lower()

# Drop rows without customer ID
df = df.dropna(subset=["customerid"])

# SQL Server connection string
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "TrustServerCertificate=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Insert products
print("Inserting products...")
products = df[["stockcode", "description", "unitprice"]].drop_duplicates()
products = products.rename(columns={"stockcode": "code", "description": "name", "unitprice": "price"})

for _, row in products.iterrows():
    cursor.execute("INSERT INTO products (code, name, price) VALUES (?, ?, ?)", row["code"], row["name"], row["price"])

# Insert users
print("Inserting users...")
users = df[["customerid", "country"]].drop_duplicates()
users = users.rename(columns={"customerid": "user_id"})

for _, row in users.iterrows():
    cursor.execute("INSERT INTO users (user_id, country) VALUES (?, ?)", row["user_id"], row["country"])

# Insert orders
print("Inserting orders...")
orders = []
for invoice, group in df.groupby("invoiceno"):
    items = group[["stockcode", "quantity"]].to_dict(orient="records")
    total = (group["quantity"] * group["unitprice"]).sum()
    customer_id = group["customerid"].iloc[0]
    orders.append((invoice, customer_id, str(items), total))

for invoice, customer_id, items, total in orders:
    cursor.execute("INSERT INTO orders (invoice, customer_id, items, total) VALUES (?, ?, ?, ?)", invoice, customer_id, items, total)

conn.commit()
conn.close()
print("Data inserted successfully into SQL Server.")

