import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Load the data.csv file
df = pd.read_csv("data.csv", encoding="ISO-8859-1")

# Drop rows with no CustomerID
df = df.dropna(subset=["CustomerID"])

# Products collection
products = df[["StockCode", "Description", "UnitPrice"]].drop_duplicates()
products = products.rename(columns={"StockCode": "code", "Description": "name", "UnitPrice": "price"})
db.products.drop()
db.products.insert_many(products.to_dict(orient="records"))

# Users collection
users = df[["CustomerID", "Country"]].drop_duplicates()
users = users.rename(columns={"CustomerID": "user_id"})
db.users.drop()
db.users.insert_many(users.to_dict(orient="records"))

#  Orders collection
orders = []
for invoice, group in df.groupby("InvoiceNo"):
    items = group[["StockCode", "Quantity"]].to_dict(orient="records")
    total = (group["Quantity"] * group["UnitPrice"]).sum()
    customer_id = group["CustomerID"].iloc[0]
    orders.append({
        "invoice": invoice,
        "customer_id": customer_id,
        "items": items,
        "total": total
    })

db.orders.drop()
db.orders.insert_many(orders)

print("Data loaded into MongoDB: products, users, orders.")

