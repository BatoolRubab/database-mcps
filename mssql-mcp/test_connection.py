import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=DESKTOP-FB2GLTI;"
    "DATABASE=mcp_demo;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)


try:
    conn = pyodbc.connect(conn_str)
    print("✅ Connection successful!")
except Exception as e:
    print("❌ Connection failed:")
    print(e)
