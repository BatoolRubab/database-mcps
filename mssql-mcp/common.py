import os
import pyodbc
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
import logging

# Load environment variables
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "mcp_demo")
SQL_USERNAME = os.getenv("SQL_USERNAME", "mcp_user")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "your_password_here")

#  Connection string
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=DESKTOP-FB2GLTI;"
    "DATABASE=mcp_demo;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

def get_connection():
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        raise RuntimeError(f"Failed to connect to SQL Server: {e}")

def get_cursor():
    return get_connection().cursor()

try:
    conn = get_connection()
    cursor = conn.cursor()
except Exception as e:
    raise RuntimeError(f"Could not establish default connection: {e}")

# Initialize FastMCP 
mcp = FastMCP("MSSQL MCP Server")

#  Table cache 
tables_cache = []
schema_cache = {}

#  Get all table names
def get_tables():
    with get_cursor() as cur:
        cur.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        return [row[0] for row in cur.fetchall()]

#  Get table schema
def get_table_schema(table_name: str):
    with get_cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
        """, (table_name,))
        return [{"name": row[0], "type": row[1]} for row in cur.fetchall()]

#  Refresh caches
def refresh_cache():
    global tables_cache, schema_cache
    tables_cache = get_tables()
    schema_cache = {}
    for table in tables_cache:
        schema_cache[table] = get_table_schema(table)




