import os
import psycopg2
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "mcp_postgres")
PG_USER = os.getenv("PG_USER", "mcp_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()
except Exception as e:
    raise RuntimeError(f"Could not connect to PostgreSQL: {e}")

# MCP instance
mcp = FastMCP("PostgreSQL MCP Server")

# Get list of tables
def get_tables():
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    return [row[0] for row in cursor.fetchall()]

# Get schema of a table
def get_table_schema(table_name: str):
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
    """, (table_name,))
    return [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]

# get metadata of a table 
def get_database_metadata():
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    metadata = []

    for table in tables:
        cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable, 
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """, (table,))
        columns = cursor.fetchall()

        metadata.append({
            "table_name": table,
            "columns": [
                {
                    "name": col[0],
                    "type": col[1],
                    "nullable": col[2],
                    "default": col[3]
                }
                for col in columns
            ]
        })

    return metadata



