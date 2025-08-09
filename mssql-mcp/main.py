from mcp.server.fastmcp import FastMCP
import pandas as pd
import logging
import re
from common import mcp, conn, get_table_schema, get_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sql_mcp_server")

# --- SQLValidator ---
class SQLValidator:
    @staticmethod
    def is_read_only_query(query: str) -> bool:
        query = query.strip().upper()

        if not (query.startswith("SELECT") or query.startswith("WITH")):
            return False

        forbidden = [
            "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
            "TRUNCATE", "MERGE", "REPLACE", "EXEC", "EXECUTE",
            "GRANT", "REVOKE", ";"
        ]

        if any(keyword in query for keyword in forbidden):
            return False

        if re.search(r';\s*\w+', query):
            return False

        return True

# TOOL 1: List all base tables
@mcp.tool("list_tables", description="List all base tables in the database")
def list_tables(data: dict) -> list:
    return get_tables()

# TOOL 2: Get schema of a table
@mcp.tool("get_table_schema", description="Get column names and types for a specific table")
def get_table_schema_tool(args: dict) -> list:
    table_name = args.get("table_name")
    if not table_name:
        return [{"error": "Missing 'table_name'"}]
    return get_table_schema(table_name)

# TOOL 3: Preview first N rows of a table
@mcp.tool("preview_table", description="Preview first N rows of a specific table")
def preview_table(args: dict) -> list:
    table_name = args.get("table_name")
    try:
        limit = int(args.get("limit", 10))
    except ValueError:
        limit = 10

    try:
        query = f"SELECT TOP {limit} * FROM [{table_name}]"
        if not SQLValidator.is_read_only_query(query):
            return [{"error": "Query is not read-only"}]

        df = pd.read_sql(query, conn)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error previewing table {table_name}: {str(e)}")
        return [{"error": str(e)}]

# TOOL 4: Run custom SELECT query
@mcp.tool("run_query", description="Execute a custom read-only SQL SELECT query")
def run_query(args: dict) -> list:
    query = args.get("query", "")
    if not SQLValidator.is_read_only_query(query):
        return [{"error": "Only read-only SELECT queries are allowed"}]

    try:
        df = pd.read_sql(query, conn)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error running query: {str(e)}")
        return [{"error": str(e)}]

# TOOL 5: Database metadata
@mcp.tool("database_metadata", description="Fetch metadata for all tables and their columns")
def database_metadata(data: dict) -> dict:
    metadata = {}
    for table in get_tables():
        metadata[table] = [col["name"] for col in get_table_schema(table)]
    return metadata


# TOOL 6: Insert Row 
@mcp.tool("insert_row", description="Insert a new row into any table")
def insert_row(args: dict) -> dict:
    from common import conn

    table = args.get("table")
    data = args.get("data")

    if not table or not isinstance(data, dict):
        return {"error": "Missing or invalid 'table' or 'data'"}

    try:
        keys = ', '.join(f"[{k}]" for k in data.keys())
        placeholders = ', '.join(['?'] * len(data))
        values = list(data.values())

        query = f"INSERT INTO [{table}] ({keys}) VALUES ({placeholders})"
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()

        return {"success": True, "inserted_into": table, "data": data}
    except Exception as e:
        logger.error(f"Insert failed: {str(e)}")
        return {"error": str(e)}



# TOOL 7: Update row 
@mcp.tool("update_row", description="Update a row in a table by primary key")
def update_row(args: dict) -> dict:
    from common import conn

    table = args.get("table")
    key_column = args.get("key_column")  # e.g., 'user_id'
    key_value = args.get("key_value")    # e.g., '12345'
    updates = args.get("updates")        # e.g., { "country": "France" }

    if not table or not key_column or key_value is None or not isinstance(updates, dict):
        return {"error": "Missing or invalid 'table', 'key_column', 'key_value', or 'updates'"}

    try:
        set_clause = ', '.join(f"[{k}] = ?" for k in updates.keys())
        values = list(updates.values()) + [key_value]

        query = f"UPDATE [{table}] SET {set_clause} WHERE [{key_column}] = ?"

        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()

        return {"success": True, "updated_table": table, "updated_fields": updates}
    except Exception as e:
        logger.error(f"Update failed: {str(e)}")
        return {"error": str(e)}
    


# TOOL 8 : Delete row
@mcp.tool("delete_row", description="Delete a row from a table by primary key")
def delete_row(args: dict) -> dict:
    from common import conn

    table = args.get("table")
    key_column = args.get("key_column")
    key_value = args.get("key_value")

    if not table or not key_column or key_value is None:
        return {"error": "Missing or invalid 'table', 'key_column', or 'key_value'"}

    try:
        query = f"DELETE FROM [{table}] WHERE [{key_column}] = ?"
        cursor = conn.cursor()
        cursor.execute(query, [key_value])
        conn.commit()

        return {
            "success": True,
            "deleted_from": table,
            "where": {key_column: key_value}
        }
    except Exception as e:
        logger.error(f"Delete failed: {str(e)}")
        return {"error": str(e)}

# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport="stdio")

