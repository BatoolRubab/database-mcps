import pandas as pd
import logging
from common import get_database_metadata, mcp, conn, get_tables, get_table_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("postgres_mcp")


#TOOL 1 : list tables 
@mcp.tool("list_tables", description="List all tables in the database")
def list_tables(args: dict) -> list:
    return get_tables()


# TOOL 2 : Table schema 
@mcp.tool("get_table_schema", description="Get column names and types for a table")
def schema_tool(args: dict) -> list:
    table = args.get("table_name")
    if not table:
        return [{"error": "Missing table_name"}]
    return get_table_schema(table)

# TOOL 3: Preview Table 
@mcp.tool("preview_table", description="Preview N rows from a table")
def preview_table(args: dict) -> str:
    table = args.get("table_name")
    limit = args.get("limit", 10)
    try:
        df = pd.read_sql(f'SELECT * FROM "{table}" LIMIT %s', conn, params=[limit])
        return df.to_string(index=False)
    except Exception as e:
        logger.error(f"Error previewing table: {e}")
        return str(e)

# TOOL 4: read only queries 
@mcp.tool("run_query", description="Run custom SELECT query")
def run_query(args: dict) -> str:
    query = args.get("query", "")
    if not query.lower().strip().startswith("select"):
        return "Only SELECT queries are allowed."
    try:
        df = pd.read_sql(query, conn)
        return df.to_string(index=False)
    except Exception as e:
        logger.error(f"Error running query: {e}")
        return str(e)

# INSERT ROW 
@mcp.tool("insert_row", description="Insert a row into any table")
def insert_row(args: dict) -> dict:
    table = args.get("table_name")
    row_data = args.get("row_data")

    if not table or not row_data:
        return {"error": "Missing 'table_name' or 'row_data'"}

    try:
        columns = list(row_data.keys())
        values = list(row_data.values())
        placeholders = ", ".join(["%s"] * len(values))
        column_names = ", ".join([f'"{col}"' for col in columns])

        query = f'INSERT INTO "{table}" ({column_names}) VALUES ({placeholders})'
        
        with conn.cursor() as cur:
            cur.execute(query, values)
            conn.commit()

        return {"success": f"Inserted into {table}"}
    except Exception as e:
        conn.rollback()
        logger.error(f"Insert failed: {e}")
        return {"error": str(e)}


# UPDATE ROW 
@mcp.tool("update_row", description="Update rows in a table based on condition")
def update_row(args: dict) -> dict:
    table = args.get("table_name")
    updates = args.get("update_data")
    condition = args.get("where")

    if not table or not updates or not condition:
        return {"error": "Missing 'table_name', 'update_data', or 'where'"}

    try:
        set_clause = ", ".join([f'"{k}" = %s' for k in updates.keys()])
        values = list(updates.values())
        query = f'UPDATE "{table}" SET {set_clause} WHERE {condition}'

        with conn.cursor() as cur:
            cur.execute(query, values)
            conn.commit()

        return {"success": f"Updated rows in {table}"}
    except Exception as e:
        conn.rollback()
        logger.error(f"Update failed: {e}")
        return {"error": str(e)}
    

# DELETE ROW 
@mcp.tool("delete_row", description="Delete rows from a table based on condition")
def delete_row(args: dict) -> dict:
    table = args.get("table_name")
    condition = args.get("where")

    if not table or not condition:
        return {"error": "Missing 'table_name' or 'where'"}

    try:
        query = f'DELETE FROM "{table}" WHERE {condition}'
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

        return {"success": f"Deleted from {table}"}
    except Exception as e:
        conn.rollback()
        logger.error(f"Delete failed: {e}")
        return {"error": str(e)}
    
# meta data tool
@mcp.tool("get_database_metadata", description="Get metadata (schema) for all tables in the database")
def get_metadata_tool(data: dict) -> list:
    try:
        return get_database_metadata()
    except Exception as e:
        logger.error(f"Metadata tool error: {e}")
        return [{"error": str(e)}]

        return [{"error": str(e)}]



# Run server
if __name__ == "__main__":
    mcp.run(transport="stdio")


