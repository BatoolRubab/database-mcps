# mongo_db_mcp.py

from mcp.server.fastmcp import FastMCP
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Import tool registration functions from other files
from index_tools import register_index_tools
from query_tools import register_query_tools
from document_tools import register_document_tools

# Load environment variables from .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Initialize MCP server
mcp = FastMCP("MongoDB MCP")

# Register tools from separate files
register_index_tools(db, mcp)
register_query_tools(db, mcp)
register_document_tools(db, mcp)

# Run the MCP server
if __name__ == "__main__":
    mcp.run(transport="stdio")





