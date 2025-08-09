from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# Assume db and mcp are passed in or shared (we'll use this from main file)
def register_index_tools(db, mcp):
    @mcp.tool("createIndex")
    def create_index(data: dict) -> str:
        collection = data.get("collection")
        field = data.get("field")
        unique = data.get("unique", False)

        if not collection or not field:
            return "'collection' and 'field' are required"

        index_name = db[collection].create_index([(field, 1)], unique=unique)
        return f" Created index: {index_name}"

    @mcp.tool("dropIndex")
    def drop_index(data: dict) -> str:
        collection = data.get("collection")
        index = data.get("index")

        if not collection or not index:
            return " 'collection' and 'index' are required"

        db[collection].drop_index(index)
        return f" Dropped index: {index}"

    @mcp.tool("indexes")
    def list_indexes(data: dict) -> list:
        collection = data.get("collection")

        if not collection:
            return ["'collection' field is required"]

        return list(db[collection].index_information().items())
