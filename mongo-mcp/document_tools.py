# document_tools.py

def register_document_tools(db, mcp):
    # Insert a single document into the specified collection
    @mcp.tool("insertOne")
    def insert_one(data: dict) -> str:
        collection = data.get("collection")
        document = data.get("document")

        if not collection or not document:
            return "'collection' and 'document' are required"

        result = db[collection].insert_one(document)
        return f"Inserted document with ID: {result.inserted_id}"

    # Update a single document based on a filter
    @mcp.tool("updateOne")
    def update_one(data: dict) -> str:
        collection = data.get("collection")
        filter_query = data.get("filter")
        update_data = data.get("update")

        if not collection or not filter_query or not update_data:
            return "'collection', 'filter', and 'update' are required"

        result = db[collection].update_one(filter_query, {"$set": update_data})
        return f"Matched {result.matched_count}, Modified {result.modified_count}"

    #  Delete a single document matching the filter
    @mcp.tool("deleteOne")
    def delete_one(data: dict) -> str:
        collection = data.get("collection")
        filter_query = data.get("filter")

        if not collection or not filter_query:
            return "'collection' and 'filter' are required"

        result = db[collection].delete_one(filter_query)
        return f" Deleted {result.deleted_count} document"
