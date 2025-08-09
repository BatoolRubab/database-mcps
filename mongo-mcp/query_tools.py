def register_query_tools(db, mcp):
    @mcp.tool("find")
    def find_documents(data: dict = {}) -> list:
        """
        Finds documents in MongoDB with optional prompt-based collection inference.
        Supports optional filtering, projection, and limiting.
        """
        print("DEBUG - find_documents called with:", data)

        collection = data.get("collection")
        prompt = str(data.get("prompt", "")).lower()

        if not collection:
            if "user" in prompt:
                collection = "users"
            elif "order" in prompt:
                collection = "orders"
            elif "product" in prompt:
                collection = "products"
            else:
                return " 'collection' is missing. Please specify it or include it in the prompt."

        filter_query = data.get("filter", {})
        projection = data.get("projection", {"_id": 0})
        limit = data.get("limit")

        try:
            cursor = db[collection].find(filter_query, projection)
            if limit:
                cursor = cursor.limit(int(limit))  # only apply limit if given
            documents = list(cursor)
            print(f"DEBUG - Returning {len(documents)} documents from '{collection}'")
            return documents
        except Exception as e:
            return f" Error during find: {str(e)}"


    #  Lists all collection names
    @mcp.tool("listCollections")
    def list_collections(data: dict = {}) -> list:
        collections = db.list_collection_names()
        print(f"DEBUG - listCollections found: {collections}")
        return collections




