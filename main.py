"""Module for managing MongoDB Atlas connections and basic operations."""

import os
from typing import Optional
from pprint import pprint

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, PyMongoError  # Updated import
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class MongoDBConnection:
    """Class to manage a singleton MongoDB connection."""

    _instance: Optional[MongoClient] = None
    URI: str = os.getenv("MONGO_URI", "")

    @staticmethod
    def get_client() -> MongoClient:
        """Get or create a MongoDB client connection.

        Returns:
            MongoClient: The MongoDB client instance.

        Raises:
            ConnectionFailure: If connection to MongoDB fails.
        """
        if MongoDBConnection._instance is None:
            try:
                MongoDBConnection._instance = MongoClient(
                    MongoDBConnection.URI,
                    server_api=ServerApi("1"),
                )
                MongoDBConnection._instance.admin.command("ping")
                print("Pinged your deployment. You successfully connected to MongoDB!")
            except ConnectionFailure as error:
                print(f"Failed to connect to MongoDB: {error}")
                raise
        return MongoDBConnection._instance

    @staticmethod
    def get_database(db_name: str) -> Database:
        """Retrieve a MongoDB database instance.

        Args:
            db_name (str): The name of the database to access.

        Returns:
            Database: The MongoDB database instance.
        """
        client = MongoDBConnection.get_client()
        return client[db_name]


def insert_sample_data(collection: Collection) -> None:
    """Insert sample data into the specified collection.

    Args:
        collection (Collection): The MongoDB collection to insert data into.

    Raises:
        PyMongoError: If insertion fails due to a MongoDB error.
    """
    sample_data = [
        {
            "name": "Alice",
            "age": 25,
            "details": {"city": "Boston", "hobbies": ["reading", "gaming"]},
        },
        {"product": "Phone", "price": 699.99, "in_stock": True},
    ]
    try:
        result = collection.insert_many(sample_data)
        print(f"Inserted documents with IDs: {result.inserted_ids}")
    except PyMongoError as error:
        print(f"Error inserting data: {error}")


def query_all_documents(collection: Collection) -> None:
    """Query and display all documents in the collection.

    Args:
        collection (Collection): The MongoDB collection to query.

    Raises:
        PyMongoError: If querying fails due to a MongoDB error.
    """
    print("\nAll documents in the collection:")
    try:
        for document in collection.find():
            pprint(document)
    except PyMongoError as error:
        print(f"Error querying data: {error}")

def main() -> None:
    """Main function to demonstrate MongoDB connection and operations."""
    # Get database and collection
    database = MongoDBConnection.get_database("st-db")
    collection = database["st-data-engineer-db"]

    # Perform operations
    insert_sample_data(collection)
    query_all_documents(collection)

if __name__ == "__main__":
    main()
