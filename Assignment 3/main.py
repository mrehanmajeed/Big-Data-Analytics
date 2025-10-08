from pymongo import MongoClient
from bson.objectid import ObjectId
import os

# Load MongoDB credentials from environment variables
mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASS")
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")


mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?authSource=admin"




# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client["mydatabase"]
collection = db["items"]

def create_item(name, description):
    result = collection.insert_one({"name": name, "description": description})
    print(f"✅ Item created with ID: {result.inserted_id}")

def read_items():
    items = collection.find()
    for item in items:
        print(f"📄 {item['_id']}: {item['name']} - {item['description']}")

def update_item(item_id, name, description):
    result = collection.update_one(
        {"_id": ObjectId(item_id)},
        {"$set": {"name": name, "description": description}}
    )
    if result.modified_count:
        print("✅ Item updated.")
    else:
        print("⚠️ Item not found.")

def delete_item(item_id):
    result = collection.delete_one({"_id": ObjectId(item_id)})
    if result.deleted_count:
        print("🗑️ Item deleted.")
    else:
        print("⚠️ Item not found.")

if __name__ == "__main__":
    while True:
        print("\n🔧 Choose operation: create / read / update / delete / exit")
        op = input("Operation: ").strip().lower()

        if op == "create":
            name = input("📝 Name: ")
            desc = input("📝 Description: ")
            create_item(name, desc)

        elif op == "read":
            read_items()

        elif op == "update":
            item_id = input("🔄 Item ID to update: ")
            name = input("📝 New Name: ")
            desc = input("📝 New Description: ")
            update_item(item_id, name, desc)

        elif op == "delete":
            item_id = input("🗑️ Item ID to delete: ")
            delete_item(item_id)

        elif op == "exit":
            print("👋 Exiting.")
            break

        else:
            print("❌ Invalid operation.")


