import pymongo
import uuid
from datetime import datetime, timezone
import random
import pandas as pd
import time

def generate_and_insert_data(text_file_path, mongo_uri, db_name, collection_name):
    """
    Generates dynamic data and inserts it into a MongoDB collection continuously.
    Stores record_timestamp as a Unix timestamp in milliseconds to align with consumer script.
    """
    start_addresses = []
    try:
        with open(text_file_path, 'r') as f:
            for line in f:
                address = line.strip()
                if address:
                    start_addresses.append(address)

        if not start_addresses:
            print(f"No valid 'start_address' found in the text file: {text_file_path}.")
            return

        client = pymongo.MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        print(f"Connected to MongoDB: {mongo_uri}, Database: {db_name}, Collection: {collection_name}")

        while True:
            address = random.choice(start_addresses)
            user_id = str(uuid.uuid4())
            case_id = str(uuid.uuid4())
            inv_id = str(uuid.uuid4())

            # Set record_timestamp to current UTC time + a small delta in the future
            current_utc_time = datetime.now(timezone.utc)
            delta_seconds = 1 # A small delta to ensure it's slightly in the future
            record_datetime_utc = current_utc_time + pd.Timedelta(seconds=delta_seconds)
            
            # Store record_timestamp as Unix timestamp in MILLISECONDS
            record_timestamp_unix_ms = int(record_datetime_utc.timestamp() * 1000)

            document = {
                "user_id": user_id,
                "case_id": case_id,
                "record_timestamp": record_timestamp_unix_ms, # Renamed from start_date
                "inv_id": inv_id,
                "start_address": address,
                "timestamp": int(datetime.now(timezone.utc).timestamp()), # General document timestamp
                "coin": random.choice(["Bitcoin", "Ethereum", "Tron"]),
                "org_id": str(uuid.uuid4()),
                "org": random.choice(["OrganizationA", "OrganizationB", "OrganizationC"])
            }
            try:
                collection.insert_one(document)
                print(f"Inserted document: inv_id={inv_id}, record_timestamp_ms={record_timestamp_unix_ms}")
            except pymongo.errors.DuplicateKeyError:
                print(f"Duplicate inv_id found: {inv_id}. Skipping insertion.")
            except Exception as e:
                print(f"An error occurred during insertion: {e}")

            time.sleep(random.randint(5, 15))

    except FileNotFoundError:
        print(f"Error: Text file not found at {text_file_path}. Please ensure '{TEXT_FILE}' is in the same folder.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error: Could not connect to MongoDB. Details: {e}")
    except KeyboardInterrupt:
        print("\nData generation stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'client' in locals() and client:
            client.close()
            print("MongoDB connection closed.")

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "try"
COLLECTION_NAME = "crypto_forensics.auto_investigations"
TEXT_FILE = "START_ADDRESS.txt" # Ensure this file exists in the same directory

if __name__ == "__main__":
    generate_and_insert_data(TEXT_FILE, MONGO_URI, DATABASE_NAME, COLLECTION_NAME)
