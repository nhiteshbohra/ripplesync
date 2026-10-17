import asyncio
import logging
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timezone
import sys
from typing import Dict, Any, List, Optional
import time

# Configuration
MONGO_URI = "mongodb://localhost:27017" # PLEASE ENSURE THIS IS CORRECT
DB_NAME = "try"
TX_COLLECTION = "transactions"
FORMATTED_TRANSACTIONS_COLLECTION = "formatted_transactions"
DONE_INVESTIGATIONS_COLLECTION = "done_investigations" # NEW: Collection to track completed investigations

# Polling Interval (in seconds)
POLLING_INTERVAL_SECONDS = 10 # Runs every 10 seconds

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- MongoDB Utility Functions ---
def get_mongo_collection(collection_name: str):
    """
    Connects to MongoDB and returns the specified collection and the client.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[collection_name], client

def setup_mongodb_indexes():
    """
    Ensures necessary indexes are created for all relevant collections.
    """
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        # Indexes for FORMATTED_TRANSACTIONS_COLLECTION
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("hash", ASCENDING)], unique=True)
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("from_address", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("investigation_id", ASCENDING)])
        logger.info(f"Indexes ensured for {FORMATTED_TRANSACTIONS_COLLECTION} collection.")

        # Indexes for DONE_INVESTIGATIONS_COLLECTION (NEW)
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("investigation_id", ASCENDING)], unique=True)
        logger.info(f"Indexes ensured for {DONE_INVESTIGATIONS_COLLECTION} collection.")

    except Exception as e:
        logger.error(f"Failed to setup MongoDB indexes: {e}")
    finally:
        if client:
            client.close()

def save_formatted_transaction(transaction_data: Dict[str, Any]) -> None:
    """
    Saves a single transaction in the specified format to the FORMATTED_TRANSACTIONS_COLLECTION.
    Updates if a document with the same 'hash' exists, otherwise inserts.
    """
    collection, client = get_mongo_collection(FORMATTED_TRANSACTIONS_COLLECTION)
    try:
        collection.update_one(
            {"hash": transaction_data["hash"]},
            {"$set": transaction_data},
            upsert=True
        )
        logger.info(f"Successfully saved formatted transaction with hash: {transaction_data.get('hash', 'N/A')} for Investigation ID: {transaction_data.get('investigation_id', 'N/A')}")
    except Exception as e:
        logger.error(f"Failed to save formatted transaction with hash {transaction_data.get('hash', 'N/A')}: {e}")
    finally:
        client.close()

# --- Done Investigations Management (NEW) ---
def is_investigation_done(investigation_id: str) -> bool:
    """
    Checks if an investigation ID has already been marked as done.
    Returns True if done, False otherwise.
    """
    collection, client = get_mongo_collection(DONE_INVESTIGATIONS_COLLECTION)
    try:
        doc = collection.find_one({"investigation_id": investigation_id})
        return doc is not None
    except Exception as e:
        logger.error(f"Error checking if investigation {investigation_id} is done: {e}")
        return False # Assume not done on error to retry
    finally:
        client.close()

def mark_investigation_as_done(investigation_id: str) -> None:
    """
    Marks an investigation ID as done by adding it to the DONE_INVESTIGATIONS_COLLECTION.
    """
    collection, client = get_mongo_collection(DONE_INVESTIGATIONS_COLLECTION)
    try:
        # Use upsert=True to insert if not exists, preventing duplicates
        collection.update_one(
            {"investigation_id": investigation_id},
            {"$set": {"investigation_id": investigation_id, "completed_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        logger.info(f"Investigation ID {investigation_id} marked as done.")
    except Exception as e:
        logger.error(f"Error marking investigation {investigation_id} as done: {e}")
    finally:
        client.close()

# --- Investigation System Simulation ---
async def get_active_investigations() -> List[Dict[str, str]]:
    """
    Simulates fetching active investigations from your system.
    Reads from 'active_investigations.txt'.
    """
    active_investigations_from_file = []
    try:
        with open("active_investigations.txt", "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(',')
                    if len(parts) == 2:
                        active_investigations_from_file.append({
                            "investigation_id": parts[0].strip(),
                            "address": parts[1].strip()
                        })
    except FileNotFoundError:
        logger.warning("active_investigations.txt not found. No investigations loaded.")
    except Exception as e:
        logger.error(f"Error reading active_investigations.txt: {e}")
    
    return active_investigations_from_file

# --- Transaction Processing Logic ---
async def process_transactions_for_from_address(
    investigation_id: str,
    target_from_address: str
) -> bool: # Returns True if processing was successful, False otherwise
    """
    Finds all transactions associated with a given from_address, formats them,
    and saves each one individually to the FORMATTED_TRANSACTIONS_COLLECTION,
    including the investigation_id.
    """
    tx_collection, client = None, None
    try:
        tx_collection, client = get_mongo_collection(TX_COLLECTION)
        logger.info(f"[{investigation_id}] Searching for transactions from address: {target_from_address}")

        transactions_cursor = tx_collection.find(
            {"from_address": target_from_address, "to_address": {"$exists": True, "$ne": None}}
        )

        total_transactions_processed = 0

        for doc in transactions_cursor:
            total_transactions_processed += 1

            formatted_doc = {
                "investigation_id": investigation_id,
                "hash": doc.get("hash"),
                "from_address": doc.get("from_address"),
                "last_updated_at": datetime.now(timezone.utc),
                "to_addresses": doc.get("to_address"),
                "DestinationTag": doc.get("DestinationTag"),
                "value": doc.get("value"),
                "fee": doc.get("fee"),
                "blockchain": "XRPL",
                "timestamp": doc.get("date")
            }

            save_formatted_transaction(formatted_doc)

        logger.info(f"[{investigation_id}] Total transactions processed and saved to {FORMATTED_TRANSACTIONS_COLLECTION} for '{target_from_address}': {total_transactions_processed}")

        if total_transactions_processed == 0:
            logger.warning(f"[{investigation_id}] No transactions with a 'to_address' field found for from_address: {target_from_address}.")
        
        return True # Indicate successful processing

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing transactions for {target_from_address}: {e}")
        return False # Indicate failed processing
    finally:
        if client:
            client.close()

async def main_loop():
    """
    Main loop to periodically check for investigations and process addresses,
    ensuring each investigation is processed only once.
    """
    setup_mongodb_indexes() # Setup indexes for all collections

    while True:
        logger.info(f"--- Starting new polling cycle for investigations (every {POLLING_INTERVAL_SECONDS} seconds) ---")
        active_investigations = await get_active_investigations()

        if not active_investigations:
            logger.info("No active investigations found. Waiting for next cycle.")
        else:
            logger.info(f"Found {len(active_investigations)} active investigation(s).")
            
            processing_tasks = []
            for investigation in active_investigations:
                investigation_id = investigation.get("investigation_id")
                address = investigation.get("address")

                if not investigation_id or not address:
                    logger.warning(f"Skipping malformed investigation entry: {investigation}")
                    continue

                if is_investigation_done(investigation_id):
                    logger.info(f"Investigation ID {investigation_id} already processed. Skipping.")
                else:
                    logger.info(f"Initiating processing for NEW Investigation ID: {investigation_id}, Address: {address}")
                    # Create a task to process and then mark as done
                    async def process_and_mark_done(inv_id, addr):
                        success = await process_transactions_for_from_address(inv_id, addr)
                        if success:
                            mark_investigation_as_done(inv_id)
                        else:
                            logger.error(f"Processing failed for Investigation ID {inv_id}. Will retry in next cycle.")
                    
                    processing_tasks.append(
                        asyncio.create_task(process_and_mark_done(investigation_id, address))
                    )
            
            if processing_tasks:
                await asyncio.gather(*processing_tasks) # Wait for all tasks to complete

        logger.info(f"--- Polling cycle complete. Waiting {POLLING_INTERVAL_SECONDS} seconds ---")
        await asyncio.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    # How to simulate investigations:
    # Create a file named 'active_investigations.txt' in the same directory as the script.
    # Each line should contain an investigation_id and an address, separated by a comma.
    # Example content for 'active_investigations.txt':
    # INV001,rR5NDJVgVpsRibiji3i4x6hTwTSPVPhag
    # INV002,rAnotherAddressYouHaveDataFor
    # You can add new lines to this file while the script is running to simulate new investigations.

    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        sys.exit(1)