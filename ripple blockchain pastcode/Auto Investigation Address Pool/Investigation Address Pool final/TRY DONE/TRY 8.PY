import multiprocessing
import logging
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timezone, timedelta
import sys
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ProcessPoolExecutor
import pytz # Import pytz

# Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "try"
AUTO_INVESTIGATIONS_COLLECTION = "crypto_forensics.auto_investigations"
FORMATTED_TRANSACTIONS_COLLECTION = "formatted_transaction"
DONE_INVESTIGATIONS_COLLECTION = "done_investigation"

# Blockchain specific collections
BITCOIN_COLLECTION = "crypto_forensics.bitcoin_investigations"
ETHEREUM_COLLECTION = "crypto_forensics.ethereum_investigation_graph_result"
TRON_COLLECTION = "crypto_forensics.tron_investigation_graph_result"

# Polling Interval (in seconds)
POLLING_INTERVAL_SECONDS = 10
# Time window for processing new investigations (e.g., 60 seconds around current time)
# This means it will process investigations with start_date from (current_time - TIME_WINDOW_SECONDS/2) to (current_time + TIME_WINDOW_SECONDS/2)
TIME_WINDOW_SECONDS = 60 

# Define India Standard Time (IST) timezone
INDIA_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- MongoDB Utility Functions ---
def get_mongo_collection(db_name: str, collection_name: str):
    """
    Connects to MongoDB and returns the specified collection and the client.
    Each process should create its own client connection.
    """
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    return db[collection_name], client

def setup_mongodb_indexes():
    """
    Ensures necessary indexes are created for all relevant collections.
    This function should ideally be called once, e.g., at the start of the main process.
    """
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("inv_id", ASCENDING)], unique=True)
        # Ensure an index on 'start_date' for efficient time-based queries
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("start_date", ASCENDING)]) 
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("from_address", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("investigation_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("blockchain", ASCENDING)])
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("investigation_id", ASCENDING)], unique=True)
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("address", ASCENDING)])
        db[BITCOIN_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[TRON_COLLECTION].create_index([("culprit_address", ASCENDING)])

    except Exception as e:
        logger.error(f"Failed to setup MongoDB indexes: {e}")
    finally:
        if client:
            client.close()

def save_formatted_transaction(transaction_data: Dict[str, Any]) -> None:
    """
    Saves a single transaction in the specified format to the FORMATTED_TRANSACTIONS_COLLECTION.
    Inserts a new document for every call.
    """
    collection, client = get_mongo_collection(DB_NAME, FORMATTED_TRANSACTIONS_COLLECTION)
    try:
        collection.insert_one(transaction_data)
        
    finally:
        client.close()

# --- Done Investigations Management ---
def is_investigation_done(investigation_id: str) -> bool:
    """
    Checks if an investigation ID has already been marked as done.
    Returns True if done, False otherwise.
    """
    collection, client = get_mongo_collection(DB_NAME, DONE_INVESTIGATIONS_COLLECTION)
    try:
        doc = collection.find_one({"investigation_id": investigation_id})
        return doc is not None
    except Exception as e:
        logger.error(f"Error checking if investigation {investigation_id} is done: {e}")
        return False
    finally:
        client.close()

def mark_investigation_as_done(investigation_id: str, address: str) -> None:
    """
    Marks an investigation ID as done by adding it to the DONE_INVESTIGATIONS_COLLECTION.
    Includes the address for which the investigation was performed.
    """
    collection, client = get_mongo_collection(DB_NAME, DONE_INVESTIGATIONS_COLLECTION)
    try:
        # Use IST for completed_at timestamp
        collection.update_one(
            {"investigation_id": investigation_id},
            {"$set": {"investigation_id": investigation_id, "completed_at": datetime.now(INDIA_TIMEZONE), "address": address}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error marking investigation {investigation_id} for address {address} as done: {e}")
    finally:
        client.close()

# --- Investigation System Fetching ---
def get_investigations_by_time_range(start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Fetches investigations from the crypto_forensics.auto_investigations collection
    within a specific time range for the 'start_date' field (Unix timestamp).
    """
    investigations_to_process = []
    collection, client = None, None
    try:
        collection, client = get_mongo_collection(DB_NAME, AUTO_INVESTIGATIONS_COLLECTION)
        
        # Convert datetime objects to Unix timestamps (seconds) for query
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        query = {"start_date": {"$gte": start_timestamp, "$lt": end_timestamp}}
        # Sort by start_date to process chronologically within the window
        cursor = collection.find(query).sort("start_date", ASCENDING) 
        
        for doc in cursor:
            required_fields = ["inv_id", "start_address", "start_date"]
            if all(field in doc for field in required_fields):
                investigation_details = {
                    "investigation_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "investigation_timestamp": doc.get("timestamp"),
                    "user_id": doc.get("user_id"),
                    "org_id": doc.get("org_id"),
                    "org": doc.get("org"),
                    "coin": doc.get("coin"),
                    # Convert Unix timestamp from DB to IST datetime object for internal use
                    "start_date": datetime.fromtimestamp(doc["start_date"], tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                }
                investigations_to_process.append(investigation_details)
            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION}: {doc}")
    except Exception as e:
        logger.error(f"Error fetching investigations by time range from {DB_NAME}.{AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    finally:
        if client:
            client.close()
    return investigations_to_process

def get_past_investigations() -> List[Dict[str, Any]]:
    """
    Fetches all investigations from crypto_forensics.auto_investigations
    where 'start_date' is in the past (relative to current IST), sorted by 'start_date' ascending.
    'start_date' in DB is assumed to be a Unix timestamp.
    """
    past_investigations = []
    collection, client = None, None
    try:
        collection, client = get_mongo_collection(DB_NAME, AUTO_INVESTIGATIONS_COLLECTION)
        
        current_time_ist = datetime.now(INDIA_TIMEZONE) # Get current time in IST
        # Convert current IST time to Unix timestamp (seconds) for query
        current_timestamp_ist = int(current_time_ist.timestamp())

        query = {"start_date": {"$lte": current_timestamp_ist}}
        # Sort by start_date to process older investigations first
        cursor = collection.find(query).sort("start_date", ASCENDING) 
        
        for doc in cursor:
            required_fields = ["inv_id", "start_address", "start_date"]
            if all(field in doc for field in required_fields):
                investigation_details = {
                    "investigation_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "investigation_timestamp": doc.get("timestamp"),
                    "user_id": doc.get("user_id"),
                    "org_id": doc.get("org_id"),
                    "org": doc.get("org"),
                    "coin": doc.get("coin"),
                    # Convert Unix timestamp from DB to IST datetime object for internal use
                    "start_date": datetime.fromtimestamp(doc["start_date"], tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                }
                past_investigations.append(investigation_details)
            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION}: {doc}")
    except Exception as e:
        logger.error(f"Error fetching past investigations from {DB_NAME}.{AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    finally:
        if client:
            client.close()
    return past_investigations

# --- Transaction Processing Logic (ADAPTED FOR MULTIPLE BLOCKCHAINS) ---
def process_transactions_for_address(
    inv_details: Dict[str, Any],
    target_address: str,
    blockchain: str,
    tx_collection_name: str,
    field_mapping: Dict[str, str]
) -> int:
    """
    Finds and processes transactions for a given address in a specific blockchain collection.
    This function is now synchronous and uses a field_mapping for common fields.
    Returns the number of transactions processed for this blockchain.
    """
    tx_collection, client = None, None
    total_transactions_processed = 0
    investigation_id = inv_details.get("investigation_id")
    try:
        tx_collection, client = get_mongo_collection(DB_NAME, tx_collection_name)

        from_field_in_db = field_mapping.get("from_address")
        if not from_field_in_db:
            logger.error(f"[{investigation_id}] Missing 'from_address' mapping for {blockchain}. Cannot process transactions.")
            return 0

        query = {from_field_in_db: target_address}
        
        transactions_cursor = tx_collection.find(query)

        for doc in transactions_cursor:
            total_transactions_processed += 1

            formatted_doc = {
                "inv_id": investigation_id,
                "address": target_address,
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"),
                "org": inv_details.get("org"),
                "coin": inv_details.get("coin"),
                "blockchain": blockchain,
                "last_updated_at": datetime.now(INDIA_TIMEZONE), # Use IST for last_updated_at
            }
            
            if "timestamp" in field_mapping and doc.get(field_mapping["timestamp"]) is not None:
                formatted_doc["timestamp"] = doc.get(field_mapping["timestamp"])
            if "to_address" in field_mapping and doc.get(field_mapping["to_address"]) is not None:
                formatted_doc["to_addresses"] = doc.get(field_mapping["to_address"])
            if "value" in field_mapping and doc.get(field_mapping["value"]) is not None:
                formatted_doc["value"] = doc.get(field_mapping["value"])
            if "fee" in field_mapping and doc.get(field_mapping["fee"]) is not None:
                formatted_doc["fee"] = doc.get(field_mapping["fee"])
            if "destination_tag" in field_mapping and doc.get(field_mapping["destination_tag"]) is not None:
                formatted_doc["DestinationTag"] = doc.get(field_mapping["destination_tag"])

            save_formatted_transaction(formatted_doc)
        
        return total_transactions_processed

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing {blockchain} transactions for {target_address} from {tx_collection_name}: {e}")
        return 0
    finally:
        if client:
            client.close()


def process_investigation(inv_details: Dict[str, Any]) -> bool:
    """
    Processes an individual investigation across all relevant blockchain collections.
    Returns True if the processing completed without critical errors, False otherwise.
    Also logs the total number of transactions found for the address.
    """
    overall_processing_successful = True
    investigation_id = inv_details.get("investigation_id")
    address = inv_details.get("address")
    total_connections_found = 0 # Initialize counter for all connections

    bitcoin_field_mapping = {
        "from_address": "culprit_address",
        "to_address": "output_address",
        "value": "value",
        "fee": "fee",
        "timestamp": "time"
    }

    ethereum_field_mapping = {
        "from_address": "culprit_address",
        "to_address": "to",
        "value": "value",
        "fee": "gasUsed",
        "timestamp": "timestamp"
    }

    tron_field_mapping = {
        "from_address": "culprit_address",
        "to_address": "to_address",
        "value": "amount",
        "fee": "fee",
        "timestamp": "timestamp",
        "destination_tag": "DestinationTag"
    }

    try:
        btc_connections = process_transactions_for_address(
            inv_details, address, "Bitcoin", BITCOIN_COLLECTION, bitcoin_field_mapping
        )
        total_connections_found += btc_connections
    except Exception as e:
        logger.error(f"[{investigation_id}] Critical error processing Bitcoin transactions: {e}")
        overall_processing_successful = False

    try:
        eth_connections = process_transactions_for_address(
            inv_details, address, "Ethereum", ETHEREUM_COLLECTION, ethereum_field_mapping
        )
        total_connections_found += eth_connections
    except Exception as e:
        logger.error(f"[{investigation_id}] Critical error processing Ethereum transactions: {e}")
        overall_processing_successful = False

    try:
        tron_connections = process_transactions_for_address(
            inv_details, address, "Tron", TRON_COLLECTION, tron_field_mapping
        )
        total_connections_found += tron_connections
    except Exception as e:
        logger.error(f"[{investigation_id}] Critical error processing Tron transactions: {e}")
        overall_processing_successful = False

    # Log the total connections found for this investigation
    logger.info(f"[{investigation_id}] Address: {address} has {total_connections_found} connections.")

    return overall_processing_successful

def process_and_mark_done_for_pool(inv_details: Dict[str, Any]):
    """
    Wrapper function to process an investigation and mark it as done,
    regardless of whether transactions were found, as long as processing didn't critically fail.
    """
    investigation_id = inv_details.get("investigation_id")
    address = inv_details.get("address")
    
    processing_status = process_investigation(inv_details)
    
    if processing_status:
        mark_investigation_as_done(investigation_id, address)
    else:
        logger.error(f"Processing for Investigation ID {investigation_id} encountered critical errors. It will NOT be marked as done and will be re-attempted in the next cycle.")

def process_investigations_in_parallel(investigations: List[Dict[str, Any]]):
    """Helper function to run investigations in parallel using ProcessPoolExecutor."""
    if not investigations:
        return

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = []
        for investigation in investigations:
            investigation_id = investigation.get("investigation_id")
            
            if not investigation_id:
                logger.warning(f"Skipping malformed investigation entry: {investigation}")
                continue

            if is_investigation_done(investigation_id):
                logger.info(f"Searching: inv_id: {investigation_id}, address: {investigation.get('address')} (already processed).")
            else:
                logger.info(f"Processing: inv_id: {investigation_id}, address: {investigation.get('address')}, start_date: {investigation.get('start_date')}")
                futures.append(executor.submit(process_and_mark_done_for_pool, investigation))
        
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"A task in the process pool failed: {e}")


def main_loop():
    """
    Main loop to first process all past investigations, then continuously poll
    for new investigations within a time window, ensuring each is processed once.
    """
    setup_mongodb_indexes()

    # --- Initial Fast Processing of Past Investigations ---
    logger.info("Starting initial fast processing of investigations with past start_date (IST)...")
    past_investigations = get_past_investigations()
    if past_investigations:
        logger.info(f"Found {len(past_investigations)} past investigations to process.")
        process_investigations_in_parallel(past_investigations)
        logger.info("Finished initial fast processing of past investigations.")
    else:
        logger.info("No past investigations found to process initially.")

    # --- Continuous Polling for New Investigations within a Time Window ---
    logger.info("Starting continuous polling for new investigations within the time window (IST).")
    while True:
        current_time_ist = datetime.now(INDIA_TIMEZONE) # Get current time in IST
        # Define the time window for new investigations
        # This will process investigations whose start_date is between 
        # (current_time_ist - TIME_WINDOW_SECONDS/2) and (current_time_ist + TIME_WINDOW_SECONDS/2)
        time_window_start = current_time_ist - timedelta(seconds=TIME_WINDOW_SECONDS / 2)
        time_window_end = current_time_ist + timedelta(seconds=TIME_WINDOW_SECONDS / 2)

        logger.info(f"Polling for investigations with start_date (IST) between {time_window_start} and {time_window_end}")
        new_investigations = get_investigations_by_time_range(time_window_start, time_window_end)

        if not new_investigations:
            logger.info(f"No new investigations found in the current time window. Waiting for {POLLING_INTERVAL_SECONDS} seconds.")
        else:
            logger.info(f"Found {len(new_investigations)} new investigations in the current time window.")
            process_investigations_in_parallel(new_investigations)
            
        time.sleep(POLLING_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        sys.exit(1)