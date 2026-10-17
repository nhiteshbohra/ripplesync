import multiprocessing
import logging
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timezone, timedelta
import sys
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ProcessPoolExecutor
import pytz
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration - Loaded from environment variables
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
AUTO_INVESTIGATIONS_COLLECTION = os.getenv("AUTO_INVESTIGATIONS_COLLECTION")
FORMATTED_TRANSACTIONS_COLLECTION = os.getenv("FORMATTED_TRANSACTIONS_COLLECTION")
DONE_INVESTIGATIONS_COLLECTION = os.getenv("DONE_INVESTIGATIONS_COLLECTION")
BLOCKCHAIN_USERS_COLLECTION = os.getenv("BLOCKCHAIN_USERS_COLLECTION")

# Blockchain specific collections
BITCOIN_COLLECTION = os.getenv("BITCOIN_COLLECTION")
ETHEREUM_COLLECTION = os.getenv("ETHEREUM_COLLECTION")
TRON_COLLECTION = os.getenv("TRON_COLLECTION")

# Polling Interval (in seconds)
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL_SECONDS"))
# Time window for processing new investigations (e.g., 60 seconds around current time)
TIME_WINDOW_SECONDS = int(os.getenv("TIME_WINDOW_SECONDS"))

# Define India Standard Time (IST) timezone
INDIA_TIMEZONE = pytz.timezone(os.getenv("INDIA_TIMEZONE"))

# Log file configuration
LOG_FILE = os.getenv("LOG_FILE")

# Setup logging
# Clear existing handlers to prevent duplicate output if re-configured
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
    ]
)
logger = logging.getLogger(__name__)

# --- NEW: Setup a separate logger for check.txt ---
check_logger = logging.getLogger("check_logger")
check_logger.setLevel(logging.INFO)
check_handler = logging.FileHandler("check.txt")
# We only want the message in check.txt, no timestamp or level
check_formatter = logging.Formatter("%(message)s")
check_handler.setFormatter(check_formatter)
check_logger.addHandler(check_handler)
# Prevent messages from propagating to the root logger (and thus to LOG_FILE)
check_logger.propagate = False
# --- END NEW LOGGER SETUP ---


# Global variable to hold the MongoDB client for each process (main and pool workers)
_mongo_client = None

# Global cache for org_id lookups to reduce redundant DB calls
_org_id_cache: Dict[str, Optional[str]] = {}

def init_mongo_client():
    """
    Initializes a MongoDB client for the current process.
    This function is designed to be used as an initializer for ProcessPoolExecutor
    and also explicitly in the main process.
    """
    global _mongo_client
    if _mongo_client is None:
        try:
            _mongo_client = MongoClient(MONGO_URI)
            logger.debug(f"MongoDB client initialized for process {multiprocessing.current_process().pid}")
        except Exception as e:
            logger.critical(f"Failed to initialize MongoDB client in process {multiprocessing.current_process().pid}: {e}")
            sys.exit(1) # Exit critically if MongoDB client cannot be initialized

def get_db_collection(collection_name: str):
    """
    Returns the specified MongoDB collection from the globally managed client.
    Assumes _mongo_client has been initialized in the current process.
    Raises RuntimeError if client is not initialized, indicating a programming error.
    """
    global _mongo_client
    if _mongo_client is None:
        raise RuntimeError("MongoDB client not initialized. Call init_mongo_client first in this process.")
    return _mongo_client[DB_NAME][collection_name]

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
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("record_timestamp", ASCENDING)])
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("coin", ASCENDING)])
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("case_id", ASCENDING)])
        
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("address", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("user_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("org_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("coin", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("timestamp", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("record_timestamp", ASCENDING)])

        db[BLOCKCHAIN_USERS_COLLECTION].create_index([("user_id", ASCENDING)], unique=True)
        db[BLOCKCHAIN_USERS_COLLECTION].create_index([("org_id", ASCENDING)])

        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("investigation_id", ASCENDING)], unique=True)
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("address", ASCENDING)])

        db[BITCOIN_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[BITCOIN_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[BITCOIN_COLLECTION].create_index([("doc_type", ASCENDING)])

        db[ETHEREUM_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("doc_type", ASCENDING)])

        db[TRON_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[TRON_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[TRON_COLLECTION].create_index([("doc_type", ASCENDING)])
        logger.info("MongoDB indexes ensured.")

    except Exception as e:
        logger.error(f"Failed to setup MongoDB indexes: {e}")
    finally:
        if client:
            client.close()

def save_formatted_transactions_bulk(transactions_data: List[Dict[str, Any]]) -> bool:
    """
    Saves multiple transactions in a single bulk insert operation.
    Returns True on successful insertion, False otherwise.
    """
    if not transactions_data:
        logger.info("No formatted transactions to insert.")
        return True # Considered successful if nothing to insert
    
    collection = get_db_collection(FORMATTED_TRANSACTIONS_COLLECTION)
    try:
        result = collection.insert_many(transactions_data)
        if result.acknowledged:
            logger.info(f"Successfully inserted {len(transactions_data)} formatted transactions.")
            return True
        else:
            logger.error(f"Failed to acknowledge insertion of {len(transactions_data)} formatted transactions.")
            return False
    except Exception as e:
        logger.error(f"Error saving formatted transactions in bulk: {e}")
        return False

def is_investigation_done(investigation_id: str) -> bool:
    """
    Checks if an investigation ID has already been marked as done.
    Returns True if done, False otherwise.
    """
    collection = get_db_collection(DONE_INVESTIGATIONS_COLLECTION)
    try:
        doc = collection.find_one({"investigation_id": investigation_id})
        return doc is not None
    except Exception as e:
        logger.error(f"Error checking if investigation {investigation_id} is done: {e}")
        return False

def mark_investigation_as_done(investigation_id: str, address: str) -> None:
    """
    Marks an investigation ID as done by adding it to the DONE_INVESTIGATIONS_COLLECTION.
    Includes the address for which the investigation was performed.
    """
    collection = get_db_collection(DONE_INVESTIGATIONS_COLLECTION)
    try:
        collection.update_one(
            {"investigation_id": investigation_id},
            {"$set": {"investigation_id": investigation_id, "address": address}},
            upsert=True
        )
        logger.info(f"Investigation {investigation_id} for address {address} marked as done.")
    except Exception as e:
        logger.error(f"Error marking investigation {investigation_id} for address {address} as done: {e}")

def get_unix_timestamp_in_seconds(doc_field: Any) -> int:
    """
    Safely extracts a Unix timestamp from a MongoDB document field.
    Handles $numberLong format and converts milliseconds to seconds.
    Returns 0 if the field is missing or cannot be converted.
    """
    if isinstance(doc_field, dict) and "$numberLong" in doc_field:
        try:
            return int(int(doc_field["$numberLong"]) / 1000)
        except (ValueError, TypeError):
            return 0
    elif isinstance(doc_field, (int, float)):
        # Assume if timestamp is very large, it's in milliseconds
        if doc_field > 1_000_000_000_000:
            return int(doc_field / 1000)
        return int(doc_field)
    return 0

def get_org_id_for_user(user_id: str) -> Optional[str]:
    """
    Queries the BLOCKCHAIN_USERS_COLLECTION to find the org_id for a given user_id.
    Uses an in-memory cache to reduce redundant database calls.
    Returns the org_id if found, None otherwise.
    """
    global _org_id_cache
    if user_id in _org_id_cache:
        logger.debug(f"Found org_id '{_org_id_cache[user_id]}' for user_id '{user_id}' in cache.")
        return _org_id_cache[user_id]

    collection = get_db_collection(BLOCKCHAIN_USERS_COLLECTION)
    try:
        doc = collection.find_one({"user_id": user_id}, {"org_id": 1, "_id": 0})
        if doc and "org_id" in doc:
            org_id = doc["org_id"]
            _org_id_cache[user_id] = org_id # Cache the result
            logger.debug(f"Found org_id '{org_id}' for user_id '{user_id}'. Cached.")
            return org_id
        else:
            _org_id_cache[user_id] = None # Cache None for not found
            logger.warning(f"No org_id found for user_id '{user_id}' in {BLOCKCHAIN_USERS_COLLECTION}. Cached None.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving org_id for user_id '{user_id}': {e}")
        return None

def get_investigations_by_time_range(start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Fetches investigations from the crypto_forensics.auto_investigations collection
    within a specific time range for the 'record_timestamp' field (Unix timestamp in milliseconds).
    """
    investigations_to_process = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
        # Convert datetime objects to milliseconds Unix timestamp for MongoDB query
        start_timestamp_ms = int(start_time.timestamp() * 1000)
        end_timestamp_ms = int(end_time.timestamp() * 1000)

        query = {"record_timestamp": {"$gte": start_timestamp_ms, "$lte": end_timestamp_ms}}
        cursor = collection.find(query).sort("record_timestamp", ASCENDING)

        for doc in cursor:
            required_fields = ["inv_id", "start_address", "coin"]
            if all(field in doc for field in required_fields):
                investigation_record_timestamp_unix = get_unix_timestamp_in_seconds(doc.get("record_timestamp"))
                if investigation_record_timestamp_unix == 0:
                    current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                    logger.warning(f"Investigation {doc.get('inv_id')} has missing or zero record_timestamp. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                    investigation_record_timestamp_unix = current_unix_timestamp

                user_id = doc.get("user_id")
                org_id = None
                if user_id:
                    org_id = get_org_id_for_user(user_id)
                
                # Convert Unix timestamp to IST datetime object for consistent storage and logging
                record_timestamp_ist = datetime.fromtimestamp(investigation_record_timestamp_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)

                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "user_id": user_id,
                    "coin": doc["coin"],
                    "record_timestamp_unix": investigation_record_timestamp_unix, # Keep for potential future use or debugging
                    "record_timestamp_ist": record_timestamp_ist, # NEW: IST datetime object
                    "org_id": org_id,
                    "case_id": doc.get("case_id")
                }
                investigations_to_process.append(investigation_details)
                
                # Log to check.txt for new investigations
                check_logger.info(f"inv_id: {doc['inv_id']}, case_id: {doc.get('case_id')}, record_timestamp: {record_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION} (missing inv_id, start_address, or coin): {doc}")
    except Exception as e:
        logger.error(f"Error fetching investigations by time range from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return investigations_to_process

def get_past_investigations() -> List[Dict[str, Any]]:
    """
    Fetches all investigations from crypto_forensics.auto_investigations
    where 'record_timestamp' is in the past (relative to current IST), sorted by 'record_timestamp' ascending.
    'record_timestamp' in DB is assumed to be a Unix timestamp in milliseconds.
    """
    past_investigations = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
        current_time_ist = datetime.now(INDIA_TIMEZONE)
        current_timestamp_ms = int(current_time_ist.timestamp() * 1000)

        query = {"record_timestamp": {"$lte": current_timestamp_ms}}
        cursor = collection.find(query).sort("record_timestamp", ASCENDING)

        for doc in cursor:
            required_fields = ["inv_id", "start_address", "coin"]
            if all(field in doc for field in required_fields):
                investigation_record_timestamp_unix = get_unix_timestamp_in_seconds(doc.get("record_timestamp"))
                if investigation_record_timestamp_unix == 0:
                    current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                    logger.warning(f"Investigation {doc.get('inv_id')} has missing or zero record_timestamp. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                    investigation_record_timestamp_unix = current_unix_timestamp

                user_id = doc.get("user_id")
                org_id = None
                if user_id:
                    org_id = get_org_id_for_user(user_id)

                # Convert Unix timestamp to IST datetime object for consistent storage and logging
                record_timestamp_ist = datetime.fromtimestamp(investigation_record_timestamp_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)

                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "user_id": user_id,
                    "coin": doc["coin"],
                    "record_timestamp_unix": investigation_record_timestamp_unix, # Keep for potential future use or debugging
                    "record_timestamp_ist": record_timestamp_ist, # NEW: IST datetime object
                    "org_id": org_id,
                    "case_id": doc.get("case_id")
                }
                past_investigations.append(investigation_details)

                # Log to check.txt for past investigations
                check_logger.info(f"inv_id: {doc['inv_id']}, case_id: {doc.get('case_id')}, record_timestamp: {record_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION} (missing inv_id, start_address, or coin): {doc}")
    except Exception as e:
        logger.error(f"Error fetching past investigations from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return past_investigations

def process_transactions_for_address(
    inv_details: Dict[str, Any],
    tx_collection_name: str,
) -> List[Dict[str, Any]]:
    """
    Finds and processes transactions for a given inv_id in a specific blockchain collection,
    filtering for documents where 'doc_type' is "node".
    Returns a list of formatted transactions.
    """
    formatted_transactions = []
    tx_collection = get_db_collection(tx_collection_name)
    investigation_id = inv_details.get("inv_id")

    try:
        query = {"inv_id": investigation_id, "doc_type": "node"}
        transactions_cursor = tx_collection.find(query)

        for doc in transactions_cursor:
            formatted_doc = {
                "inv_id": investigation_id,
                "address": doc.get("address"),
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"),
                "coin": inv_details.get("coin"),
                "case_id": doc.get("case_id"),
                "record_timestamp": inv_details.get("record_timestamp_ist") # Now storing IST datetime object
            }
            formatted_transactions.append(formatted_doc)

        logger.debug(f"[{investigation_id}] Found {len(formatted_transactions)} 'node' documents in {tx_collection_name} for inv_id: {investigation_id}")
        return formatted_transactions

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing transactions from {tx_collection_name}: {e}")
        return []

def process_investigation(inv_details: Dict[str, Any]) -> bool:
    """
    Processes an individual investigation across all relevant blockchain collections,
    based on the 'coin' type specified in the investigation details.
    Returns True if the processing completed successfully AND transactions were saved, False otherwise.
    Also logs the total number of transactions found for the address.
    """
    overall_processing_successful = True
    investigation_id_for_logging = inv_details.get("inv_id")
    address = inv_details.get("address")
    coin_type = inv_details.get("coin")

    if coin_type:
        coin_type = coin_type.upper()
    else:
        logger.warning(f"[{investigation_id_for_logging}] Investigation has no 'coin' type specified. Skipping transaction processing.")
        return False

    all_formatted_transactions = []

    try:
        if coin_type == "BTC" or coin_type == "BITCOIN":
            logger.info(f"[{investigation_id_for_logging}] Processing Bitcoin transactions for address: {address}")
            btc_transactions = process_transactions_for_address(
                inv_details, BITCOIN_COLLECTION
            )
            all_formatted_transactions.extend(btc_transactions)
        elif coin_type == "ETH" or coin_type == "ETHEREUM":
            logger.info(f"[{investigation_id_for_logging}] Processing Ethereum transactions for address: {address}")
            eth_transactions = process_transactions_for_address(
                inv_details, ETHEREUM_COLLECTION
            )
            all_formatted_transactions.extend(eth_transactions)
        elif coin_type == "TRX" or coin_type == "TRON":
            logger.info(f"[{investigation_id_for_logging}] Processing Tron transactions for address: {address}")
            tron_transactions = process_transactions_for_address(
                inv_details, TRON_COLLECTION
            )
            all_formatted_transactions.extend(tron_transactions)
        else:
            logger.warning(f"[{investigation_id_for_logging}] Unknown coin type '{coin_type}'. Skipping transaction processing.")
            overall_processing_successful = False # Mark as false if coin type is unknown

    except Exception as e:
        logger.critical(f"[{investigation_id_for_logging}] An unexpected critical error occurred during coin-specific transaction processing: {e}")
        overall_processing_successful = False

    # Only attempt to save if processing was successful so far
    if overall_processing_successful:
        if all_formatted_transactions:
            if not save_formatted_transactions_bulk(all_formatted_transactions):
                logger.error(f"[{investigation_id_for_logging}] Failed to save formatted transactions. Marking overall processing as failed.")
                overall_processing_successful = False
        else:
            logger.info(f"[{investigation_id_for_logging}] No 'node' connections found for {address} (Coin: {coin_type}). No transactions to save.")
            # Still considered successful if no transactions were found, but processing was fine.

    logger.info(f"[{investigation_id_for_logging}] Investigated ID: {address} (Coin: {coin_type}) has {len(all_formatted_transactions)} total 'node' connections. Overall processing status: {overall_processing_successful}")

    return overall_processing_successful

def process_and_mark_done_for_pool(inv_details: Dict[str, Any]):
    """
    Wrapper function to process an investigation and mark it as done,
    only if processing completed without critical errors AND data was successfully saved.
    """
    investigation_id = inv_details.get("inv_id")
    address = inv_details.get("address")

    processing_status = process_investigation(inv_details)

    if processing_status:
        mark_investigation_as_done(investigation_id, address)
    else:
        logger.error(f"Processing for Investigation ID {investigation_id} encountered critical errors or failed to save data. It will NOT be marked as done and will be re-attempted in the next cycle.")

def process_investigations_in_parallel(investigations: List[Dict[str, Any]]):
    """Helper function to run investigations in parallel using ProcessPoolExecutor."""
    if not investigations:
        logger.info("No investigations to process in parallel.")
        return

    # Use max_workers=multiprocessing.cpu_count() for optimal utilization
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count(), initializer=init_mongo_client) as executor:
        futures = []
        for investigation in investigations:
            investigation_id = investigation.get("inv_id")

            if not investigation_id:
                logger.warning(f"Skipping malformed investigation entry (missing inv_id): {investigation}")
                continue
            if not investigation.get("coin"):
                logger.warning(f"Skipping investigation {investigation_id} (missing coin type).")
                continue

            if is_investigation_done(investigation_id):
                logger.info(f"Searching: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')} (already processed).")
            else:
                org_id_log = f", org_id: {investigation.get('org_id')}" if investigation.get('org_id') else ""
                # Use the already calculated IST datetime object for logging
                log_record_timestamp_ist = investigation.get('record_timestamp_ist')

                logger.info(f"Submitting Investigation ID: {investigation.get('inv_id')} (Coin: {investigation.get('coin')}, record_timestamp: {log_record_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S IST')}){org_id_log} for parallel processing.")
                futures.append(executor.submit(process_and_mark_done_for_pool, investigation))

        for future in futures:
            try:
                future.result() # Wait for results and propagate exceptions
            except Exception as e:
                logger.error(f"A task in the process pool failed: {e}")

def main_loop():
    """
    Main loop to first process all past investigations, then continuously poll
    for new investigations within a time window, ensuring each is processed once.
    """
    global _mongo_client

    init_mongo_client() # Initialize MongoDB client for the main process
    logger.info("MongoDB client initialized for main process.")

    try:
        logger.info("Starting initial fast processing of investigations with past record_timestamp (IST)...")
        past_investigations = get_past_investigations()
        if past_investigations:
            logger.info(f"Found {len(past_investigations)} past investigations to process.")
            process_investigations_in_parallel(past_investigations)
            logger.info("Finished initial fast processing of past investigations.")
        else:
            logger.info("No past investigations found to process initially.")

        logger.info("Starting continuous polling for new investigations within the time window (IST).")
        while True:
            current_time_ist = datetime.now(INDIA_TIMEZONE)
            # Define a time window around the current time
            time_window_start = current_time_ist - timedelta(seconds=TIME_WINDOW_SECONDS / 2)
            time_window_end = current_time_ist + timedelta(seconds=TIME_WINDOW_SECONDS / 2)

            log_time_window_start_str = time_window_start.strftime('%Y-%m-%d %H:%M:%S IST')
            log_time_window_end_str = time_window_end.strftime('%Y-%m-%d %H:%M:%S IST')

            logger.info(f"Polling for investigations with record_timestamp (IST) between {log_time_window_start_str} and {log_time_window_end_str}")
            new_investigations = get_investigations_by_time_range(time_window_start, time_window_end)

            if not new_investigations:
                logger.info(f"No new investigations found in the current time window. Waiting for {POLLING_INTERVAL_SECONDS} seconds.")
            else:
                logger.info(f"Found {len(new_investigations)} new investigations in the current time window.")
                for inv in new_investigations:
                    # Use the already calculated IST datetime object for logging
                    log_record_timestamp_ist = inv.get('record_timestamp_ist')
                    org_id_log = f", org_id: {inv.get('org_id')}" if inv.get('org_id') else ""
                    logger.info(f"  Found Investigation ID: {inv.get('inv_id')} (Coin: {inv.get('coin')}, record_timestamp: {log_record_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S IST')}){org_id_log}")
                process_investigations_in_parallel(new_investigations)

            time.sleep(POLLING_INTERVAL_SECONDS)

    finally:
        if _mongo_client:
            _mongo_client.close()
            logger.info("MongoDB client for main process closed.")

if __name__ == "__main__":
    try:
        setup_mongodb_indexes() # Moved to run once at script start
        main_loop()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0) # Exit cleanly on user interruption
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        sys.exit(1) # Exit with error code for unexpected exceptions
