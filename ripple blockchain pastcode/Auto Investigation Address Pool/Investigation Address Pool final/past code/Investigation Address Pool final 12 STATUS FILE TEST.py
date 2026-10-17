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

# NEW: Status file configuration
STATUS_FILE = os.getenv("STATUS_FILE")

# Setup logging
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
check_formatter = logging.Formatter("%(message)s")
check_handler.setFormatter(check_formatter)
check_logger.addHandler(check_handler)
check_logger.propagate = False
# --- END NEW LOGGER SETUP ---


# Global variable to hold the MongoDB client for each process (main and pool workers)
_mongo_client = None

# Global cache for org_id lookups to reduce redundant DB calls
_org_id_cache: Dict[str, Optional[str]] = {}

# Define coin type to collection mapping
COIN_COLLECTION_MAP = {
    "BTC": BITCOIN_COLLECTION,
    "BITCOIN": BITCOIN_COLLECTION,
    "ETH": ETHEREUM_COLLECTION,
    "ETHEREUM": ETHEREUM_COLLECTION,
    "TRX": TRON_COLLECTION,
    "TRON": TRON_COLLECTION,
}

# --- MODIFIED: Functions for status file management (only CURRENT_PROCESS) ---
def write_current_process_status(status_value: int):
    """Writes the current process status (0 or 1) to the status file."""
    try:
        with open(STATUS_FILE, 'w') as f:
            f.write(f"CURRENT_PROCESS={status_value}\n")
        logger.debug(f"Status 'CURRENT_PROCESS={status_value}' written to {STATUS_FILE}")
    except IOError as e:
        logger.error(f"Error writing to status file {STATUS_FILE}: {e}")

def read_current_process_status() -> int:
    """Reads the current process status from the status file.
    Returns 1 if the file indicates running, 0 otherwise (default)."""
    try:
        if not os.path.exists(STATUS_FILE):
            logger.warning(f"Status file {STATUS_FILE} not found. Assuming CURRENT_PROCESS=0 (not running).")
            return 0 # Default to not running if file doesn't exist

        with open(STATUS_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith("CURRENT_PROCESS="):
                    try:
                        return int(line.split("=")[1])
                    except ValueError:
                        logger.warning(f"Invalid CURRENT_PROCESS value in status file {STATUS_FILE}. Assuming 0 (not running).")
                        return 0
        logger.warning(f"'CURRENT_PROCESS' not found in status file {STATUS_FILE}. Assuming 0 (not running).")
        return 0 # Default if line not found
    except IOError as e:
        logger.error(f"Error reading from status file {STATUS_FILE}: {e}")
        return 0 # Default to not running if there's an issue reading
    except Exception as e:
        logger.error(f"Unexpected error while reading status file {STATUS_FILE}: {e}")
        return 0
# --- END MODIFIED: Functions for status file management ---


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

        # Define indexes for all collections in a list for cleaner code
        collections_and_indexes = {
            AUTO_INVESTIGATIONS_COLLECTION: [
                ([("inv_id", ASCENDING)], {"unique": True}),
                ([("record_timestamp", ASCENDING)], {}),
                ([("coin", ASCENDING)], {}),
                ([("case_id", ASCENDING)], {}),
            ],
            FORMATTED_TRANSACTIONS_COLLECTION: [
                ([("inv_id", ASCENDING)], {}),
                ([("address", ASCENDING)], {}),
                ([("user_id", ASCENDING)], {}),
                ([("org_id", ASCENDING)], {}),
                ([("coin", ASCENDING)], {}),
                ([("timestamp", ASCENDING)], {}),
                ([("record_timestamp", ASCENDING)], {}),
            ],
            BLOCKCHAIN_USERS_COLLECTION: [
                ([("user_id", ASCENDING)], {"unique": True}),
                ([("org_id", ASCENDING)], {}),
            ],
            DONE_INVESTIGATIONS_COLLECTION: [
                ([("investigation_id", ASCENDING)], {"unique": True}),
                ([("address", ASCENDING)], {}),
                ([("currenttimestamp", ASCENDING)], {}),
            ],
            BITCOIN_COLLECTION: [
                ([("inv_id", ASCENDING)], {}),
                ([("culprit_address", ASCENDING)], {}),
                ([("doc_type", ASCENDING)], {}),
            ],
            ETHEREUM_COLLECTION: [
                ([("inv_id", ASCENDING)], {}),
                ([("culprit_address", ASCENDING)], {}),
                ([("doc_type", ASCENDING)], {}),
            ],
            TRON_COLLECTION: [
                ([("inv_id", ASCENDING)], {}),
                ([("culprit_address", ASCENDING)], {}),
                ([("doc_type", ASCENDING)], {}),
            ],
        }

        for collection_name, indexes in collections_and_indexes.items():
            for keys, options in indexes:
                db[collection_name].create_index(keys, **options)
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
        return True
    
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
        return collection.find_one({"investigation_id": investigation_id}) is not None
    except Exception as e:
        logger.error(f"Error checking if investigation {investigation_id} is done: {e}")
        return False

def mark_investigation_as_done(inv_details: Dict[str, Any]) -> None:
    """
    Marks an investigation ID as done by adding it to the DONE_INVESTIGATIONS_COLLECTION.
    Includes relevant details from inv_details and a timestamp (in IST).
    """
    collection = get_db_collection(DONE_INVESTIGATIONS_COLLECTION)
    try:
        current_time_ist = datetime.now(INDIA_TIMEZONE)
        
        collection.update_one(
            {"investigation_id": inv_details["inv_id"]},
            {"$set": {
                "investigation_id": inv_details["inv_id"],
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"),
                "case_id": inv_details.get("case_id"),
                "address": inv_details.get("address"), # Ensure address is also stored
                "currenttimestamp": current_time_ist
            }},
            upsert=True
        )
        logger.info(f"Investigation {inv_details['inv_id']} for address {inv_details.get('address')} marked as done with timestamp {current_time_ist.strftime('%Y-%m-%d %H:%M:%S IST')}.")
    except Exception as e:
        logger.error(f"Error marking investigation {inv_details.get('inv_id')} for address {inv_details.get('address')} as done: {e}")

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
        if doc_field > 1_000_000_000_000: # Assume milliseconds
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
        org_id = doc.get("org_id") if doc else None
        _org_id_cache[user_id] = org_id
        logger.debug(f"Found org_id '{org_id}' for user_id '{user_id}'. Cached." if org_id else f"No org_id found for user_id '{user_id}'. Cached None.")
        return org_id
    except Exception as e:
        logger.error(f"Error retrieving org_id for user_id '{user_id}': {e}")
        return None

def _fetch_and_format_investigations(query: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Helper function to fetch and format investigations based on a given MongoDB query.
    """
    investigations = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
        cursor = collection.find(query).sort("record_timestamp", ASCENDING)

        for doc in cursor:
            inv_id = doc.get("inv_id")
            start_address = doc.get("start_address")
            coin = doc.get("coin")

            if not all([inv_id, start_address, coin]):
                logger.warning(f"Skipping malformed investigation document (missing inv_id, start_address, or coin): {doc}")
                continue

            investigation_record_timestamp_unix = get_unix_timestamp_in_seconds(doc.get("record_timestamp"))
            if investigation_record_timestamp_unix == 0:
                current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                logger.warning(f"Investigation {inv_id} has missing or zero record_timestamp. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                investigation_record_timestamp_unix = current_unix_timestamp

            user_id = doc.get("user_id")
            org_id = get_org_id_for_user(user_id) if user_id else None
            
            record_timestamp_ist = datetime.fromtimestamp(investigation_record_timestamp_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)

            investigation_details = {
                "inv_id": inv_id,
                "address": start_address,
                "user_id": user_id,
                "coin": coin,
                "record_timestamp_unix": investigation_record_timestamp_unix,
                "record_timestamp_ist": record_timestamp_ist,
                "org_id": org_id,
                "case_id": doc.get("case_id")
            }
            investigations.append(investigation_details)
            
            check_logger.info(f"inv_id: {inv_id}, case_id: {doc.get('case_id')}, record_timestamp: {record_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

    except Exception as e:
        logger.error(f"Error fetching investigations from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return investigations

def get_investigations_by_time_range(start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Fetches investigations from the crypto_forensics.auto_investigations collection
    within a specific time range for the 'record_timestamp' field (Unix timestamp in milliseconds).
    """
    start_timestamp_ms = int(start_time.timestamp() * 1000)
    end_timestamp_ms = int(end_time.timestamp() * 1000)
    query = {"record_timestamp": {"$gte": start_timestamp_ms, "$lte": end_timestamp_ms}}
    return _fetch_and_format_investigations(query)

def get_past_investigations() -> List[Dict[str, Any]]:
    """
    Fetches all investigations from crypto_forensics.auto_investigations
    where 'record_timestamp' is in the past (relative to current IST), sorted by 'record_timestamp' ascending.
    """
    current_timestamp_ms = int(datetime.now(INDIA_TIMEZONE).timestamp() * 1000)
    query = {"record_timestamp": {"$lte": current_timestamp_ms}}
    return _fetch_and_format_investigations(query)

def process_transactions_for_address(
    inv_details: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Finds and processes transactions for a given inv_id in a specific blockchain collection,
    filtering for documents where 'doc_type' is "node".
    Returns a list of formatted transactions.
    """
    formatted_transactions = []
    investigation_id = inv_details.get("inv_id")
    coin_type = inv_details.get("coin", "").upper()
    tx_collection_name = COIN_COLLECTION_MAP.get(coin_type)

    if not tx_collection_name:
        logger.warning(f"[{investigation_id}] Unknown coin type '{inv_details.get('coin')}'. Skipping transaction processing.")
        return []

    tx_collection = get_db_collection(tx_collection_name)
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
                "record_timestamp": inv_details.get("record_timestamp_ist")
            }
            formatted_transactions.append(formatted_doc)

        logger.debug(f"[{investigation_id}] Found {len(formatted_transactions)} 'node' documents in {tx_collection_name} for inv_id: {investigation_id}")
        return formatted_transactions

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing transactions from {tx_collection_name}: {e}")
        return []

def process_investigation(inv_details: Dict[str, Any]) -> bool:
    """
    Processes an individual investigation, finds related transactions, and saves them.
    Returns True if successful (even if no transactions found), False otherwise.
    """
    investigation_id = inv_details.get("inv_id")
    address = inv_details.get("address")
    coin_type = inv_details.get("coin")

    logger.info(f"[{investigation_id}] Starting processing for Address: {address} (Coin: {coin_type})")

    all_formatted_transactions = process_transactions_for_address(inv_details)
    
    if all_formatted_transactions:
        if not save_formatted_transactions_bulk(all_formatted_transactions):
            logger.error(f"[{investigation_id}] Failed to save formatted transactions.")
            return False
        else:
            logger.info(f"[{investigation_id}] Successfully processed and saved {len(all_formatted_transactions)} 'node' connections.")
            return True
    else:
        logger.info(f"[{investigation_id}] No 'node' connections found for {address} (Coin: {coin_type}). No transactions to save.")
        return True # Still considered successful if no transactions were found

def process_and_mark_done_for_pool(inv_details: Dict[str, Any]):
    """
    Wrapper function for multiprocessing pool: processes an investigation and marks it as done
    if successful.
    """
    investigation_id = inv_details.get("inv_id")

    if is_investigation_done(investigation_id):
        logger.info(f"Skipping inv_id: {investigation_id}, address: {inv_details.get('address')} (already processed).")
        return

    if process_investigation(inv_details):
        mark_investigation_as_done(inv_details)
    else:
        logger.error(f"Processing for Investigation ID {investigation_id} failed. It will NOT be marked as done and may be re-attempted.")

def process_investigations_in_parallel(investigations: List[Dict[str, Any]]):
    """Helper function to run investigations in parallel using ProcessPoolExecutor."""
    if not investigations:
        logger.info("No investigations to process in parallel.")
        return

    max_workers = multiprocessing.cpu_count()
    logger.info(f"Using {max_workers} processes for parallel investigation processing.")

    with ProcessPoolExecutor(max_workers=max_workers, initializer=init_mongo_client) as executor:
        futures = []
        for investigation in investigations:
            # MODIFIED: Check only CURRENT_PROCESS status before submitting a new task [cite: 1]
            if read_current_process_status() == 0:
                logger.info("CURRENT_PROCESS=0 found in status file. Stopping new task submissions.")
                break

            investigation_id = investigation.get("inv_id")
            if not investigation_id:
                logger.warning(f"Skipping malformed investigation entry (missing inv_id): {investigation}")
                continue

            # Log submission
            log_record_timestamp_ist = investigation.get('record_timestamp_ist')
            org_id_log = f", org_id: {investigation.get('org_id')}" if investigation.get('org_id') else ""
            logger.info(f"Submitting Investigation ID: {investigation_id} (Coin: {investigation.get('coin')}, record_timestamp: {log_record_timestamp_ist.strftime('%Y-%m-%d %H:%M:%S IST')}){org_id_log} for parallel processing.")
            
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
    global _mongo_client

    init_mongo_client()
    logger.info("MongoDB client initialized for main process.")

    # MODIFIED: Initialize status file: CURRENT_PROCESS=1 (running) [cite: 1]
    write_current_process_status(1)

    try:
        logger.info("Starting initial processing of past investigations...")
        past_investigations = get_past_investigations()
        if past_investigations:
            logger.info(f"Found {len(past_investigations)} past investigations to process.")
            process_investigations_in_parallel(past_investigations)
            logger.info("Finished initial processing of past investigations.")
        else:
            logger.info("No past investigations found to process initially.")

        logger.info("Starting continuous polling for new investigations.")
        while read_current_process_status() == 1: # MODIFIED: Check only CURRENT_PROCESS from status file [cite: 1]
            current_time_ist = datetime.now(INDIA_TIMEZONE)
            time_window_start = current_time_ist - timedelta(seconds=TIME_WINDOW_SECONDS / 2)
            time_window_end = current_time_ist + timedelta(seconds=TIME_WINDOW_SECONDS / 2)

            logger.info(f"Polling for investigations (IST) between {time_window_start.strftime('%Y-%m-%d %H:%M:%S')} and {time_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
            new_investigations = get_investigations_by_time_range(time_window_start, time_window_end)

            if new_investigations:
                logger.info(f"Found {len(new_investigations)} new investigations in the current time window.")
                process_investigations_in_parallel(new_investigations)
            else:
                logger.info("No new investigations found in the current time window.")
            
            # MODIFIED: Check status again before sleeping [cite: 1]
            if read_current_process_status() == 0:
                logger.info("CURRENT_PROCESS=0 found in status file. Exiting main loop.")
                break

            logger.info(f"Waiting for {POLLING_INTERVAL_SECONDS} seconds before next poll.")
            time.sleep(POLLING_INTERVAL_SECONDS)

    finally:
        if _mongo_client:
            _mongo_client.close()
            logger.info("MongoDB client for main process closed.")
        # MODIFIED: Ensure status is reset on exit: CURRENT_PROCESS=0 (not running) [cite: 1]
        write_current_process_status(0)

if __name__ == "__main__":
    try:
        setup_mongodb_indexes()
        main_loop()
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        # MODIFIED: Ensure status is set to 0 on unexpected error: CURRENT_PROCESS=0 [cite: 1]
        write_current_process_status(0)
        sys.exit(1)