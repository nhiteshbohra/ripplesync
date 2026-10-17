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
from dotenv import load_dotenv # Import load_dotenv

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
LAST_RUN_TIMESTAMP_FILE = os.getenv("LAST_RUN_TIMESTAMP_FILE") # File to store the last successful run timestamp 

# Setup logging
# Clear existing handlers to prevent duplicate output if re-configured
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        # logging.StreamHandler(sys.stdout) # Removed to suppress terminal output
    ]
)
logger = logging.getLogger(__name__)

# Global variable to hold the MongoDB client for each process (main and pool workers)
_mongo_client = None

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
            sys.exit(1)

def get_db_collection(collection_name: str):
    """
    Returns the specified MongoDB collection from the globally managed client.
    Assumes _mongo_client has been initialized in the current process.
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
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("start_date", ASCENDING)])
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("coin", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("address", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("user_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("org_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("coin", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("timestamp", ASCENDING)])

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

def save_formatted_transactions_bulk(transactions_data: List[Dict[str, Any]]) -> None:
    """
    Saves multiple transactions in a single bulk insert operation.
    """
    if not transactions_data:
        return
    collection = get_db_collection(FORMATTED_TRANSACTIONS_COLLECTION)
    try:
        collection.insert_many(transactions_data)
        logger.debug(f"Successfully inserted {len(transactions_data)} formatted transactions.")
    except Exception as e:
        logger.error(f"Error saving formatted transactions in bulk: {e}")

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
            {"$set": {"investigation_id": investigation_id, "completed_at": datetime.now(INDIA_TIMEZONE), "address": address}},
            upsert=True
        )
        logger.debug(f"Investigation {investigation_id} for address {address} marked as done.")
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
        if doc_field > 1_000_000_000_000:
            return int(doc_field / 1000)
        return int(doc_field)
    return 0

def get_org_id_for_user(user_id: str) -> Optional[str]:
    """
    Queries the BLOCKCHAIN_USERS_COLLECTION to find the org_id for a given user_id.
    Returns the org_id if found, None otherwise.
    """
    collection = get_db_collection(BLOCKCHAIN_USERS_COLLECTION)
    try:
        doc = collection.find_one({"user_id": user_id}, {"org_id": 1, "_id": 0})
        if doc and "org_id" in doc:
            logger.debug(f"Found org_id '{doc['org_id']}' for user_id '{user_id}'.")
            return doc["org_id"]
        else:
            logger.warning(f"No org_id found for user_id '{user_id}' in {BLOCKCHAIN_USERS_COLLECTION}.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving org_id for user_id '{user_id}': {e}")
        return None

def get_investigations_by_time_range(start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Fetches investigations from the crypto_forensics.auto_investigations collection
    within a specific time range for the 'start_date' field (Unix timestamp in milliseconds).
    """
    investigations_to_process = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
        start_timestamp_ms = int(start_time.timestamp() * 1000)
        end_timestamp_ms = int(end_time.timestamp() * 1000)

        query = {"start_date": {"$gte": start_timestamp_ms, "$lte": end_timestamp_ms}}
        cursor = collection.find(query).sort("start_date", ASCENDING)

        for doc in cursor:
            required_fields = ["inv_id", "start_address", "coin"]
            if all(field in doc for field in required_fields):
                investigation_start_date_unix = get_unix_timestamp_in_seconds(doc.get("start_date"))
                if investigation_start_date_unix == 0:
                    current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                    logger.warning(f"Investigation {doc.get('inv_id')} has missing or zero start_date. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                    investigation_start_date_unix = current_unix_timestamp

                user_id = doc.get("user_id")
                org_id = None
                if user_id:
                    org_id = get_org_id_for_user(user_id)

                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "user_id": user_id,
                    "coin": doc["coin"],
                    "start_date_unix": investigation_start_date_unix,
                    "org_id": org_id # Include org_id here
                }
                investigations_to_process.append(investigation_details)
            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION} (missing inv_id, start_address, or coin): {doc}")
    except Exception as e:
        logger.error(f"Error fetching investigations by time range from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return investigations_to_process

def get_past_investigations() -> List[Dict[str, Any]]:
    """
    Fetches all investigations from crypto_forensics.auto_investigations
    where 'start_date' is in the past (relative to current IST), sorted by 'start_date' ascending.
    'start_date' in DB is assumed to be a Unix timestamp in milliseconds.
    """
    past_investigations = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
        current_time_ist = datetime.now(INDIA_TIMEZONE)
        current_timestamp_ms = int(current_time_ist.timestamp() * 1000)

        query = {"start_date": {"$lte": current_timestamp_ms}}
        cursor = collection.find(query).sort("start_date", ASCENDING)

        for doc in cursor:
            required_fields = ["inv_id", "start_address", "coin"]
            if all(field in doc for field in required_fields):
                investigation_start_date_unix = get_unix_timestamp_in_seconds(doc.get("start_date"))
                if investigation_start_date_unix == 0:
                    current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                    logger.warning(f"Investigation {doc.get('inv_id')} has missing or zero start_date. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                    investigation_start_date_unix = current_unix_timestamp

                user_id = doc.get("user_id")
                org_id = None
                if user_id:
                    org_id = get_org_id_for_user(user_id)

                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "user_id": user_id,
                    "coin": doc["coin"],
                    "start_date_unix": investigation_start_date_unix,
                    "org_id": org_id # Include org_id here
                }
                past_investigations.append(investigation_details)
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
                "timestamp": inv_details.get("start_date"),
                "address": doc.get("address"),
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"), # Include org_id in formatted transaction
                "coin": inv_details.get("coin"),
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
    Returns True if the processing completed without critical errors, False otherwise.
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

    except Exception as e:
        logger.critical(f"[{investigation_id_for_logging}] An unexpected critical error occurred during coin-specific transaction processing: {e}")
        overall_processing_successful = False

    if all_formatted_transactions:
        save_formatted_transactions_bulk(all_formatted_transactions)

    logger.info(f"[{investigation_id_for_logging}] Investigated ID: {address} (Coin: {coin_type}) has {len(all_formatted_transactions)} total 'node' connections.")

    return overall_processing_successful

def process_and_mark_done_for_pool(inv_details: Dict[str, Any]):
    """
    Wrapper function to process an investigation and mark it as done,
    regardless of whether transactions were found, as long as processing didn't critically fail.
    """
    investigation_id = inv_details.get("inv_id")
    address = inv_details.get("address")

    processing_status = process_investigation(inv_details)

    if processing_status:
        mark_investigation_as_done(investigation_id, address)
    else:
        logger.error(f"Processing for Investigation ID {investigation_id} encountered critical errors. It will NOT be marked as done and will be re-attempted in the next cycle.")

def process_investigations_in_parallel(investigations: List[Dict[str, Any]]):
    """Helper function to run investigations in parallel using ProcessPoolExecutor."""
    if not investigations:
        logger.info("No investigations to process in parallel.")
        return

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
                logger.debug(f"Searching: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')} (already processed).")
            else:
                log_start_date_unix = investigation.get('start_date_unix') or int(datetime.now(timezone.utc).timestamp())
                log_start_date_ist = datetime.fromtimestamp(log_start_date_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                org_id_log = f", org_id: {investigation.get('org_id')}" if investigation.get('org_id') else ""
                logger.info(f"Submitting for processing: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')}, coin: {investigation.get('coin')}, start_date: {log_start_date_ist}{org_id_log}")
                futures.append(executor.submit(process_and_mark_done_for_pool, investigation))

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"A task in the process pool failed: {e}")

def read_last_run_timestamp() -> Optional[datetime]:
    """Reads the last successful run timestamp from a file."""
    if os.path.exists(LAST_RUN_TIMESTAMP_FILE):
        try:
            with open(LAST_RUN_TIMESTAMP_FILE, 'r') as f:
                timestamp_str = f.read().strip()
                return datetime.fromisoformat(timestamp_str).astimezone(INDIA_TIMEZONE)
        except Exception as e:
            logger.error(f"Error reading last run timestamp from {LAST_RUN_TIMESTAMP_FILE}: {e}")
    return None

def write_last_run_timestamp(timestamp: datetime):
    """Writes the current successful run timestamp to a file."""
    try:
        with open(LAST_RUN_TIMESTAMP_FILE, 'w') as f:
            f.write(timestamp.isoformat())
        logger.info(f"Last successful run timestamp updated to: {timestamp.isoformat()}")
    except Exception as e:
        logger.error(f"Error writing last run timestamp to {LAST_RUN_TIMESTAMP_FILE}: {e}")

def main_loop():
    """
    Main loop to first process all past investigations, then continuously poll
    for new investigations within a time window, ensuring each is processed once.
    """
    global _mongo_client

    init_mongo_client()
    logger.info("MongoDB client initialized for main process.")

    last_run_time = read_last_run_timestamp()
    if last_run_time:
        logger.info(f"Previous successful run recorded at: {last_run_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
    else:
        logger.info("No previous successful run timestamp found.")

    try:
        setup_mongodb_indexes()

        logger.info("Starting initial fast processing of investigations with past start_date (IST)...")
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
            time_window_start = current_time_ist - timedelta(seconds=TIME_WINDOW_SECONDS / 2)
            time_window_end = current_time_ist + timedelta(seconds=TIME_WINDOW_SECONDS / 2)

            log_time_window_start_str = time_window_start.strftime('%Y-%m-%d %H:%M:%S IST')
            log_time_window_end_str = time_window_end.strftime('%Y-%m-%d %H:%M:%S IST')

            logger.info(f"Polling for investigations with start_date (IST) between {log_time_window_start_str} and {log_time_window_end_str}")
            new_investigations = get_investigations_by_time_range(time_window_start, time_window_end)

            if not new_investigations:
                logger.info(f"No new investigations found in the current time window. Waiting for {POLLING_INTERVAL_SECONDS} seconds.")
            else:
                logger.info(f"Found {len(new_investigations)} new investigations in the current time window.")
                for inv in new_investigations:
                    log_start_date_unix = inv.get('start_date_unix')
                    # Ensure start_date_unix is not None before converting
                    if log_start_date_unix is not None:
                        log_start_date_ist = datetime.fromtimestamp(log_start_date_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                        org_id_log = f", org_id: {inv.get('org_id')}" if inv.get('org_id') else ""
                        logger.info(f"  Found Investigation ID: {inv.get('inv_id')} (Coin: {inv.get('coin')}, start_date: {log_start_date_ist.strftime('%Y-%m-%d %H:%M:%S IST')}){org_id_log}")
                    else:
                        logger.warning(f"  Found Investigation ID: {inv.get('inv_id')} (Coin: {inv.get('coin')}) has no valid start_date_unix.")
                process_investigations_in_parallel(new_investigations)

            write_last_run_timestamp(datetime.now(INDIA_TIMEZONE)) # Update timestamp after each cycle
            time.sleep(POLLING_INTERVAL_SECONDS)

    finally:
        if _mongo_client:
            _mongo_client.close()
            logger.info("MongoDB client for main process closed.")

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        sys.exit(1)