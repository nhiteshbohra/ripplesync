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
# INCREASED: Widening the time window to catch more events, especially those slightly in the future.
TIME_WINDOW_SECONDS = 300 # Changed from 60 to 300 (5 minutes)

# Define India Standard Time (IST) timezone
INDIA_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Setup logging
# Changed default logging level to INFO to reduce verbose output
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
            sys.exit(1) # Exit the process if client cannot be initialized

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
        # Use a temporary client for index setup, as the main _mongo_client might not be fully initialized yet
        # or we don't want index setup to block the main client if it's already in use.
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        # Create indexes if they don't exist
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("inv_id", ASCENDING)], unique=True)
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("start_date", ASCENDING)])
        # Index for formatted_transaction will now reflect the new field names
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("culprit_ID", ASCENDING)]) # New index for investigated address
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("address", ASCENDING)]) # New index for actual culprit address from tx
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("case_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("blockchain", ASCENDING)])

        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("investigation_id", ASCENDING)], unique=True)
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("address", ASCENDING)])
        db[BITCOIN_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[TRON_COLLECTION].create_index([("culprit_address", ASCENDING)])
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

# --- Done Investigations Management ---
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
        # Use IST for completed_at timestamp
        collection.update_one(
            {"investigation_id": investigation_id},
            {"$set": {"investigation_id": investigation_id, "completed_at": datetime.now(INDIA_TIMEZONE), "address": address}},
            upsert=True
        )
        logger.debug(f"Investigation {investigation_id} for address {address} marked as done.")
    except Exception as e:
        logger.error(f"Error marking investigation {investigation_id} for address {address} as done: {e}")

# --- Investigation System Fetching ---
def get_investigations_by_time_range(start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
    """
    Fetches investigations from the crypto_forensics.auto_investigations collection
    within a specific time range for the 'start_date' field (Unix timestamp).
    """
    investigations_to_process = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
        # Convert datetime objects (which are timezone-aware in IST) to Unix timestamps (seconds).
        # .timestamp() method correctly converts the datetime to UTC timestamp before returning.
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        logger.debug(f"Querying MongoDB for start_date between {start_timestamp} (inclusive) and {end_timestamp} (inclusive)")

        query = {"start_date": {"$gte": start_timestamp, "$lte": end_timestamp}}
        # Sort by start_date to process chronologically within the window
        cursor = collection.find(query).sort("start_date", ASCENDING)

        for doc in cursor:
            required_fields = ["inv_id", "start_address", "start_date"]
            if all(field in doc for field in required_fields):
                # Convert Unix timestamp from DB (assumed to be UTC) to IST datetime object for internal use
                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"], # This is the start_address/investigated address
                    "timestamp": doc.get("timestamp"), # This is the investigation creation timestamp
                    "user_id": doc.get("user_id"),
                    "org_id": doc.get("org_id"),
                    "org": doc.get("org"),
                    "coin": doc.get("coin"),
                    "start_date": datetime.fromtimestamp(doc["start_date"], tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                }
                investigations_to_process.append(investigation_details)
                logger.debug(f"Found investigation: {investigation_details['inv_id']} with start_date: {investigation_details['start_date']}")
            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION}: {doc}")
    except Exception as e:
        logger.error(f"Error fetching investigations by time range from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return investigations_to_process

def get_past_investigations() -> List[Dict[str, Any]]:
    """
    Fetches all investigations from crypto_forensics.auto_investigations
    where 'start_date' is in the past (relative to current IST), sorted by 'start_date' ascending.
    'start_date' in DB is assumed to be a Unix timestamp.
    """
    past_investigations = []
    collection = get_db_collection(AUTO_INVESTIGATIONS_COLLECTION)
    try:
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
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"], # This is the start_address/investigated address
                    "timestamp": doc.get("timestamp"), # This is the investigation creation timestamp
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
        logger.error(f"Error fetching past investigations from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return past_investigations

# --- Transaction Processing Logic (ADAPTED FOR MULTIPLE BLOCKCHAINS) ---
def process_transactions_for_address(
    inv_details: Dict[str, Any],
    target_address: str, # This is the 'start_address' from auto_investigations
    blockchain: str,
    tx_collection_name: str,
    field_mapping: Dict[str, str]
) -> List[Dict[str, Any]]: # Returns a list of formatted transactions
    """
    Finds and processes transactions for a given address in a specific blockchain collection.
    Returns a list of formatted transactions to be bulk inserted,
    containing the fields specified in your example output, including the new renamings.
    """
    formatted_transactions = []
    tx_collection = get_db_collection(tx_collection_name)
    investigation_id = inv_details.get("inv_id")

    try:
        from_field_in_db = field_mapping.get("from_address")
        if not from_field_in_db:
            logger.error(f"[{investigation_id}] Missing 'from_address' mapping for {blockchain}. Cannot process transactions.")
            return []

        query = {from_field_in_db: target_address}

        transactions_cursor = tx_collection.find(query)

        for doc in transactions_cursor:
            # Construct the document with the newly requested field names
            formatted_doc = {
                "inv_id": investigation_id,
                "culprit_ID": target_address, # Renamed 'address' (investigated address) to 'culprit_ID'
                "address": doc.get("address"), # Renamed 'culprit_address' (from tx doc) to 'address'
                "timestamp": inv_details.get("start_date"),
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"),
                "org": inv_details.get("org"),
                "coin": inv_details.get("coin"),
                "last_updated_at": datetime.now(INDIA_TIMEZONE),
            }

            formatted_transactions.append(formatted_doc)

        return formatted_transactions

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing {blockchain} transactions for {target_address} from {tx_collection_name}: {e}")
        return []


def process_investigation(inv_details: Dict[str, Any]) -> bool:
    """
    Processes an individual investigation across all relevant blockchain collections.
    Returns True if the processing completed without critical errors, False otherwise.
    Also logs the total number of transactions found for the address.
    """
    overall_processing_successful = True
    investigation_id_for_logging = inv_details.get("inv_id")
    address = inv_details.get("address") # This is the start_address / investigated address

    all_formatted_transactions = []

    # Field mappings remain the same as they map internal DB fields
    bitcoin_field_mapping = {
        "from_address": "culprit_address"
    }

    ethereum_field_mapping = {
        "from_address": "culprit_address"
    }

    tron_field_mapping = {
        "from_address": "culprit_address"
    }

    try:
        btc_transactions = process_transactions_for_address(
            inv_details, address, "Bitcoin", BITCOIN_COLLECTION, bitcoin_field_mapping
        )
        all_formatted_transactions.extend(btc_transactions)
    except Exception as e:
        logger.error(f"[{investigation_id_for_logging}] Critical error processing Bitcoin transactions: {e}")
        overall_processing_successful = False

    try:
        eth_transactions = process_transactions_for_address(
            inv_details, address, "Ethereum", ETHEREUM_COLLECTION, ethereum_field_mapping
        )
        all_formatted_transactions.extend(eth_transactions)
    except Exception as e:
        logger.error(f"[{investigation_id_for_logging}] Critical error processing Ethereum transactions: {e}")
        overall_processing_successful = False

    try:
        tron_transactions = process_transactions_for_address(
            inv_details, address, "Tron", TRON_COLLECTION, tron_field_mapping
        )
        all_formatted_transactions.extend(tron_transactions)
    except Exception as e:
        logger.error(f"[{investigation_id_for_logging}] Critical error processing Tron transactions: {e}")
        overall_processing_successful = False

    # Perform bulk insert after processing all blockchains for this investigation
    if all_formatted_transactions:
        save_formatted_transactions_bulk(all_formatted_transactions)

    logger.info(f"[{investigation_id_for_logging}] Investigated ID: {address} has {len(all_formatted_transactions)} connections.")

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

    # Use ProcessPoolExecutor with an initializer to set up MongoDB client once per process
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count(), initializer=init_mongo_client) as executor:
        futures = []
        for investigation in investigations:
            investigation_id = investigation.get("inv_id")

            if not investigation_id:
                logger.warning(f"Skipping malformed investigation entry: {investigation}")
                continue

            if is_investigation_done(investigation_id):
                logger.debug(f"Searching: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')} (already processed).")
            else:
                logger.info(f"Processing: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')}, start_date: {investigation.get('start_date')}")
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
    global _mongo_client # Declare intent to modify the global variable in the main process

    # Initialize MongoDB client for the main process
    init_mongo_client()
    logger.info("MongoDB client initialized for main process.")

    try:
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
            current_time_ist = datetime.now(INDIA_TIMEZONE)
            time_window_start = current_time_ist - timedelta(seconds=TIME_WINDOW_SECONDS / 2)
            time_window_end = current_time_ist + timedelta(seconds=TIME_WINDOW_SECONDS / 2)

            logger.info(f"Polling for investigations with start_date (IST) between {time_window_start.strftime('%Y-%m-%d %H:%M:%S')} and {time_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
            new_investigations = get_investigations_by_time_range(time_window_start, time_window_end)

            if not new_investigations:
                logger.info(f"No new investigations found in the current time window. Waiting for {POLLING_INTERVAL_SECONDS} seconds.")
            else:
                logger.info(f"Found {len(new_investigations)} new investigations in the current time window.")
                for inv in new_investigations:
                    logger.info(f"  Found Investigation ID: {inv.get('inv_id')} (start_date: {inv.get('start_date').strftime('%Y-%m-%d %H:%M:%S IST')})")
                process_investigations_in_parallel(new_investigations)

            time.sleep(POLLING_INTERVAL_SECONDS)

    finally:
        # Ensure the main process's MongoDB client is closed
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