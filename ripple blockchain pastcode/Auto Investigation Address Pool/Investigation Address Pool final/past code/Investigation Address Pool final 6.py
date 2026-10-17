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
FORMATTED_TRANSACTIONS_COLLECTION = "investigation_pool"
DONE_INVESTIGATIONS_COLLECTION = "inv_done" 

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
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("inv_id", ASCENDING)], unique=True)
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("start_date", ASCENDING)])
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("coin", ASCENDING)]) # Added index on coin for efficiency
        # Indexes for FORMATTED_TRANSACTIONS_COLLECTION adjusted to new schema
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("address", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("user_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("coin", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("timestamp", ASCENDING)])


        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("investigation_id", ASCENDING)], unique=True)
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("address", ASCENDING)])
        # IMPORTANT: Add indexes for inv_id and doc_type if you query by these fields
        db[BITCOIN_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[BITCOIN_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[BITCOIN_COLLECTION].create_index([("doc_type", ASCENDING)]) # New index for doc_type

        db[ETHEREUM_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("doc_type", ASCENDING)]) # New index for doc_type

        db[TRON_COLLECTION].create_index([("inv_id", ASCENDING)])
        db[TRON_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[TRON_COLLECTION].create_index([("doc_type", ASCENDING)]) # New index for doc_type
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
        # Assuming if it's an int/float, it's already in seconds, or needs conversion from ms.
        # Given the screenshot's start_date (162197240000), it's milliseconds.
        # This check ensures it's divided by 1000 if it appears to be milliseconds.
        if doc_field > 1_000_000_000_000: # Heuristic for milliseconds
             return int(doc_field / 1000)
        return int(doc_field)
    return 0

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

        logger.debug(f"Querying MongoDB for start_date (ms) between {start_timestamp_ms} (inclusive) and {end_timestamp_ms} (inclusive)")

        query = {"start_date": {"$gte": start_timestamp_ms, "$lte": end_timestamp_ms}}
        cursor = collection.find(query).sort("start_date", ASCENDING)

        for doc in cursor:
            required_fields = ["inv_id", "start_address", "coin"] # 'coin' is now a required field
            if all(field in doc for field in required_fields):
                investigation_start_date_unix = get_unix_timestamp_in_seconds(doc.get("start_date"))
                if investigation_start_date_unix == 0:
                    current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                    logger.warning(f"Investigation {doc.get('inv_id')} has missing or zero start_date. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                    investigation_start_date_unix = current_unix_timestamp

                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "timestamp_from_auto_inv": get_unix_timestamp_in_seconds(doc.get("record_timestamp")), # Kept for potential future use or if needed for other logic
                    "user_id": doc.get("user_id"),
                    "org_id": doc.get("org_id"),
                    "org": doc.get("org"),
                    "coin": doc["coin"], # Ensure coin is captured
                    "start_date_unix": investigation_start_date_unix # This is the start_date in Unix seconds
                }
                log_start_date_ist = datetime.fromtimestamp(investigation_start_date_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                investigations_to_process.append(investigation_details)
                logger.debug(f"Found investigation: {investigation_details['inv_id']} with start_date: {log_start_date_ist}")
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
            required_fields = ["inv_id", "start_address", "coin"] # 'coin' is now a required field
            if all(field in doc for field in required_fields):
                investigation_start_date_unix = get_unix_timestamp_in_seconds(doc.get("start_date"))
                if investigation_start_date_unix == 0:
                    current_unix_timestamp = int(datetime.now(timezone.utc).timestamp())
                    logger.warning(f"Investigation {doc.get('inv_id')} has missing or zero start_date. Using current UTC Unix timestamp ({current_unix_timestamp}) as fallback.")
                    investigation_start_date_unix = current_unix_timestamp

                investigation_details = {
                    "inv_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "timestamp_from_auto_inv": get_unix_timestamp_in_seconds(doc.get("record_timestamp")), # Kept for potential future use or if needed for other logic
                    "user_id": doc.get("user_id"),
                    "org_id": doc.get("org_id"),
                    "org": doc.get("org"),
                    "coin": doc["coin"], # Ensure coin is captured
                    "start_date_unix": investigation_start_date_unix # This is the start_date in Unix seconds
                }
                log_start_date_ist = datetime.fromtimestamp(investigation_start_date_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                past_investigations.append(investigation_details)
            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION} (missing inv_id, start_address, or coin): {doc}")
    except Exception as e:
        logger.error(f"Error fetching past investigations from {AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    return past_investigations

def process_transactions_for_address(
    inv_details: Dict[str, Any],
    target_address: str, # This is the 'start_address' from auto_investigations
    blockchain: str,
    tx_collection_name: str,
    field_mapping: Dict[str, str] # field_mapping is kept for potential future use or alternative query fields
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
        # Query by inv_id AND doc_type: "node"
        query = {"inv_id": investigation_id, "doc_type": "node"}
        logger.debug(f"[{investigation_id}] Querying {tx_collection_name} by inv_id: {investigation_id} and doc_type: 'node'")

        transactions_cursor = tx_collection.find(query)

        for doc in transactions_cursor:
            # Construct formatted_doc based on the user's requested format
            formatted_doc = {
                "inv_id": investigation_id,
                # Using 'start_date_unix' from inv_details as per user's request for 'timestamp'
                "timestamp": inv_details.get("start_date_unix"),
                "address": doc.get("address"), # This is the 'address' field from the Bitcoin/Ethereum/Tron doc
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"),
                "org": inv_details.get("org"),
                "coin": inv_details.get("coin"),
            }
            formatted_transactions.append(formatted_doc)

        logger.debug(f"[{investigation_id}] Found {len(formatted_transactions)} 'node' documents in {tx_collection_name} for inv_id: {investigation_id}")
        return formatted_transactions

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing {blockchain} transactions from {tx_collection_name}: {e}")
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
    address = inv_details.get("address") # This is the start_address / investigated address
    coin_type = inv_details.get("coin") # Get the coin type from investigation details

    if coin_type:
        coin_type = coin_type.upper() # Convert to uppercase for consistent comparison
    else:
        logger.warning(f"[{investigation_id_for_logging}] Investigation has no 'coin' type specified. Skipping transaction processing.")
        return False # Cannot process if coin type is unknown

    all_formatted_transactions = []

    # Field mappings are still defined but might not be used if you query by inv_id
    bitcoin_field_mapping = {"culprit_address": "culprit_address"}
    ethereum_field_mapping = {"culprit_address": "culprit_address"}
    tron_field_mapping = {"culprit_address": "culprit_address"}

    try:
        if coin_type == "BTC" or coin_type == "BITCOIN":
            logger.info(f"[{investigation_id_for_logging}] Processing Bitcoin transactions for address: {address}")
            btc_transactions = process_transactions_for_address(
                inv_details, address, "BTC", BITCOIN_COLLECTION, bitcoin_field_mapping
            )
            all_formatted_transactions.extend(btc_transactions)
        elif coin_type == "ETH" or coin_type == "ETHEREUM":
            logger.info(f"[{investigation_id_for_logging}] Processing Ethereum transactions for address: {address}")
            eth_transactions = process_transactions_for_address(
                inv_details, address, "ETH", ETHEREUM_COLLECTION, ethereum_field_mapping
            )
            all_formatted_transactions.extend(eth_transactions)
        elif coin_type == "TRX" or coin_type == "TRON": # This block will now specifically handle Tron
            logger.info(f"[{investigation_id_for_logging}] Processing Tron transactions for address: {address}")
            tron_transactions = process_transactions_for_address(
                inv_details, address, "TRX", TRON_COLLECTION, tron_field_mapping
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

    # Use multiprocessing.cpu_count() for max_workers, or a specific number if you want to limit
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count(), initializer=init_mongo_client) as executor:
        futures = []
        for investigation in investigations:
            investigation_id = investigation.get("inv_id")

            if not investigation_id:
                logger.warning(f"Skipping malformed investigation entry (missing inv_id): {investigation}")
                continue
            if not investigation.get("coin"): # Ensure coin is present before processing
                logger.warning(f"Skipping investigation {investigation_id} (missing coin type).")
                continue

            if is_investigation_done(investigation_id):
                logger.debug(f"Searching: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')} (already processed).")
            else:
                # Use a default timestamp if start_date_unix is missing, for logging clarity
                log_start_date_unix = investigation.get('start_date_unix') or int(datetime.now(timezone.utc).timestamp())
                log_start_date_ist = datetime.fromtimestamp(log_start_date_unix, tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                logger.info(f"Submitting for processing: inv_id: {investigation_id}, investigated_ID: {investigation.get('address')}, coin: {investigation.get('coin')}, start_date: {log_start_date_ist}")
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
                    log_start_date_ist = datetime.fromtimestamp(inv.get('start_date_unix'), tz=timezone.utc).astimezone(INDIA_TIMEZONE)
                    logger.info(f"  Found Investigation ID: {inv.get('inv_id')} (Coin: {inv.get('coin')}, start_date: {log_start_date_ist.strftime('%Y-%m-%d %H:%M:%S IST')})")
                process_investigations_in_parallel(new_investigations)

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