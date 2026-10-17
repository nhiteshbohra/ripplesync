import multiprocessing
import logging
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timezone
import sys
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ProcessPoolExecutor

# Configuration
MONGO_URI = "mongodb://localhost:27017/"  # PLEASE ENSURE THIS IS CORRECT - Added trailing slash for clarity
DB_NAME = "try"  # Changed to crypto_forensics database
AUTO_INVESTIGATIONS_COLLECTION = "crypto_forensics.auto_investigations" # Collection for active investigations
FORMATTED_TRANSACTIONS_COLLECTION = "formatted_transactions"
DONE_INVESTIGATIONS_COLLECTION = "done_investigations"

# Blockchain specific collections
BITCOIN_COLLECTION = "crypto_forensics.bitcoin_investigations"
ETHEREUM_COLLECTION = "crypto_forensics.ethereum_investigation_graph_result"
TRON_COLLECTION = "crypto_forensics.tron_investigation_graph_result"
# Polling Interval (in seconds)
POLLING_INTERVAL_SECONDS = 10  # Runs every 10 seconds

# Setup logging
# For multiprocessing, it's often better to configure logging to be process-safe
# For simplicity, we'll keep basic config, but for production, consider QueueHandler
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
        
        # Indexes for AUTO_INVESTIGATIONS_COLLECTION
        db[AUTO_INVESTIGATIONS_COLLECTION].create_index([("inv_id", ASCENDING)], unique=True)
        logger.info(f"Indexes ensured for {AUTO_INVESTIGATIONS_COLLECTION} collection.")

        # Indexes for FORMATTED_TRANSACTIONS_COLLECTION
        # Removed unique constraint on hash if using insert_one for all documents
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("hash", ASCENDING)], sparse=True) 
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("from_address", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("investigation_id", ASCENDING)])
        db[FORMATTED_TRANSACTIONS_COLLECTION].create_index([("blockchain", ASCENDING)])
        logger.info(f"Indexes ensured for {FORMATTED_TRANSACTIONS_COLLECTION} collection.")

        # Indexes for DONE_INVESTIGATIONS_COLLECTION
        # Add index for 'address' in done_investigations if it's frequently queried
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("investigation_id", ASCENDING)], unique=True)
        db[DONE_INVESTIGATIONS_COLLECTION].create_index([("address", ASCENDING)]) # New index
        logger.info(f"Indexes ensured for {DONE_INVESTIGATIONS_COLLECTION} collection.")

        # Indexes for blockchain-specific collections
        # Assuming 'culprit_address' is the 'from' field across all blockchain collections
        db[BITCOIN_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[ETHEREUM_COLLECTION].create_index([("culprit_address", ASCENDING)])
        db[TRON_COLLECTION].create_index([("culprit_address", ASCENDING)])
        logger.info(f"Indexes ensured for blockchain specific collections using 'culprit_address'.")

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
        # Changed from update_one to insert_one to save all documents
        collection.insert_one(transaction_data)
        logger.info(f"Successfully inserted formatted transaction with hash: {transaction_data.get('hash', 'N/A')} for Investigation ID: {transaction_data.get('inv_id', 'N/A')} on {transaction_data.get('blockchain', 'N/A')}")
    except Exception as e:
        logger.error(f"Failed to insert formatted transaction with hash {transaction_data.get('hash', 'N/A')} on blockchain {transaction_data.get('blockchain', 'N/A')}: {e}")
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
        return False  # Assume not done on error to retry
    finally:
        client.close()

def mark_investigation_as_done(investigation_id: str, address: str) -> None: # Added 'address' parameter
    """
    Marks an investigation ID as done by adding it to the DONE_INVESTIGATIONS_COLLECTION.
    Includes the address for which the investigation was performed.
    """
    collection, client = get_mongo_collection(DB_NAME, DONE_INVESTIGATIONS_COLLECTION)
    try:
        collection.update_one(
            {"investigation_id": investigation_id},
            {"$set": {"investigation_id": investigation_id, "completed_at": datetime.now(timezone.utc), "address": address}}, # Added 'address'
            upsert=True
        )
        logger.info(f"Investigation ID {investigation_id} for address {address} marked as done.")
    except Exception as e:
        logger.error(f"Error marking investigation {investigation_id} for address {address} as done: {e}")
    finally:
        client.close()

# --- Investigation System Fetching ---
def get_active_investigations() -> List[Dict[str, Any]]:
    """
    Fetches active investigations from the crypto_forensics.auto_investigations collection synchronously,
    including user_id, org_id, org, and coin.
    """
    active_investigations_from_db = []
    collection, client = None, None
    try:
        collection, client = get_mongo_collection(DB_NAME, AUTO_INVESTIGATIONS_COLLECTION)
        cursor = collection.find({}) # Fetch all active investigations
        for doc in cursor:
            # Ensure required fields exist, gracefully handle missing optional ones
            required_fields = ["inv_id", "start_address"]
            if all(field in doc for field in required_fields):
                investigation_details = {
                    "investigation_id": doc["inv_id"],
                    "address": doc["start_address"],
                    "investigation_timestamp": doc.get("timestamp"), # Investigation timestamp
                    "user_id": doc.get("user_id"),
                    "org_id": doc.get("org_id"),
                    "org": doc.get("org"),
                    "coin": doc.get("coin")
                }
                active_investigations_from_db.append(investigation_details)
            else:
                logger.warning(f"Skipping malformed investigation document in {AUTO_INVESTIGATIONS_COLLECTION}: {doc}")
    except Exception as e:
        logger.error(f"Error fetching active investigations from {DB_NAME}.{AUTO_INVESTIGATIONS_COLLECTION}: {e}")
    finally:
        if client:
            client.close()
    return active_investigations_from_db

# --- Transaction Processing Logic (ADAPTED FOR MULTIPLE BLOCKCHAINS) ---
def process_transactions_for_address(
    inv_details: Dict[str, Any], # Now passing the whole investigation details dict
    target_address: str,
    blockchain: str,
    tx_collection_name: str,
    from_field: str, # Field in the source collection that holds the 'from' address
    to_field: str,   # Field in the source collection that holds the 'to' address
    hash_field: str, # Field for transaction hash
    value_field: str, # Field for value
    fee_field: str, # Field for fee
    timestamp_field: str, # Field for timestamp/date
    destination_tag_field: Optional[str] = None # Optional field for destination tag
) -> int: # Returns the number of transactions processed
    """
    Finds and processes transactions for a given address in a specific blockchain collection.
    This function is now synchronous.
    """
    tx_collection, client = None, None
    total_transactions_processed = 0
    investigation_id = inv_details.get("investigation_id") # Extract inv_id for logging
    try:
        tx_collection, client = get_mongo_collection(DB_NAME, tx_collection_name)
        logger.info(f"[{investigation_id}] Searching for {blockchain} transactions from address: {target_address} in {tx_collection_name}")

        query = {from_field: target_address}
        
        transactions_cursor = tx_collection.find(query)

        for doc in transactions_cursor:
            total_transactions_processed += 1

            # Build formatted_doc based on the desired output format
            formatted_doc = {
                "inv_id": investigation_id,
                "address": target_address, # This is the "culprit_address" used for querying
                "timestamp": doc.get(timestamp_field), # Transaction timestamp
                "user_id": inv_details.get("user_id"),
                "org_id": inv_details.get("org_id"),
                "org": inv_details.get("org"),
                "coin": inv_details.get("coin"), # Coin from the investigation details
                "hash": doc.get(hash_field), # Transaction hash
                "blockchain": blockchain,
                "last_updated_at": datetime.now(timezone.utc), # When this record was last updated
            }
            
            # Add transaction-specific optional fields if they exist and are not None
            if to_field and doc.get(to_field) is not None:
                formatted_doc["to_addresses"] = doc.get(to_field)
            if value_field and doc.get(value_field) is not None:
                formatted_doc["value"] = doc.get(value_field)
            if fee_field and doc.get(fee_field) is not None:
                formatted_doc["fee"] = doc.get(fee_field)
            if destination_tag_field and doc.get(destination_tag_field) is not None:
                formatted_doc["DestinationTag"] = doc.get(destination_tag_field)

            save_formatted_transaction(formatted_doc)

        logger.info(f"[{investigation_id}] Total {blockchain} transactions processed and saved to {FORMATTED_TRANSACTIONS_COLLECTION} for '{target_address}': {total_transactions_processed}")

        if total_transactions_processed == 0:
            logger.warning(f"[{investigation_id}] No {blockchain} transactions found for address: {target_address} in {tx_collection_name}.")
        
        return total_transactions_processed

    except Exception as e:
        logger.error(f"[{investigation_id}] An error occurred while processing {blockchain} transactions for {target_address} from {tx_collection_name}: {e}")
        return 0 # Indicate no transactions processed due to error
    finally:
        if client:
            client.close()


def process_investigation(inv_details: Dict[str, Any]) -> bool: # Now accepts inv_details dict
    """
    Processes an individual investigation across all relevant blockchain collections.
    This function is now synchronous and will be run in a separate process.
    Returns True if at least one transaction was processed successfully from any chain, False otherwise.
    """
    overall_success = False
    investigation_id = inv_details.get("investigation_id")
    address = inv_details.get("address")
    
    logger.info(f"[{investigation_id}] Starting investigation for address: {address}")

    # Process Bitcoin transactions
    try:
        btc_processed = process_transactions_for_address(
            inv_details, address, "Bitcoin", BITCOIN_COLLECTION, # Pass inv_details
            from_field="culprit_address", # Changed to 'culprit_address'
            to_field="output_address",   # <<< VERIFY THIS FIELD NAME
            hash_field="tx_hash",        # <<< VERIFY THIS FIELD NAME
            value_field="value",         # <<< VERIFY THIS FIELD NAME
            fee_field="fee",             # <<< VERIFY THIS FIELD NAME
            timestamp_field="time"       # <<< VERIFY THIS FIELD NAME
        )
        if btc_processed > 0:
            overall_success = True
    except Exception as e:
        logger.error(f"[{investigation_id}] Error processing Bitcoin transactions: {e}")

    # Process Ethereum transactions
    try:
        eth_processed = process_transactions_for_address(
            inv_details, address, "Ethereum", ETHEREUM_COLLECTION, # Pass inv_details
            from_field="culprit_address", # Changed to 'culprit_address'
            to_field="to",               # <<< VERIFY THIS FIELD NAME
            hash_field="hash",           # <<< VERIFY THIS FIELD NAME
            value_field="value",         # <<< VERIFY THIS FIELD NAME
            fee_field="gasUsed",         # <<< VERIFY THIS FIELD NAME (often gasUsed for Ether, check your schema)
            timestamp_field="timestamp"  # <<< VERIFY THIS FIELD NAME
        )
        if eth_processed > 0:
            overall_success = True
    except Exception as e:
        logger.error(f"[{investigation_id}] Error processing Ethereum transactions: {e}")

    # Process Tron transactions
    try:
        tron_processed = process_transactions_for_address(
            inv_details, address, "Tron", TRON_COLLECTION, # Pass inv_details
            from_field="culprit_address",   # Changed to 'culprit_address'
            to_field="to_address",       # <<< VERIFY THIS FIELD NAME
            hash_field="hash",           # <<< VERIFY THIS FIELD NAME
            value_field="amount",        # <<< VERIFY THIS FIELD NAME
            fee_field="fee",             # <<< VERIFY THIS FIELD NAME
            timestamp_field="timestamp", # <<< VERIFY THIS FIELD NAME
            destination_tag_field="DestinationTag" # <<< VERIFY THIS FIELD NAME (Optional)
        )
        if tron_processed > 0:
            overall_success = True
    except Exception as e:
        logger.error(f"[{investigation_id}] Error processing Tron transactions: {e}")

    return overall_success

# Helper function to be run in a separate process by ProcessPoolExecutor
def process_and_mark_done_for_pool(inv_details: Dict[str, Any]): # Now accepts inv_details dict
    """
    Wrapper function to process an investigation and mark it as done.
    This function will be submitted to the ProcessPoolExecutor.
    """
    investigation_id = inv_details.get("investigation_id")
    address = inv_details.get("address") # Get address from inv_details
    success = process_investigation(inv_details) # Pass inv_details
    if success:
        mark_investigation_as_done(investigation_id, address) # Pass address to mark_investigation_as_done
    else:
        logger.error(f"Processing failed for Investigation ID {investigation_id}. Will retry in next cycle (no transactions processed or an error occurred).")


def main_loop():
    """
    Main loop to periodically check for investigations and process addresses,
    ensuring each investigation is processed only once using multiprocessing.
    """
    setup_mongodb_indexes()  # Setup indexes for all collections once

    while True:
        logger.info(f"--- Starting new polling cycle for investigations (every {POLLING_INTERVAL_SECONDS} seconds) ---")
        active_investigations = get_active_investigations()

        if not active_investigations:
            logger.info("No active investigations found in auto_investigations collection. Waiting for next cycle.")
        else:
            logger.info(f"Found {len(active_investigations)} active investigation(s).")

            # Use ProcessPoolExecutor to manage a pool of worker processes
            # max_workers can be adjusted. multiprocessing.cpu_count() is a common choice.
            with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                futures = []
                for investigation in active_investigations:
                    investigation_id = investigation.get("investigation_id")
                    
                    if not investigation_id:
                        logger.warning(f"Skipping malformed investigation entry: {investigation}")
                        continue

                    if is_investigation_done(investigation_id):
                        logger.info(f"Investigation ID {investigation_id} already processed. Skipping.")
                    else:
                        logger.info(f"Initiating processing for NEW Investigation ID: {investigation_id}, Address: {investigation.get('address')}")
                        
                        # Submit the task to the process pool
                        futures.append(executor.submit(process_and_mark_done_for_pool, investigation)) # Pass the entire investigation dict
                
                # Wait for all submitted tasks to complete and handle potential exceptions
                for future in futures:
                    try:
                        future.result() # This will re-raise any exceptions that occurred in the child process
                    except Exception as e:
                        logger.error(f"A task in the process pool failed: {e}")

        logger.info(f"--- Polling cycle complete. Waiting {POLLING_INTERVAL_SECONDS} seconds ---")
        time.sleep(POLLING_INTERVAL_SECONDS) # Use time.sleep for synchronous delay


if __name__ == "__main__":
    # Ensure your MongoDB is running and accessible at MONGO_URI.
    # The script will now read active investigations from 'crypto_forensics.auto_investigations'
    # and search for transactions in 'crypto_forensics.bitcoin_investigations',
    # 'crypto_forensics.ethereum_investigation_graph_result', and
    # 'crypto_forensics.tron_investigation_graph_result'.
    #
    # The 'from' address field in all blockchain specific collections is now assumed to be 'culprit_address'.
    # You MUST verify the other field names used in process_transactions_for_address calls
    # (e.g., 'output_address', 'tx_hash' for Bitcoin; 'to', 'hash' for Ethereum; etc.)
    # to match the exact schema of your respective blockchain investigation collections.
    # If these field names are incorrect, the script will not find the data.
    #
    # IMPORTANT: The save_formatted_transaction function now uses insert_one.
    # This means if your source collections return multiple documents with the
    # same 'hash' (e.g., different aspects of the same transaction), they will
    # all be inserted as separate documents into 'formatted_transactions'.
    # If you intend for 'formatted_transactions' to only hold unique transactions
    # based on their hash, you would need a more sophisticated de-duplication
    # logic or ensure your source data doesn't produce duplicates for the
    # same logical transaction.

    try:
        main_loop() # Direct call to the synchronous main loop
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        sys.exit(1)