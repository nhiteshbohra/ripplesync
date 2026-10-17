import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import AccountTx
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any
import time

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xrpl_data"
ACCOUNT_TX_COLLECTION = "account_transactions" # Collection for account-specific transactions
META_MEMOS_COLLECTION = "meta_memos"
FAILED_TX_COLLECTION = "failed_account_transactions" # New collection for failed account transaction fetches
SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233"
]
MAX_RETRIES = 10 # Increased retries for more robustness
REQUEST_TIMEOUT = 30 # Increased timeout
BATCH_CONCURRENCY = 4 # Concurrency for async tasks within a single XRPL client connection (for potential future parallel page fetches)
RECONNECT_DELAY_BASE = 1 # Base delay for exponential backoff
TRANSACTION_LIMIT_PER_REQUEST = 200 # Max transactions per account_tx request

# Setup logging to file
# All log messages will now be written to 'xrpl_account_fetcher.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("xrpl_account_fetcher.log", "w"), # Log to a file
        logging.StreamHandler() # Also log to console
    ]
)
logger = logging.getLogger(__name__)

# Global variables for MongoDB client
# These will be initialized once in the main process
mongo_client = None
db = None

# Define a mapping for field renames
# This ensures consistency with the original ledger fetching script's output
FIELD_RENAMES = {
    "ledger_index": "block_number",
    "ledger_hash": "block_hash",
    "Account": "from_address",
    "Destination": "to_address",
    # Add more renames as needed for other fields
}

# Schema definitions (for validation/type hinting purposes)
# These are the fields we expect directly at the top level of the transaction document
# This schema helps in validating and type hinting the flattened transaction data.
TX_SCHEMA_FIELDS = {
    'TransactionType': str,
    'from_address': str, # Renamed from 'Account'
    'hash': str,
    'to_address': str,   # Renamed from 'Destination'
    'Fee': int,
    'Flags': int,
    'DestinationTag': int,
    'SourceTag': int,
    'InvoiceID': str,
    'Sequence': int,
    'SigningPubKey': str,
    'TxnSignature': str,
    'Amount': Any, # Amount can be object or string (XRP in drops), so Any is appropriate
    'Paths': Any, # Paths can be complex, keep as Any for now
    'LastLedgerSequence': int,
    'OfferSequence': int,
    'TakerGets': Any, # Can be object or string
    'TakerPays': Any, # Can be object or string
    'OwnerNode': str,
    'PreviousTxnID': str,
    'PreviousTxnLgrSeq': int,
    'block_number': int, # Added for consistency with ledger data (from ledger_index)
    'block_hash': str,   # Added for consistency with ledger data (from ledger_hash)
    'close_time_iso': str, # Added for consistency (from 'date' field)
    'status': str, # Added for consistency (from TransactionResult in meta)
}

# Schema for meta and memos collection
META_MEMOS_SCHEMA_FIELDS = {
    'block_number': int, # Renamed from 'ledger_index'
    'block_hash': str,   # Renamed from 'ledger_hash'
    'transaction_hash': str,
    'memos': list,
    'meta': dict,
    'close_time_iso': str,
    '_inserted_at': datetime
}

# New schema for failed account transactions
FAILED_TX_SCHEMA_FIELDS = {
    'account_address': str,
    'marker': Any, # Store the marker to retry from (can be string or dict)
    'last_failed_at': datetime,
    'retry_count': int
}

def get_account_address() -> str:
    """
    Prompts the user to enter a Ripple account address and performs basic validation.
    """
    while True:
        address = input("Enter the Ripple account address (e.g., r...): ").strip()
        # Basic validation: starts with 'r' and has a reasonable length for a Ripple address
        if len(address) >= 25 and address.startswith('r'):
            return address
        else:
            logger.error("Please enter a valid Ripple account address (starts with 'r', minimum length ~25-35 characters).")

async def connect_xrpl() -> AsyncWebsocketClient:
    """
    Attempts to connect to an XRPL server from the SERVERS list with retries and exponential backoff.
    Returns an AsyncWebsocketClient instance upon successful connection.
    Raises an Exception if all connection attempts fail.
    """
    for server in SERVERS:
        for attempt in range(MAX_RETRIES):
            try:
                client = AsyncWebsocketClient(server)
                # Attempt to open the connection with a timeout
                await asyncio.wait_for(client.open(), timeout=REQUEST_TIMEOUT)
                logger.info(f"Connected to XRPL server: {server}")
                return client
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to {server}: {e}")
                # Exponential backoff before retrying
                await asyncio.sleep(RECONNECT_DELAY_BASE * (2 ** attempt))
                if attempt == MAX_RETRIES - 1:
                    # If all retries fail for this server, break and try the next server
                    break
    # If all servers fail after all retries, raise an exception
    raise Exception("All XRPL servers failed to connect after multiple attempts.")

def apply_inserted_at(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adds an '_inserted_at' timestamp (UTC now) to the given document.
    """
    doc['_inserted_at'] = datetime.now(timezone.utc)
    return doc

def save_to_mongo(collection_name: str, data: Any) -> None:
    """
    Saves data to the specified MongoDB collection.
    It uses the global 'db' object, which is initialized once in the main process.
    Handles both single documents and lists of documents.
    """
    if not data:
        return
    
    # Ensure db is initialized
    if db is None:
        logger.error(f"MongoDB client not initialized. Cannot save to {collection_name}.")
        return

    try:
        collection = db[collection_name]
        if isinstance(data, list):
            # Apply _inserted_at to each document in the list before inserting
            processed_data_list = [apply_inserted_at(doc) for doc in data]
            # Insert multiple documents; ordered=False allows partial inserts on error
            collection.insert_many(processed_data_list, ordered=False)
            logger.info(f"Saved {len(processed_data_list)} documents to {collection_name}")
        elif isinstance(data, dict):
            # Apply _inserted_at to the single document
            processed_data = apply_inserted_at(data)
            collection.insert_one(processed_data)
            logger.info(f"Saved 1 document to {collection_name}")
    except Exception as e:
        logger.error(f"Failed to save to {collection_name}: {e}")

def process_account_transactions(account_address: str, transactions_raw: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Processes raw transaction data obtained from the `account_tx` method.
    It flattens the transaction details, extracts memo and meta information,
    and formats fields for consistent storage.
    Returns a tuple of lists: (processed_transactions, meta_memos).
    """
    if not transactions_raw:
        return [], []

    processed_txs = []
    meta_memos = []
    
    for tx_entry in transactions_raw:
        # In account_tx response, transaction details are under 'tx' and meta under 'meta'
        tx_data = tx_entry.get('tx', {})
        meta_data = tx_entry.get('meta', {})

        if not tx_data:
            logger.warning(f"Skipping transaction entry with no 'tx' data: {tx_entry}")
            continue

        # Extract common fields, applying renames and conversions
        block_number = tx_data.get('ledger_index')
        block_hash = tx_data.get('ledger_hash', '')
        # 'date' field in account_tx is an XRPL timestamp (seconds since 2000-01-01)
        close_time = tx_data.get('date', 0)
        # Convert XRPL timestamp to ISO 8601 human-readable format
        close_time_iso = datetime.fromtimestamp(close_time + 946684800, timezone.utc).isoformat() if close_time else ""

        processed_tx = {
            "account_address": account_address, # Add the specific account address to the transaction
            "block_number": block_number,
            "block_hash": block_hash,
            "close_time_iso": close_time_iso,
            "status": "success" if meta_data.get("TransactionResult") == "tesSUCCESS" else "failed"
        }
        
        # Flatten all fields from the XRPL transaction object (tx_data) directly into processed_tx
        for k, v in tx_data.items():
            new_key = FIELD_RENAMES.get(k, k)
            # Avoid overwriting fields already set (like block_number, block_hash, etc.)
            if new_key not in processed_tx:
                processed_tx[new_key] = v

        # Validate and convert specific transaction fields based on TX_SCHEMA_FIELDS
        for field, field_type in TX_SCHEMA_FIELDS.items():
            if field in processed_tx and processed_tx[field] is not None:
                value = processed_tx[field]
                # Special handling for Amount, TakerGets, TakerPays as they can be objects or strings
                if field in ['Amount', 'TakerGets', 'TakerPays']:
                    if isinstance(value, str):
                        # If it's a string (e.g., XRP drops), try to convert to int
                        try:
                            processed_tx[field] = int(value)
                        except ValueError:
                            # If conversion to int fails, keep as string and log a warning
                            logger.warning(f"Invalid {field} format in transaction {processed_tx.get('hash', 'N/A')}. Expected int or object, got str (not convertible to int): {value}")
                            processed_tx[field] = value
                    elif isinstance(value, dict):
                        # If it's an object (issued currency), keep it as a dict
                        processed_tx[field] = value
                    else:
                        logger.warning(f"Unexpected type for {field} in transaction {processed_tx.get('hash', 'N/A')}: {type(value).__name__} - {value}")
                elif field == 'Paths':
                    # Paths can be a complex list of lists of dicts, store as is
                    processed_tx[field] = value
                else:
                    # For other fields, attempt conversion based on schema's expected type
                    try:
                        processed_tx[field] = field_type(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid {field} format in transaction {processed_tx.get('hash', 'N/A')}. Expected {field_type.__name__}, got {type(value).__name__}: {value}")
        
        processed_txs.append(processed_tx)
        
        # Process memos and include the full 'meta' field
        meta_memo_data = {
            "account_address": account_address, # Add the specific account address to the memo entry
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_hash": tx_data.get("hash", ""),
            "memos": tx_data.get("Memos", []), # Memos are typically within the 'tx' object
            "meta": meta_data, # The full 'meta' object
            "close_time_iso": close_time_iso,
            '_inserted_at': datetime.now(timezone.utc)
        }
        
        # Validate memos and meta fields based on META_MEMOS_SCHEMA_FIELDS
        for field, field_type in META_MEMOS_SCHEMA_FIELDS.items():
            if field in meta_memo_data and field != '_inserted_at' and meta_memo_data[field] is not None:
                try:
                    meta_memo_data[field] = field_type(meta_memo_data[field])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid {field} format in memos for transaction {tx_data.get('hash', '')}")
        
        meta_memos.append(meta_memo_data)
    
    return processed_txs, meta_memos

async def fetch_all_account_transactions(client: AsyncWebsocketClient, account_address: str) -> None:
    """
    Fetches all transactions for a given Ripple account address, handling pagination.
    It continuously requests pages of transactions using the 'marker' until all are retrieved.
    Saves fetched transactions and memos to MongoDB.
    """
    global db # Access the global db object for failed transactions
    
    current_marker = None
    total_fetched_tx = 0
    page_count = 0

    # Check for existing failed transactions for this account to resume fetching
    if db is not None:
        failed_entry = db[FAILED_TX_COLLECTION].find_one({"account_address": account_address})
        if failed_entry:
            current_marker = failed_entry.get("marker")
            logger.info(f"Resuming fetch for account {account_address} from marker: {current_marker}")

    while True:
        page_count += 1
        logger.info(f"Fetching page {page_count} of transactions for account {account_address}. Current marker: {current_marker}")
        
        transactions_page = []
        next_marker = None
        
        # Retry loop for fetching a single page
        for attempt in range(MAX_RETRIES):
            try:
                # Ensure the client connection is open before making a request
                if not client.is_open():
                    logger.warning(f"Client connection closed for account {account_address}. Attempting to reconnect...")
                    await client.close() # Close existing (potentially broken) connection
                    client = await connect_xrpl() # Get a new, fresh connection

                request_params = {
                    "account": account_address,
                    "limit": TRANSACTION_LIMIT_PER_REQUEST, # Number of transactions per page
                    "binary": False, # Request JSON format
                    "forward": True # Request oldest transactions first
                }
                if current_marker:
                    request_params["marker"] = current_marker

                request = AccountTx(**request_params)
                # Send the request with a timeout
                response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
                
                transactions_page = response.result.get('transactions', [])
                next_marker = response.result.get('marker') # Marker for the next page
                break # Page fetched successfully, break out of retry loop
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to fetch transactions for account {account_address} (page {page_count}): {e}")
                # Exponential backoff before retrying the same page
                await asyncio.sleep(RECONNECT_DELAY_BASE * (2 ** attempt))
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Failed to fetch page {page_count} for account {account_address} after all retries. Saving to failed transactions dataset.")
                    # If all retries fail for a page, save the current state to the failed collection
                    if db is not None:
                        set_fields = {
                            "last_failed_at": datetime.now(timezone.utc),
                            "marker": current_marker # Store the marker where it failed so we can resume
                        }
                        db[FAILED_TX_COLLECTION].update_one(
                            {"account_address": account_address},
                            {"$set": set_fields, "$inc": {"retry_count": 1}}, # Increment retry count
                            upsert=True # Insert if not exists, update if exists
                        )
                    return # Stop processing for this account if a page fails persistently

        if transactions_page:
            # Process and save the fetched transactions and meta/memos
            tx_processed, mm_data = process_account_transactions(account_address, transactions_page)
            save_to_mongo(ACCOUNT_TX_COLLECTION, tx_processed)
            save_to_mongo(META_MEMOS_COLLECTION, mm_data)
            total_fetched_tx += len(tx_processed)
            logger.info(f"Fetched and saved {len(tx_processed)} transactions for account {account_address} (Total: {total_fetched_tx})")
            
            # If a page was successfully fetched and processed, remove any corresponding failed entry
            if db is not None:
                db[FAILED_TX_COLLECTION].delete_one({"account_address": account_address})
        else:
            logger.info(f"No more transactions found for account {account_address} or page was empty.")
            break # No more transactions or an empty page means we're done

        if not next_marker:
            logger.info(f"Finished fetching all transactions for account {account_address}. No more markers.")
            break # No more pages to fetch

        current_marker = next_marker
        # Add a small delay between pages to avoid hitting rate limits on the XRPL server
        await asyncio.sleep(1) 

def main_fetch_account_transactions() -> None:
    """
    Main function to orchestrate the entire process of fetching all transactions
    for a single Ripple account. It handles MongoDB initialization and cleanup.
    """
    global mongo_client, db # Declare global to modify them
    overall_start_time = time.time()
    
    try:
        account_address = get_account_address()

        # Initialize MongoDB client in the main process
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        logger.info("MongoDB client initialized.")

        # Define an async function to run the core fetching logic
        async def run_fetch():
            client = None
            try:
                client = await connect_xrpl() # Establish XRPL client connection
                await fetch_all_account_transactions(client, account_address) # Start fetching
            finally:
                if client:
                    await client.close() # Ensure XRPL client connection is closed
        
        # Run the asynchronous fetching process
        asyncio.run(run_fetch())

        # After fetching, check and delete the failed transactions collection if it's empty
        logger.info("Checking if the failed account transactions collection is empty for deletion.")
        if FAILED_TX_COLLECTION in db.list_collection_names():
            final_failed_count = db[FAILED_TX_COLLECTION].count_documents({})
            if final_failed_count == 0:
                logger.info(f"Failed account transactions collection '{FAILED_TX_COLLECTION}' is empty. Deleting the collection.")
                db.drop_collection(FAILED_TX_COLLECTION)
                logger.info(f"Collection '{FAILED_TX_COLLECTION}' deleted successfully.")
            else:
                logger.warning(f"Failed account transactions collection '{FAILED_TX_COLLECTION}' is not empty. {final_failed_count} items remain. Collection will not be deleted.")
        else:
            logger.info(f"Failed account transactions collection '{FAILED_TX_COLLECTION}' does not exist (perhaps it was never created or already deleted).")

    except Exception as e:
        overall_end_time = time.time()
        overall_execution_time = overall_end_time - overall_start_time
        logger.error(f"Account transaction fetch failed after {overall_execution_time:.2f} seconds: {e}")
        sys.exit(1) # Exit with an error code
    finally:
        # Ensure MongoDB client is closed in all cases
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB client closed.")
        overall_end_time = time.time()
        overall_execution_time = overall_end_time - overall_start_time
        logger.info(f"Overall account transaction fetch process completed in {overall_execution_time:.2f} seconds.")

# Main execution block
if __name__ == "__main__":
    try:
        main_fetch_account_transactions()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1) # Exit gracefully on user interruption
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1) # Exit on any other unexpected error
