import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any, Optional
import time
import json # Import for pretty printing raw JSON

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "current_fetch_data_2"
LEDGER_COLLECTION = "ledgers"
TX_COLLECTION = "transactions"
META_MEMOS_COLLECTION = "meta_memos"
FAILED_TX_COLLECTION = "failed_transactions" # New collection for failed transactions
SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233" # Testnet, good for testing
]
MAX_RETRIES = 3
REQUEST_TIMEOUT = 15 # Increased timeout slightly for network fluctuations
POLLING_INTERVAL_SECONDS = 5 # How often to check for new ledgers
RECONNECT_DELAY_SECONDS = 5 # Delay before attempting to reconnect

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Schema definitions (for validation/type hinting purposes)
LEDGER_SCHEMA_FIELDS = {
    'block_number': int,
    'block_hash': str,
    'timestamp': int, # Raw XRPL timestamp
    'timestamp_iso': str,
    'total_coins': int,
    'parent_hash': str,
    'account_hash': str,
    'transaction_hash': str,
    '_inserted_at': datetime
}

# Define the expected fields for the main transactions collection.
# Use Optional[] for fields that might not always be present in every transaction type.
TX_SCHEMA_FIELDS = {
    'TransactionType': str,
    'from_address': str,
    'transaction_hash': str,
    'to_address': Optional[str],
    'Fee': int,
    'Flags': Optional[int],
    'DestinationTag': Optional[int],
    'SourceTag': Optional[int],
    'InvoiceID': Optional[str],
    'Sequence': int,
    'SigningPubKey': str,
    'TxnSignature': str,
    'LastLedgerSequence': Optional[int],
    'OfferSequence': Optional[str], # Can be string or int in XRPL, storing as str
    'OwnerNode': Optional[str],
    'PreviousTxnID': Optional[str],
    'PreviousTxnLgrSeq': Optional[int],
    'ctid': Optional[str], # Crucial field for this discussion

    # Flattened Amount fields (for the primary value transfer in a transaction)
    'value_currency': Optional[str],
    'value_issuer': Optional[str],
    'value_value': Optional[str],

    # Nested TakerGets and TakerPays (for OfferCreate transactions, will be dictionaries in MongoDB)
    'TakerGets': Optional[dict], # Will store {currency: ..., issuer: ..., value: ...}
    'TakerPays': Optional[dict], # Will store {currency: ..., issuer: ..., value: ...}
    
    # Block info
    'block_number': int,
    'block_hash': str,
    'timestamp': int,
    'timestamp_iso': str,
    'status': str
}

# Define the expected fields for the meta_memos collection.
META_MEMOS_SCHEMA_FIELDS = {
    'block_number': int,
    'block_hash': str,
    'transaction_hash': str,
    'memos': list, # List of memo objects
    'meta': dict, # Full meta object
    'timestamp_iso': str,
    '_inserted_at': datetime
}

# --- MongoDB Setup and Indexing ---
def setup_mongodb_indexes():
    """
    Ensures necessary indexes are created for MongoDB collections for efficient querying.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        # Ledger collection indexes
        db[LEDGER_COLLECTION].create_index([("block_number", ASCENDING)], unique=True)
        db[LEDGER_COLLECTION].create_index([("timestamp", ASCENDING)])
        logger.info(f"Indexes ensured for {LEDGER_COLLECTION} collection.")

        # Transactions collection indexes
        db[TX_COLLECTION].create_index([("transaction_hash", ASCENDING)], unique=True, sparse=True)
        db[TX_COLLECTION].create_index([("block_number", ASCENDING)])
        db[TX_COLLECTION].create_index([("from_address", ASCENDING)])
        db[TX_COLLECTION].create_index([("to_address", ASCENDING)], sparse=True)
        db[TX_COLLECTION].create_index([("TransactionType", ASCENDING)])
        db[TX_COLLECTION].create_index([("timestamp", ASCENDING)])
        db[TX_COLLECTION].create_index([("ctid", ASCENDING)], sparse=True) # Index for ctid
        logger.info(f"Indexes ensured for {TX_COLLECTION} collection.")

        # Meta Memos collection indexes
        db[META_MEMOS_COLLECTION].create_index([("transaction_hash", ASCENDING)], unique=True, sparse=True)
        db[META_MEMOS_COLLECTION].create_index([("block_number", ASCENDING)])
        logger.info(f"Indexes ensured for {META_MEMOS_COLLECTION} collection.")
        
        # Failed Transactions collection indexes
        db[FAILED_TX_COLLECTION].create_index([("timestamp", ASCENDING)])
        db[FAILED_TX_COLLECTION].create_index([("block_number", ASCENDING)], sparse=True)
        logger.info(f"Indexes ensured for {FAILED_TX_COLLECTION} collection.")

        client.close()
    except Exception as e:
        logger.error(f"Failed to setup MongoDB indexes: {e}")

# --- XRPL Client Connection ---
async def connect_xrpl() -> AsyncWebsocketClient:
    """
    Connects to an XRP Ledger server using a WebSocket client.
    Retries connection attempts with fallback servers if initial attempts fail.
    Raises an exception if all servers fail to connect after max retries.
    """
    for server in SERVERS:
        for attempt in range(MAX_RETRIES):
            try:
                client = AsyncWebsocketClient(server)
                await asyncio.wait_for(client.open(), timeout=REQUEST_TIMEOUT)
                logger.info(f"Connected to XRPL server: {server}")
                return client
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to {server}: {e}")
                if attempt == MAX_RETRIES - 1:
                    break # All retries exhausted for this server
        logger.warning(f"Failed to connect to {server} after {MAX_RETRIES} attempts. Trying next server...")
    raise Exception("All XRPL servers failed to connect after multiple retries.")

# --- Data Processing ---
def convert_xrpl_timestamp_to_iso(xrpl_timestamp: int) -> str:
    """Converts XRPL timestamp (seconds since 2000-01-01) to ISO 8601 string."""
    return datetime.fromtimestamp(xrpl_timestamp + 946684800, timezone.utc).isoformat()

def process_transactions(block_number: int, block_hash: str, timestamp: int, transactions: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Processes a list of raw XRPL transaction dictionaries.
    Flattens nested fields, renames keys, and separates transaction data from meta/memos data.
    Ensures all fields defined in TX_SCHEMA_FIELDS are present, setting to None if not found.

    Args:
        block_number (int): The ledger index (block number) associated with these transactions.
        block_hash (str): The ledger hash (block hash) associated with these transactions.
        timestamp (int): The close time timestamp of the ledger.
        transactions (List[Dict]): A list of raw transaction dictionaries from the XRPL response.

    Returns:
        tuple[List[Dict], List[Dict]]: A tuple containing two lists:
            - processed_txs (List[Dict]): A list of flattened and processed transaction documents.
            - meta_memos (List[Dict]): A list of documents containing meta and memo data.
    """
    if not transactions:
        return [], []

    timestamp_iso = convert_xrpl_timestamp_to_iso(timestamp)

    processed_txs = []
    meta_memos_list = []

    TX_RENAME_MAPPING = {
        'Account': 'from_address',
        'Destination': 'to_address',
        'hash': 'transaction_hash', # This maps the root hash
    }

    # Helper function to process currency objects (XRP or IOU)
    def process_currency_object_detail(currency_data: Any) -> Dict:
        """Processes a currency object (Amount, TakerGets, TakerPays) into a standardized dict."""
        if isinstance(currency_data, dict):
            # It's an issued currency (IOU) or an XRP object with explicit 'value'
            return {
                "currency": currency_data.get("currency"),
                "issuer": currency_data.get("issuer"),
                "value": currency_data.get("value")
            }
        elif isinstance(currency_data, (str, int)):
            # It's XRP (native currency), represented as a string of drops
            return {
                "currency": "XRP",
                "issuer": None, # XRP has no issuer
                "value": str(currency_data)
            }
        else:
            logger.warning(f"Unexpected type for currency data: {type(currency_data).__name__} - {currency_data}. Returning empty dict.")
            return {"currency": None, "issuer": None, "value": None}


    for idx, tx in enumerate(transactions):
        tx_json_data = tx.get("tx_json", {})
        
        # --- DEBUGGING: Print raw transaction for inspection ---
        # This will help you verify if 'ctid' and currency fields are actually present in the raw data
        if idx < 5: # Print first 5 raw transactions for debugging
            logger.debug(f"Raw Transaction {idx+1} for ledger {block_number}:\n{json.dumps(tx, indent=2)}")
        # --- END DEBUGGING ---

        # Get transaction hash and ctid from the TOP LEVEL of the transaction object
        transaction_hash_from_tx_root = tx.get("hash")
        ctid_from_tx_root = tx.get("ctid") 

        # Initialize processed_tx with all fields from TX_SCHEMA_FIELDS set to None
        # This ensures all fields are present in the MongoDB document.
        processed_tx = {field: None for field in TX_SCHEMA_FIELDS} 
        
        # Populate common fields and those from the top-level 'tx' object
        processed_tx.update({
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp,
            "timestamp_iso": timestamp_iso,
            "transaction_hash": transaction_hash_from_tx_root, 
            "ctid": ctid_from_tx_root, # This should now correctly capture ctid if present in the raw tx object
            "status": "success" if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else "failed"
        })

        # Process fields from 'tx_json_data'
        for k, v in tx_json_data.items():
            if k == 'Memos': # EXCLUDE 'Memos' from the main transaction document
                continue

            mapped_k = TX_RENAME_MAPPING.get(k, k)

            if k == 'Amount': # 'Amount' field is flattened
                processed_currency_data = process_currency_object_detail(v)
                processed_tx['value_currency'] = processed_currency_data['currency']
                processed_tx['value_issuer'] = processed_currency_data['issuer']
                processed_tx['value_value'] = processed_currency_data['value']
            elif k in ('TakerGets', 'TakerPays'): # 'TakerGets' and 'TakerPays' are nested dictionaries
                processed_tx[mapped_k] = process_currency_object_detail(v)
            elif isinstance(v, dict): # Generic dictionary flattening for *other* dicts
                for sub_k, sub_v in v.items():
                    # Avoid overwriting already mapped root transaction_hash if tx_json also has a 'hash' key
                    if f"{mapped_k}_{sub_k}" == "transaction_hash" and "transaction_hash" in processed_tx:
                        continue
                    processed_tx[f"{mapped_k}_{sub_k}"] = sub_v
            elif isinstance(v, list): # Handle lists (e.g., Paths, SignerEntries, AuthAccounts)
                processed_list_elements = []
                for item in v:
                    if isinstance(item, dict):
                        # Flatten each dictionary within the list
                        flattened_item_dict = {}
                        for sub_k, sub_v in item.items():
                            flattened_item_dict[f"{sub_k}"] = sub_v 
                        processed_list_elements.append(flattened_item_dict)
                    else:
                        # Keep non-dictionary list elements as they are
                        processed_list_elements.append(item)
                processed_tx[mapped_k] = processed_list_elements
            else: # Simple types
                processed_tx[mapped_k] = v

        # --- Type Validation and Conversion for processed_tx ---
        # This loop ensures that values conform to the schema types, converting or setting to None if invalid.
        for field, expected_type in TX_SCHEMA_FIELDS.items():
            current_value = processed_tx.get(field) # Use .get() as field might have been set to None earlier

            # Determine the base type for Optional fields
            if hasattr(expected_type, '__origin__') and expected_type.__origin__ is Optional:
                base_type = expected_type.__args__[0]
            else:
                base_type = expected_type

            if current_value is not None:
                if not isinstance(current_value, base_type):
                    try:
                        if base_type is int:
                            processed_tx[field] = int(current_value)
                        elif base_type is str:
                            processed_tx[field] = str(current_value)
                        elif base_type is dict:
                            # If expected dict but got non-dict, set to None
                            processed_tx[field] = None
                            logger.warning(
                                f"Field '{field}' was expected as dict but got {type(current_value).__name__} for tx {processed_tx.get('transaction_hash', 'N/A')}. "
                                f"Original: {current_value}. Setting to None."
                            )
                        # Add more conversions as needed (float, etc.)
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Invalid type for field '{field}' in tx {processed_tx.get('transaction_hash', 'N/A')}. "
                            f"Expected {base_type.__name__}, got {type(current_value).__name__}: {current_value}. Setting to None."
                        )
                        processed_tx[field] = None
            # If current_value is None and expected_type is not Optional, it's already None from initialization.
            # No need to explicitly set to None again, but the warning below can be useful for debugging.
            elif not (hasattr(expected_type, '__origin__') and expected_type.__origin__ is Optional):
                 logger.debug(f"Field '{field}' is None in tx {processed_tx.get('transaction_hash', 'N/A')}, but schema expects {expected_type.__name__}.")


        processed_txs.append(processed_tx)

        # --- Prepare meta_memos data ---
        # Initialize meta_memo_data with all fields from META_MEMOS_SCHEMA_FIELDS set to None
        meta_memo_data = {field: None for field in META_MEMOS_SCHEMA_FIELDS}
        meta_memo_data.update({
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_hash": transaction_hash_from_tx_root, # Use the root hash
            "memos": tx_json_data.get("Memos", []),
            "meta": tx.get("meta", {}),
            "timestamp_iso": timestamp_iso,
            '_inserted_at': datetime.now(timezone.utc)
        })

        # Validate memos and meta fields based on META_MEMOS_SCHEMA_FIELDS
        for field, expected_type in META_MEMOS_SCHEMA_FIELDS.items():
            current_value = meta_memo_data.get(field)
            if hasattr(expected_type, '__origin__') and expected_type.__origin__ is Optional:
                base_type = expected_type.__args__[0]
            else:
                base_type = expected_type

            if current_value is not None:
                if not isinstance(current_value, base_type):
                    try:
                        if base_type is int:
                            meta_memo_data[field] = int(current_value)
                        elif base_type is str:
                            meta_memo_data[field] = str(current_value)
                        # Add more conversions as needed
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Invalid type for meta/memo field '{field}' in tx {transaction_hash_from_tx_root}. "
                            f"Expected {base_type.__name__}, got {type(current_value).__name__}: {current_value}. Setting to None."
                        )
                        meta_memo_data[field] = None
            elif not (hasattr(expected_type, '__origin__') and expected_type.__origin__ is Optional):
                logger.debug(f"Meta/Memo field '{field}' is None in tx {transaction_hash_from_tx_root}, but schema expects {expected_type.__name__}.")
        
        meta_memos_list.append(meta_memo_data)

    return processed_txs, meta_memos_list

# --- MongoDB Save Logic ---
def save_to_mongo(collection_name: str, data: List[Dict]) -> None:
    """
    Saves data to a specified MongoDB collection.
    Handles upserts for ledger data and bulk inserts for transactions/memos.
    If an insertion fails for transactions or memos, the data is logged to a failed transactions collection.

    Args:
        collection_name (str): The name of the MongoDB collection to save data to.
        data (List[Dict]): A list of dictionaries to save.
    """
    if not data:
        return
    
    mongo_client = None
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        collection = db[collection_name]

        if collection_name == LEDGER_COLLECTION:
            for item in data:
                collection.update_one(
                    {"block_number": item.get("block_number")},
                    {"$set": item},
                    upsert=True
                )
        elif collection_name in [TX_COLLECTION, META_MEMOS_COLLECTION]:
            try:
                collection.insert_many(data, ordered=False) # ordered=False to continue on errors
            except BulkWriteError as bwe:
                inserted_count = bwe.details.get('nInserted', 0)
                write_errors = bwe.details.get('writeErrors', [])
                logger.warning(f"BulkWriteError for {collection_name}: {len(write_errors)} errors, {inserted_count} successfully inserted.")
                
                failed_collection = db[FAILED_TX_COLLECTION]
                
                # For simplicity, log the error details and some context from the failed batch
                failed_entry = {
                    "original_collection": collection_name,
                    "data_sample": data[0] if data else {}, # Store a sample of the data
                    "error_message": str(bwe),
                    "timestamp": datetime.now(timezone.utc),
                    "block_number": data[0].get("block_number") if data and isinstance(data[0], dict) else None,
                    "block_hash": data[0].get("block_hash") if data and isinstance(data[0], dict) else None,
                    "details": bwe.details # Store full details for debugging
                }
                failed_collection.insert_one(failed_entry)
                logger.error(f"Failed to save some documents to {collection_name}. Details logged to {FAILED_TX_COLLECTION}.")
            except Exception as e:
                # Catch any other insert_many errors
                failed_collection = db[FAILED_TX_COLLECTION]
                failed_entry = {
                    "original_collection": collection_name,
                    "data_sample": data[0] if data else {},
                    "error_message": str(e),
                    "timestamp": datetime.now(timezone.utc),
                    "block_number": data[0].get("block_number") if data and isinstance(data[0], dict) else None,
                    "block_hash": data[0].get("block_hash") if data and isinstance(data[0], dict) else None,
                }
                failed_collection.insert_one(failed_entry)
                logger.error(f"Failed to save to {collection_name}. Data logged to {FAILED_TX_COLLECTION}: {e}")
        logger.info(f"Successfully processed and saved {len(data)} documents to {collection_name}.")
    except Exception as e:
        logger.error(f"A general error occurred while saving to {collection_name}: {e}")
    finally:
        if mongo_client:
            mongo_client.close()


async def _process_single_ledger_with_transactions(client: AsyncWebsocketClient, ledger_index: int) -> None:
    """
    Fetches a single ledger and its transactions from the XRPL, processes them,
    and saves them to MongoDB.

    Args:
        client (AsyncWebsocketClient): The connected XRPL WebSocket client.
        ledger_index (int): The index of the ledger to fetch.
    """
    for attempt in range(MAX_RETRIES):
        try:
            request = Ledger(
                ledger_index=str(ledger_index),
                transactions=True,
                expand=True
            )
            response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)

            ledger_data = response.result.get('ledger', {})
            raw_transactions = ledger_data.pop('transactions', [])

            if not ledger_data:
                logger.warning(f"No ledger data received for ledger {ledger_index}. Attempt {attempt + 1}.")
                continue # Try next attempt if data is empty

            # Prepare ledger data for saving
            close_time = ledger_data.get('close_time', 0)
            ledger_doc = {
                'block_number': ledger_data.get('ledger_index'),
                'block_hash': ledger_data.get('ledger_hash'),
                'timestamp': close_time,
                'timestamp_iso': convert_xrpl_timestamp_to_iso(close_time),
                'total_coins': ledger_data.get('total_coins'),
                'parent_hash': ledger_data.get('parent_hash'),
                'account_hash': ledger_data.get('account_hash'),
                'transaction_hash': ledger_data.get('transaction_hash'), # This is root tx hash
                '_inserted_at': datetime.now(timezone.utc)
            }
            save_to_mongo(LEDGER_COLLECTION, [ledger_doc])
            logger.info(f"Saved/Updated ledger {ledger_doc['block_number']} to {LEDGER_COLLECTION}")

            # Process and save transactions and memos
            if raw_transactions:
                tx_processed, mm_data = process_transactions(
                    ledger_doc.get('block_number'),
                    ledger_doc.get('block_hash'),
                    ledger_doc.get('timestamp'),
                    raw_transactions
                )
                if tx_processed:
                    save_to_mongo(TX_COLLECTION, tx_processed)
                    logger.info(f"Saved {len(tx_processed)} transactions for block {ledger_doc['block_number']} to {TX_COLLECTION}")
                if mm_data:
                    save_to_mongo(META_MEMOS_COLLECTION, mm_data)
                    logger.info(f"Saved {len(mm_data)} meta/memos for block {ledger_doc['block_number']} to {META_MEMOS_COLLECTION}")
            else:
                logger.info(f"No transactions found for block {ledger_doc['block_number']}")
            return # Success, exit attempts loop
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching ledger {ledger_index}. Attempt {attempt + 1}/{MAX_RETRIES}.")
        except Exception as e:
            logger.error(f"Error fetching/processing ledger {ledger_index} (Attempt {attempt + 1}/{MAX_RETRIES}): {e}. Forcing client re-initialization.")
            # If a single ledger processing fails critically, close client and break to re-establish connection
            if client and client.is_open():
                await client.close()
            client = None
            break # Break out of inner for loop to trigger outer loop's reconnection logic
            
        if attempt == MAX_RETRIES - 1:
            logger.error(f"Failed to fetch/process ledger {ledger_index} after {MAX_RETRIES} attempts. Skipping this ledger.")
            return

async def get_latest_validated_ledger_index(client: AsyncWebsocketClient) -> int:
    """
    Fetches the index of the latest validated ledger from the XRPL.

    Args:
        client (AsyncWebsocketClient): The connected XRPL WebSocket client.

    Returns:
        int: The index of the latest validated ledger, or 0 if an error occurs.
    """
    try:
        request = Ledger(ledger_index="validated")
        response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
        return response.result.get('ledger_index', 0)
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting latest validated ledger index.")
        return 0
    except Exception as e:
        logger.error(f"Failed to get latest validated ledger index: {e}")
        return 0

async def fetch_current_transactions_loop() -> None:
    """
    Continuously fetches and saves transactions from the latest validated XRPL ledgers.
    It polls for new ledgers at a defined interval and processes all ledgers
    from the last processed one up to the current latest.
    Includes reconnection logic for a more robust operation.
    """
    last_processed_ledger_index = 0
    client: Optional[AsyncWebsocketClient] = None # Initialize client as Optional

    # Ensure MongoDB indexes are set up before starting the main loop
    setup_mongodb_indexes()

    try:
        logger.info("Starting continuous fetching of current XRPL transactions...")

        # Initial connection and getting starting ledger index
        try:
            client = await connect_xrpl()
            initial_latest_ledger = await get_latest_validated_ledger_index(client)
            if initial_latest_ledger > 0:
                # Start from the ledger *before* the current latest to ensure no gaps if service restarts
                # or if the very latest ledger is not fully stable yet.
                last_processed_ledger_index = initial_latest_ledger - 1
                logger.info(f"Initial last processed ledger set to: {last_processed_ledger_index}")
            else:
                logger.warning("Could not get initial latest ledger index. Starting from 0.")
        except Exception as e:
            logger.error(f"Initial connection or ledger index fetch failed: {e}. Will attempt to reconnect in loop.")
            client = None # Ensure client is None to force reconnection in the loop

        while True:
            if client is None or not client.is_open():
                logger.info("Client is not connected or connection is closed. Attempting to reconnect...")
                try:
                    client = await connect_xrpl()
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}. Retrying in {RECONNECT_DELAY_SECONDS} seconds...")
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
                    continue # Skip to next loop iteration to try connecting again

            latest_ledger_index = await get_latest_validated_ledger_index(client)

            if latest_ledger_index > 0 and latest_ledger_index > last_processed_ledger_index:
                # Process ledgers in batches or one by one up to the latest
                for ledger_to_fetch in range(last_processed_ledger_index + 1, latest_ledger_index + 1):
                    logger.info(f"Processing ledger: {ledger_to_fetch} (latest validated: {latest_ledger_index})...")
                    try:
                        # Pass the client to the processing function
                        await _process_single_ledger_with_transactions(client, ledger_to_fetch)
                        last_processed_ledger_index = ledger_to_fetch # Update only on success
                    except Exception as e:
                        logger.error(f"Critical error during ledger {ledger_to_fetch} processing: {e}. Forcing client re-initialization.")
                        # If a single ledger processing fails critically, close client and break to re-establish connection
                        if client and client.is_open():
                            await client.close()
                        client = None
                        break # Break out of inner for loop to trigger outer loop's reconnection logic

            else:
                logger.info(f"No new ledgers since {last_processed_ledger_index}. Latest: {latest_ledger_index}. Waiting for {POLLING_INTERVAL_SECONDS} seconds...")

            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    except Exception as e:
        logger.critical(f"An unhandled critical error occurred in the main fetching loop: {e}")
    finally:
        if client and client.is_open():
            await client.close()
            logger.info("XRPL client connection closed.")

# Main execution
if __name__ == "__main__":
    try:
        asyncio.run(fetch_current_transactions_loop())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during script execution: {e}")
        sys.exit(1)