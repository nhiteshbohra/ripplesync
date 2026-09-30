import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any, Optional # Added Optional
import time
# import random # Removed as TRy 11.py doesn't use it for polling jitter
# import websockets # Removed as specific WebSocket exceptions are not handled this way in TRy 11.py

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "current_fetch_data" # Keep original DB name for this file
LEDGER_COLLECTION = "ledgers"
TX_COLLECTION = "transactions"
META_MEMOS_COLLECTION = "meta_memos"
FAILED_TX_COLLECTION = "failed_transactions" # New collection for failed transactions
METADATA_COLLECTION = "_metadata" # New collection to store last_processed_ledger_index

SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233"
]
MAX_RETRIES = 10 # Increased to match TRy 11.py
REQUEST_TIMEOUT = 30 # Increased to match TRy 11.py
POLLING_INTERVAL_SECONDS = 5 # How often to check for new ledgers
RECONNECT_DELAY_BASE = 1 # Base delay for exponential backoff, to match TRy 11.py
# MAX_RECONNECT_BACKOFF_POWER = 5 # Removed as TRy 11.py uses RECONNECT_DELAY_BASE * (2 ** attempt) directly

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define a mapping for field renames (from TRy 11.py)
FIELD_RENAMES = {
    "ledger_index": "block_number",
    "ledger_hash": "block_hash",
    "Account": "from_address",
    "Destination": "to_address",
}

# Schema definitions (for validation/type hinting purposes) - Updated to match TRy 11.py
LEDGER_SCHEMA_FIELDS = {
    'block_number': int, # Renamed from 'ledger_index'
    'block_hash': str,   # Renamed from 'ledger_hash'
    'close_time': int,
    'close_time_human': str,
    'total_coins': int,
    'parent_hash': str,
    'account_hash': str,
    'transaction_hash': str,
    'num_transactions': int,
    '_inserted_at': datetime
}

# These are the fields we expect directly at the top level of the transaction document
# This schema helps in validating and type hinting the flattened transaction data.
# Updated to match TRy 11.py
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
    'Amount': Any, # Amount can be object or string, so Any is appropriate
    'Paths': Any, # Paths can be complex, keep as Any for now
    'LastLedgerSequence': int,
    'OfferSequence': int, # Changed to int as per TRy 11.py, though XRPL can sometimes return string
    'TakerGets': Any, # Can be object or string
    'TakerPays': Any, # Can be object or string
    'OwnerNode': str,
    'PreviousTxnID': str,
    'PreviousTxnLgrSeq': int,
}

# Updated to match TRy 11.py
META_MEMOS_SCHEMA_FIELDS = {
    'block_number': int, # Renamed from 'ledger_index'
    'block_hash': str,   # Renamed from 'ledger_hash'
    'transaction_hash': str,
    'memos': list,
    'meta': dict,
    'close_time_iso': str,
    '_inserted_at': datetime
}

# New schema for failed transactions (from TRy 11.py)
FAILED_TX_SCHEMA_FIELDS = {
    'block_number': int,
    'block_hash': str,
    'num_transactions': int,
    'close_time': int, # Include close_time for re-processing
    'last_failed_at': datetime,
    'retry_count': int
}

# MongoDB utility functions (kept as they are not in TRy 11.py's bulk fetch)
def get_mongo_collection(collection_name: str):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[collection_name], client

def get_last_processed_ledger_index() -> int:
    collection, client = get_mongo_collection(METADATA_COLLECTION)
    try:
        doc = collection.find_one({"_id": "last_processed_ledger"})
        return doc.get("index", 0) if doc else 0
    finally:
        client.close()

def update_last_processed_ledger_index(ledger_index: int) -> None:
    collection, client = get_mongo_collection(METADATA_COLLECTION)
    try:
        collection.update_one(
            {"_id": "last_processed_ledger"},
            {"$set": {"index": ledger_index, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )
    finally:
        client.close()

# Connect to XRP Ledger server with fallback and exponential backoff
async def connect_xrpl() -> AsyncWebsocketClient:
    """
    Connects to an XRP Ledger server using a WebSocket client with exponential backoff.
    Retries connection attempts with fallback servers if initial attempts fail.
    Raises an exception if all servers fail to connect after max retries.
    """
    for server in SERVERS:
        for attempt in range(MAX_RETRIES):
            delay = RECONNECT_DELAY_BASE * (2 ** attempt) # Exponential backoff
            try:
                client = AsyncWebsocketClient(server)
                await asyncio.wait_for(client.open(), timeout=REQUEST_TIMEOUT)
                logger.info(f"Connected to XRPL server: {server}")
                return client
            except asyncio.TimeoutError:
                logger.error(f"Attempt {attempt + 1} timed out connecting to {server}. Retrying in {delay} seconds...")
            except Exception as e: # Catch general exceptions including WebSocketException
                logger.error(f"Attempt {attempt + 1} failed to connect to {server} (General Error): {e}. Retrying in {delay} seconds...")
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(delay)
            else:
                break # Last attempt failed, break from inner loop
    raise Exception("All XRPL servers failed to connect after multiple retries")

# Process transaction data (flatten all nested dictionaries and remove memos)
def process_transactions(block_number: int, block_hash: str, close_time: int, transactions: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Processes raw transaction data, flattening it and extracting memo and meta information.
    Specifically, it flattens the 'tx_json' field directly into the main transaction document.
    Returns a tuple of lists: (processed_transactions, meta_memos).
    Uses the new field names (block_number, block_hash, from_address, to_address).
    Handles 'Amount', 'TakerGets', 'TakerPays' which can be strings (XRP) or objects (issued currencies).
    (Logic adapted from TRy 11.py's process_transactions)
    """
    if not transactions:
        return [], []

    close_time_iso = datetime.fromtimestamp(close_time + 946684800, timezone.utc).isoformat() if close_time else ""

    processed_txs = []
    meta_memos = []
    
    for tx in transactions:
        # Start with common fields and then flatten the transaction details
        processed_tx = {
            "block_number": block_number,
            "block_hash": block_hash,
            "close_time_iso": close_time_iso,
            "status": "success" if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else "failed"
        }
        
        # Flatten all fields from the XRPL transaction object directly into processed_tx
        # Exclude 'meta' and 'tx_json' as 'meta' is processed separately for memos and full meta,
        # and 'tx_json' content will be flattened.
        for k, v in tx.items():
            if k != 'meta' and k != 'tx_json':
                # Apply renames for top-level fields like 'Account' and 'Destination'
                new_key = FIELD_RENAMES.get(k, k)
                processed_tx[new_key] = v
        
        # Flatten tx_json if it exists
        tx_json_data = tx.get('tx_json')
        if tx_json_data and isinstance(tx_json_data, dict):
            for k, v in tx_json_data.items():
                # Apply renames for fields within tx_json, e.g., 'Account' and 'Destination'
                new_key = FIELD_RENAMES.get(k, k)
                # Avoid overwriting top-level fields if tx_json contains them
                if new_key not in processed_tx:
                    processed_tx[new_key] = v
                else:
                    logger.warning(f"Skipping flattening of tx_json field '{k}' as it already exists at top level for transaction {tx.get('hash', 'N/A')}")

        # Validate and convert specific transaction fields based on TX_SCHEMA_FIELDS
        for field, field_type in TX_SCHEMA_FIELDS.items():
            if field in processed_tx and processed_tx[field] is not None:
                value = processed_tx[field]
                # Special handling for Amount, TakerGets, TakerPays as they can be objects or strings
                if field in ['Amount', 'TakerGets', 'TakerPays']:
                    if isinstance(value, str):
                        # If it's a string, try to convert to int (for XRP drops)
                        try:
                            processed_tx[field] = int(value)
                        except ValueError:
                            logger.warning(f"Invalid {field} format in transaction {processed_tx.get('hash', 'N/A')}. Expected int or object, got str (not convertible to int): {value}")
                            # Keep as string if conversion to int fails, or handle as needed
                            processed_tx[field] = value
                    elif isinstance(value, dict):
                        # If it's an object (issued currency), keep it as a dict
                        processed_tx[field] = value
                    else:
                        logger.warning(f"Unexpected type for {field} in transaction {processed_tx.get('hash', 'N/A')}: {type(value).__name__} - {value}")
                elif field == 'Paths':
                    # Paths can be complex list of lists of dicts, store as is
                    processed_tx[field] = value
                else:
                    # For other fields, attempt conversion based on schema
                    try:
                        processed_tx[field] = field_type(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid {field} format in transaction {processed_tx.get('hash', 'N/A')}. Expected {field_type.__name__}, got {type(value).__name__}: {value}")
        
        processed_txs.append(processed_tx)
        
        # Process memos and include the full 'meta' field
        meta_memo_data = {
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_hash": tx.get("hash", ""),
            # Correctly extract memos from tx_json
            "memos": tx_json_data.get("Memos", []) if tx_json_data else [], 
            "meta": tx.get("meta", {}), # Now explicitly including the full 'meta' object
            "close_time_iso": close_time_iso,
            '_inserted_at': datetime.now(timezone.utc)
        }
        
        # Validate memos and meta fields based on META_MEMOS_SCHEMA_FIELDS
        for field, field_type in META_MEMOS_SCHEMA_FIELDS.items():
            if field in meta_memo_data and field != '_inserted_at' and meta_memo_data[field] is not None:
                try:
                    meta_memo_data[field] = field_type(meta_memo_data[field])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid {field} format in memos for transaction {tx.get('hash', '')}")
        
        meta_memos.append(meta_memo_data)
    
    return processed_txs, meta_memos

# Save data to MongoDB
def save_to_mongo(collection_name: str, data: Any) -> None:
    """
    Saves data to a specified MongoDB collection.
    Handles upserts for ledger data and bulk inserts for transactions/memos.
    (Logic adapted from TRy 11.py's save_to_mongo)
    """
    if not data:
        return
    mongo_collection, mongo_client = None, None
    try:
        mongo_collection, mongo_client = get_mongo_collection(collection_name)
        
        if collection_name == LEDGER_COLLECTION:
            if isinstance(data, list) and data:
                # Use update_one with upsert=True for ledgers to prevent duplicates
                for item in data:
                    # Apply renames to the item before validation and saving
                    # For ledgers, TRy 11.py applies renames in _process_only_ledger_batch
                    # and save_to_mongo validates against renamed schema.
                    # Here, we assume the data coming in is already using the new names
                    # or that the processing function has handled it.
                    validated_item = {}
                    for field, field_type in LEDGER_SCHEMA_FIELDS.items():
                        if field in item:
                            try:
                                # Ensure 'num_transactions' is an int
                                if field == 'num_transactions' and item[field] is not None:
                                    validated_item[field] = int(item[field])
                                else:
                                    validated_item[field] = field_type(item[field]) if item[field] is not None else None
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid {field} format in ledger {item.get('block_number', '')}")
                    validated_item['_inserted_at'] = datetime.now(timezone.utc)
                    mongo_collection.update_one(
                        {"block_number": validated_item.get("block_number")},
                        {"$set": validated_item},
                        upsert=True
                    )
            elif isinstance(data, dict): 
                validated_data = {}
                for field, field_type in LEDGER_SCHEMA_FIELDS.items():
                    if field in data:
                        try:
                            # Ensure 'num_transactions' is an int
                            if field == 'num_transactions' and data[field] is not None:
                                validated_data[field] = int(data[field])
                            else:
                                validated_data[field] = field_type(data[field]) if data[field] is not None else None
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid {field} format in ledger {data.get('block_number', '')}")
                validated_data['_inserted_at'] = datetime.now(timezone.utc)
                mongo_collection.update_one(
                    {"block_number": validated_data.get("block_number")},
                    {"$set": validated_data},
                    upsert=True
                )
        elif collection_name in [TX_COLLECTION, META_MEMOS_COLLECTION]:
            if isinstance(data, list) and data:
                try:
                    mongo_collection.insert_many(data, ordered=False)
                except Exception as e:
                    # TRy 11.py's save_to_mongo doesn't have this BulkWriteError handling for insert_many.
                    # It relies on _process_transactions_for_ledger to log failed ledgers.
                    # For consistency with TRy 11.py, we simplify this.
                    logger.error(f"Failed to save {len(data)} documents to {collection_name}: {e}")
        logger.info(f"Successfully processed and saved {len(data)} documents to {collection_name}.")
    except Exception as e:
        logger.error(f"A general error occurred while saving to {collection_name}: {e}")
    finally:
        if mongo_client:
            mongo_client.close()

async def _process_single_ledger_with_transactions(client: AsyncWebsocketClient, ledger_index: int) -> bool:
    """
    Fetches a single ledger and its transactions from the XRPL, processes them,
    and saves them to MongoDB. Retries with exponential backoff.
    Returns True on success, False on failure after all retries.
    (Logic adapted from TRy 11.py's _process_transactions_for_ledger and fetch_transactions)
    """
    current_client = client
    for attempt in range(MAX_RETRIES):
        try:
            # Check if the client is still open before making a request
            if not current_client.is_open():
                logger.warning(f"Client connection closed for ledger {ledger_index}. Attempting to reconnect...")
                if current_client:
                    await current_client.close() # Ensure it's closed before trying to open again
                current_client = await connect_xrpl() # Get a new client connection (might be a different server)

            request = Ledger(
                ledger_index=str(ledger_index),
                transactions=True,
                expand=True
            )
            response = await asyncio.wait_for(current_client.request(request), timeout=REQUEST_TIMEOUT)

            ledger_data = response.result.get('ledger', {})
            raw_transactions = ledger_data.pop('transactions', [])

            if not ledger_data:
                logger.warning(f"No ledger data received for ledger {ledger_index}. Attempt {attempt + 1}.")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RECONNECT_DELAY_BASE * (2 ** attempt))
                    continue
                return False

            # Prepare ledger data for saving, using new field names
            close_time = ledger_data.get('close_time', 0)
            close_time_human = ledger_data.get('close_time_human', '')
            num_transactions = len(raw_transactions) # Calculate num_transactions here

            ledger_doc = {
                'block_number': ledger_data.get('ledger_index'),
                'block_hash': ledger_data.get('ledger_hash'),
                'close_time': close_time,
                'close_time_human': close_time_human,
                'total_coins': ledger_data.get('total_coins'),
                'parent_hash': ledger_data.get('parent_hash'),
                'account_hash': ledger_data.get('account_hash'),
                'transaction_hash': ledger_data.get('transaction_hash'),
                'num_transactions': num_transactions,
                '_inserted_at': datetime.now(timezone.utc)
            }
            save_to_mongo(LEDGER_COLLECTION, [ledger_doc])
            logger.info(f"Saved/Updated ledger {ledger_doc['block_number']} to {LEDGER_COLLECTION}")

            # Process and save transactions and memos
            if raw_transactions:
                tx_processed, mm_data = process_transactions(
                    ledger_doc.get('block_number'),
                    ledger_doc.get('block_hash'),
                    ledger_doc.get('close_time'), # Pass close_time
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
            return True # Success, exit attempts loop
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching ledger {ledger_index}. Attempt {attempt + 1}/{MAX_RETRIES}.")
        except Exception as e:
            logger.error(f"Error fetching/processing ledger {ledger_index} (Attempt {attempt + 1}/{MAX_RETRIES}): {e}. Forcing client re-initialization.")
            # If a single ledger processing fails critically, close client and break to re-establish connection
            if current_client and current_client.is_open():
                await current_client.close()
            current_client = None
            break # Break out of inner for loop to trigger outer loop's reconnection logic
            
        if attempt == MAX_RETRIES - 1:
            logger.error(f"Failed to fetch/process ledger {ledger_index} after {MAX_RETRIES} attempts. Skipping this ledger.")
            # Save to failed transactions dataset if all attempts fail
            failed_entry = {
                "block_number": ledger_index,
                "block_hash": ledger_data.get('ledger_hash', ''), # Use original hash if available
                "num_transactions": num_transactions,
                "close_time": close_time,
                "last_failed_at": datetime.now(timezone.utc),
                "retry_count": 1 # Initial retry count
            }
            # Use update_one with upsert=True to either insert or update if it already exists
            # $inc operator ensures retry_count is incremented by 1 (or initialized to 1 if new)
            mongo_collection, mongo_client = get_mongo_collection(FAILED_TX_COLLECTION)
            try:
                mongo_collection.update_one(
                    {"block_number": ledger_index},
                    {"$set": failed_entry, "$inc": {"retry_count": 1}},
                    upsert=True
                )
                logger.info(f"Logged failed ledger {ledger_index} to {FAILED_TX_COLLECTION}.")
            except Exception as db_e:
                logger.error(f"Failed to log failed ledger {ledger_index} to {FAILED_TX_COLLECTION}: {db_e}")
            finally:
                if mongo_client:
                    mongo_client.close()
            return False

async def get_latest_validated_ledger_index(client: AsyncWebsocketClient) -> int:
    """
    Fetches the index of the latest validated ledger from the XRPL.
    
    Args:
        client (AsyncWebsocketClient): The connected XRPL WebSocket client.

    Returns:
        int: The index of the latest validated ledger, or 0 if an error occurs.
    """
    try:
        # Request the latest validated ledger information
        request = Ledger(ledger_index="validated")
        response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
        return response.result.get('ledger_index', 0)
    except asyncio.TimeoutError:
        logger.error("Timeout getting latest validated ledger index.")
        return 0
    except Exception as e:
        logger.error(f"Failed to get latest validated ledger index (General Error): {e}")
        return 0

async def fetch_current_transactions_loop() -> None:
    """
    Continuously fetches and saves transactions from the latest validated XRPL ledgers.
    It polls for new ledgers at a defined interval and processes all ledgers
    from the last processed one up to the current latest.
    Includes reconnection logic for a more robust operation.
    """
    last_processed_ledger_index = get_last_processed_ledger_index() # Load from DB on startup
    client: Optional[AsyncWebsocketClient] = None # Initialize client as Optional

    try:
        logger.info("Starting continuous fetching of current XRPL transactions...")

        # Get initial latest ledger index if no previous record exists
        if last_processed_ledger_index == 0:
            try:
                client = await connect_xrpl()
                initial_latest_ledger = await get_latest_validated_ledger_index(client)
                if initial_latest_ledger > 0:
                    last_processed_ledger_index = initial_latest_ledger - 1 # Start from ledger just before current latest
                    logger.info(f"Initial last processed ledger set to: {last_processed_ledger_index} (derived from current latest).")
                else:
                    logger.warning("Could not get initial latest ledger index. Starting from 0 and hoping for the best.")
            except Exception as e:
                logger.error(f"Failed to establish initial connection or ledger index fetch failed: {e}. Will attempt to reconnect in loop.")
                client = None # Ensure client is None to force reconnection in the loop
        else:
            logger.info(f"Resuming from last processed ledger: {last_processed_ledger_index}.")

        while True:
            if client is None or not client.is_open():
                logger.info("Client is not connected or connection is closed. Attempting to reconnect...")
                try:
                    client = await connect_xrpl()
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}. Retrying in {RECONNECT_DELAY_BASE} seconds...")
                    await asyncio.sleep(RECONNECT_DELAY_BASE)
                    continue # Skip to next loop iteration to try connecting again

            latest_ledger_index = await get_latest_validated_ledger_index(client)

            if latest_ledger_index > 0 and latest_ledger_index > last_processed_ledger_index:
                # Process ledgers in batches or one by one up to the latest
                for ledger_to_fetch in range(last_processed_ledger_index + 1, latest_ledger_index + 1):
                    logger.info(f"Processing ledger: {ledger_to_fetch} (latest validated: {latest_ledger_index})...")
                    try:
                        # Pass the client to the processing function
                        success = await _process_single_ledger_with_transactions(client, ledger_to_fetch)
                        if success:
                            last_processed_ledger_index = ledger_to_fetch # Update only on success
                            update_last_processed_ledger_index(last_processed_ledger_index) # Persist to DB
                        else:
                            # If a ledger fails after all retries, log it and break from inner loop
                            # to force a client reconnection, as the failure might indicate
                            # a persistent issue with the current client connection.
                            logger.error(f"Failed to process ledger {ledger_to_fetch} after all retries. Forcing client reconnection.")
                            if client and client.is_open():
                                await client.close()
                            client = None # Trigger reconnection
                            break # Break from inner for loop to allow outer loop to handle reconnection
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
        sys.exit(0) # Exit with code 0 for graceful exit, handled by the loop's finally block
    except Exception as e:
        logger.critical(f"An unexpected error occurred during application startup or top-level execution: {e}", exc_info=True)
        sys.exit(1)
