import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any
import time

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "current_fetch_data"
LEDGER_COLLECTION = "ledgers"
TX_COLLECTION = "transactions"
META_MEMOS_COLLECTION = "meta_memos"
FAILED_TX_COLLECTION = "failed_transactions" # New collection for failed transactions
SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233"
]
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
POLLING_INTERVAL_SECONDS = 5 # How often to check for new ledgers
RECONNECT_DELAY_SECONDS = 5 # Delay before attempting to reconnect

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Schema definitions (for validation/type hinting purposes)
LEDGER_SCHEMA_FIELDS = {
    'block_number': int,
    'block_hash': str,
    'timestamp': int, # Renamed from close_time
    'timestamp_iso': str, # Renamed from close_time_human
    'total_coins': int,
    'parent_hash': str,
    'account_hash': str,
    'transaction_hash': str,
    '_inserted_at': datetime
}

# These are the fields we expect directly at the top level of the transaction document
# Includes common flattened fields. Note: This list might not cover ALL possible flattened fields
# for every transaction type, but handles the most common nested objects like Amount.
TX_SCHEMA_FIELDS = {
    'TransactionType': str,
    'from_address': str, # Renamed from Account
    'transaction_hash': str, # Renamed from hash
    'to_address': str, # Renamed from Destination
    'Fee': int,
    'Flags': int,
    'DestinationTag': int, # Added
    'SourceTag': int, # Added
    'InvoiceID': str,
    'Sequence': int,
    'SigningPubKey': str,
    'TxnSignature': str,
    'LastLedgerSequence': int,
    'OfferSequence': str, # Changed to str as it can be a string in some cases
    'OwnerNode': str,
    'PreviousTxnID': str,
    'PreviousTxnLgrSeq': int,
    
    # Common flattened fields for currency amounts/objects, renamed
    'value_currency': str, # Renamed from Amount_currency
    'value_issuer': str, # Renamed from Amount_issuer
    'value_value': str, # Renamed from Amount_value
    'TakerGets_currency': str,
    'TakerGets_issuer': str,
    'TakerGets_value': str,
    'TakerPays_currency': str,
    'TakerPays_issuer': str,
    'TakerPays_value': str,
    
    # Block info (renamed)
    'block_number': int,
    'block_hash': str,
    'timestamp': int, # Added for raw timestamp in TX
    'timestamp_iso': str,
    'status': str
}

META_MEMOS_SCHEMA_FIELDS = {
    'block_number': int, # Renamed from ledger_index
    'block_hash': str, # Renamed from ledger_hash
    'transaction_hash': str,
    'memos': list,
    'meta': dict,
    'timestamp_iso': str, # Renamed from close_time_iso
    '_inserted_at': datetime
}

# Connect to XRP Ledger server with fallback
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
                    break
    raise Exception("All XRPL servers failed to connect")

# Process transaction data (flatten all nested dictionaries and remove memos)
def process_transactions(block_number: int, block_hash: str, timestamp: int, transactions: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Processes a list of raw XRPL transaction dictionaries.
    Flattens nested fields, renames keys, and separates transaction data from meta/memos data.
    Performs basic type validation based on defined schemas.
    
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

    # Convert XRPL timestamp (seconds since 2000-01-01) to ISO format
    timestamp_iso = datetime.fromtimestamp(timestamp + 946684800, timezone.utc).isoformat() if timestamp else ""

    processed_txs = []
    meta_memos = []
    
    # Mapping for direct renames of transaction top-level keys
    TX_RENAME_MAPPING = {
        'Account': 'from_address',
        'Destination': 'to_address',
        'hash': 'transaction_hash', # This maps the root hash
        'SourceTag': 'SourceTag', # Keep as is, just ensure it's handled
        'DestinationTag': 'DestinationTag' # Keep as is, just ensure it's handled
    }

    # Special handling for amount-like fields that are dictionaries
    AMOUNT_LIKE_FIELDS = {
        'Amount': 'value',
        'TakerGets': 'taker_gets',
        'TakerPays': 'taker_pays'
    }

    for tx in transactions:
        processed_tx = {
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp, # Added raw timestamp
            "timestamp_iso": timestamp_iso,
            "status": "success" if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else "failed"
        }
        
        # Get the 'tx_json' and top-level 'hash' from the raw XRPL transaction object
        tx_json_data = tx.get("tx_json", {})
        transaction_hash_from_tx_root = tx.get("hash", "")
        
        # Add the top-level transaction hash directly
        if transaction_hash_from_tx_root:
            processed_tx["transaction_hash"] = transaction_hash_from_tx_root

        # Iterate through the fields within 'tx_json_data' to process them
        for k, v in tx_json_data.items():
            if k == 'Memos': # EXCLUDE 'Memos' from the transaction document
                continue 
            
            # Apply direct renames for top-level keys before further processing
            mapped_k = TX_RENAME_MAPPING.get(k, k)

            # If the value is a dictionary, flatten its contents
            if isinstance(v, dict):
                # Check for amount-like fields that need specific prefixes
                if k in AMOUNT_LIKE_FIELDS:
                    new_prefix = AMOUNT_LIKE_FIELDS[k]
                    for sub_k, sub_v in v.items():
                        processed_tx[f"{new_prefix}_{sub_k}"] = sub_v
                else: # Generic dictionary flattening for other dicts (if any)
                    for sub_k, sub_v in v.items():
                        processed_tx[f"{mapped_k}_{sub_k}"] = sub_v
            # If the value is a list, and it contains dictionaries, flatten those dictionaries
            elif isinstance(v, list):
                processed_list_elements = []
                for item in v:
                    if isinstance(item, dict):
                        # Flatten each dictionary within the list
                        flattened_item_dict = {}
                        for sub_k, sub_v in item.items():
                            # For lists of dicts (like Paths), the prefix is parent_key_item_sub_key
                            # The parent key 'k' needs to be mapped if it's in RENAME_MAPPING
                            list_prefix_k = TX_RENAME_MAPPING.get(k, k)
                            flattened_item_dict[f"{list_prefix_k}_item_{sub_k}"] = sub_v
                        processed_list_elements.append(flattened_item_dict)
                    else:
                        # Keep non-dictionary list elements as they are
                        processed_list_elements.append(item)
                processed_tx[mapped_k] = processed_list_elements
            else:
                # For simple types (strings, ints, bools, etc.), copy directly after applying rename
                processed_tx[mapped_k] = v
        
        # Apply type validation for specific fields defined in TX_SCHEMA_FIELDS
        for field, field_type in TX_SCHEMA_FIELDS.items():
            if field in processed_tx and processed_tx[field] is not None:
                try:
                    # Special handling for SourceTag and DestinationTag - ensure they are integers if present
                    if field in ['SourceTag', 'DestinationTag'] and not isinstance(processed_tx[field], int):
                        processed_tx[field] = int(processed_tx[field])
                    elif field == 'OfferSequence' and not isinstance(processed_tx[field], str):
                        processed_tx[field] = str(processed_tx[field])
                    else:
                        processed_tx[field] = field_type(processed_tx[field])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid {field} format in transaction {processed_tx.get('transaction_hash', 'N/A')}. Expected {field_type.__name__}, got {type(processed_tx[field]).__name__}: {processed_tx[field]}")
        
        processed_txs.append(processed_tx)
        
        # Process memos and include the full 'meta' field
        meta_memo_data = {
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_hash": tx.get("hash", ""), # Use original hash for consistency as it's the root hash
            "memos": tx_json_data.get("Memos", []), 
            "meta": tx.get("meta", {}), 
            "timestamp_iso": timestamp_iso,
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
    Handles upserts for ledger data to prevent duplicates and insert_many for transactions/memos.
    If an insertion fails for transactions or memos, the data is logged to a failed transactions collection.
    
    Args:
        collection_name (str): The name of the MongoDB collection to save data to.
        data (Any): The data to save (can be a single dict or a list of dicts).
    """
    if not data:
        return
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        collection = db[collection_name]
        if collection_name == LEDGER_COLLECTION:
            if isinstance(data, list) and data:
                # Use update_one with upsert=True for ledgers to prevent duplicates
                for item in data:
                    validated_item = {}
                    for field, field_type in LEDGER_SCHEMA_FIELDS.items():
                        if field in item:
                            try:
                                validated_item[field] = field_type(item[field]) if item[field] is not None else None
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid {field} format in ledger {item.get('block_number', '')}")
                    validated_item['_inserted_at'] = datetime.now(timezone.utc)
                    collection.update_one(
                        {"block_number": validated_item.get("block_number")},
                        {"$set": validated_item},
                        upsert=True
                    )
            elif isinstance(data, dict): 
                validated_data = {}
                for field, field_type in LEDGER_SCHEMA_FIELDS.items():
                    if field in data:
                        try:
                            validated_data[field] = field_type(data[field]) if data[field] is not None else None
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid {field} format in ledger {data.get('block_number', '')}")
                validated_data['_inserted_at'] = datetime.now(timezone.utc)
                collection.update_one(
                    {"block_number": validated_data.get("block_number")},
                    {"$set": validated_data},
                    upsert=True
                )
        elif collection_name in [TX_COLLECTION, META_MEMOS_COLLECTION]:
            if isinstance(data, list) and data:
                try:
                    collection.insert_many(data, ordered=False)
                except Exception as e:
                    # Log the failed batch to a separate collection
                    failed_collection = db[FAILED_TX_COLLECTION]
                    failed_entry = {
                        "original_collection": collection_name,
                        "data": data,
                        "error_message": str(e),
                        "timestamp": datetime.now(timezone.utc)
                    }
                    # Attempt to get block_number and block_hash from the first item if available
                    if data and isinstance(data[0], dict):
                        failed_entry["block_number"] = data[0].get("block_number")
                        failed_entry["block_hash"] = data[0].get("block_hash")
                    failed_collection.insert_one(failed_entry)
                    logger.error(f"Failed to save to {collection_name}. Data logged to {FAILED_TX_COLLECTION}: {e}")
        mongo_client.close()
    except Exception as e:
        logger.error(f"Failed to save to {collection_name}: {e}")

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
            transactions = ledger_data.pop('transactions', []) 
            
            if not ledger_data:
                logger.warning(f"No ledger data received for {ledger_index}")
                return

            # Process and save ledger data
            close_time = ledger_data.get('close_time', 0)
            
            # Renaming and popping original fields into new ones
            ledger_data['timestamp_iso'] = datetime.fromtimestamp(close_time + 946684800, timezone.utc).isoformat() if close_time else ""
            ledger_data['block_number'] = ledger_data.pop('ledger_index')
            ledger_data['block_hash'] = ledger_data.pop('ledger_hash')
            ledger_data['timestamp'] = ledger_data.pop('close_time') # Keep the raw timestamp

            save_to_mongo(LEDGER_COLLECTION, [ledger_data]) 
            logger.info(f"Saved/Updated ledger {ledger_data['block_number']} to {LEDGER_COLLECTION}")

            # Process and save transactions and memos
            if transactions:
                # Pass the new names to process_transactions
                tx_processed, mm_data = process_transactions(
                    ledger_data.get('block_number'),
                    ledger_data.get('block_hash'),
                    ledger_data.get('timestamp'),
                    transactions
                )
                if tx_processed:
                    save_to_mongo(TX_COLLECTION, tx_processed)
                    logger.info(f"Saved {len(tx_processed)} transactions for block {ledger_data['block_number']} to {TX_COLLECTION}")
                if mm_data:
                    save_to_mongo(META_MEMOS_COLLECTION, mm_data)
                    logger.info(f"Saved {len(mm_data)} meta/memos for block {ledger_data['block_number']} to {META_MEMOS_COLLECTION}")
            else:
                logger.info(f"No transactions found for block {ledger_data['block_number']}")
            return 
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to fetch/process data for ledger {ledger_index}: {e}")
            if attempt == MAX_RETRIES - 1:
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
        # Request the latest validated ledger information
        request = Ledger(ledger_index="validated")
        response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
        return response.result.get('ledger_index', 0)
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
    last_processed_ledger_index = 0 # Keep track of the last processed ledger to avoid re-processing
    client = None
    try:
        logger.info("Starting continuous fetching of current XRPL transactions...")

        # Get the initial latest ledger to start from
        # Attempt to connect before getting the initial ledger index
        try:
            client = await connect_xrpl()
            initial_latest_ledger = await get_latest_validated_ledger_index(client)
            if initial_latest_ledger > 0:
                last_processed_ledger_index = initial_latest_ledger - 1 # Start from the ledger just before the current latest
                logger.info(f"Initial last processed ledger set to: {last_processed_ledger_index}")
        except Exception as e:
            logger.error(f"Failed to establish initial connection or get initial ledger: {e}. Retrying in loop...")
            client = None # Ensure client is None to force reconnection in the loop

        while True:
            if client is None or not client.is_open():
                logger.info("Client is not connected. Attempting to reconnect...")
                try:
                    client = await connect_xrpl()
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}. Retrying in {RECONNECT_DELAY_SECONDS} seconds...")
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
                    continue # Skip to next loop iteration to try connecting again

            latest_ledger_index = 0
            try:
                latest_ledger_index = await get_latest_validated_ledger_index(client)
            except Exception as e:
                logger.error(f"Error getting latest validated ledger index: {e}. Forcing reconnection...")
                if client and client.is_open():
                    await client.close() # Close the problematic client
                client = None # Set client to None to trigger reconnection in next iteration
                await asyncio.sleep(RECONNECT_DELAY_SECONDS) # Wait before retrying
                continue # Skip to next loop iteration

            if latest_ledger_index > 0 and latest_ledger_index > last_processed_ledger_index:
                # Fetch and process all ledgers from the last processed + 1 up to the current latest
                for ledger_to_fetch in range(last_processed_ledger_index + 1, latest_ledger_index + 1):
                    logger.info(f"Processing ledger: {ledger_to_fetch} (up to latest: {latest_ledger_index})...")
                    try:
                        await _process_single_ledger_with_transactions(client, ledger_to_fetch)
                        last_processed_ledger_index = ledger_to_fetch # Update after each successful fetch
                    except Exception as e:
                        logger.error(f"Error processing ledger {ledger_to_fetch}: {e}. Skipping to next ledger.")
                        # Do not update last_processed_ledger_index for this failed ledger,
                        # so it will be re-attempted in the next cycle.
                        # Optionally, you could log this specific ledger to a failed_ledgers collection.
                        if client and client.is_open():
                            await client.close() # Close the problematic client
                        client = None # Force reconnection
                        break # Break out of inner for loop to trigger outer loop's reconnection logic
            else:
                logger.info(f"No new ledgers since {last_processed_ledger_index}. Waiting for {POLLING_INTERVAL_SECONDS} seconds...")
            
            await asyncio.sleep(POLLING_INTERVAL_SECONDS)  # Wait before checking again

    except Exception as e:
        logger.error(f"An unhandled error occurred in the main fetching loop: {e}")
    finally:
        if client and client.is_open():
            await client.close()
            logger.info("XRPL client connection closed.")

# Main execution
if __name__ == "__main__":
    try:
        # Run the continuous fetching loop
        asyncio.run(fetch_current_transactions_loop())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user (Ctrl+C). Exiting gracefully.")
        sys.exit(0) # Exit with code 0 for graceful exit
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
