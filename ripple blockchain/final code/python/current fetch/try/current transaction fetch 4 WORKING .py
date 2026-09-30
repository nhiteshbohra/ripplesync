import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any, Optional, Union # Added Union for type hinting
import time
import json

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "current_fetch_data_3" # Updated DB Name for new format
LEDGER_COLLECTION = "ledgers"
TX_COLLECTION = "transactions"
META_MEMOS_COLLECTION = "meta_memos"
FAILED_TX_COLLECTION = "failed_transactions"
SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233" # Testnet, good for testing
]
MAX_RETRIES = 3
REQUEST_TIMEOUT = 15
POLLING_INTERVAL_SECONDS = 5
RECONNECT_DELAY_SECONDS = 5

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- USER PROVIDED FORMAT (with minor adjustments for Optional/Any consistency) ---
FIELD_RENAMES = {
    "ledger_index": "block_number",
    "ledger_hash": "block_hash",
    "Account": "from_address",
    "Destination": "to_address",
}

LEDGER_SCHEMA_FIELDS = {
    'block_number': int,
    'block_hash': str,
    'close_time': int,
    'close_time_human': str,
    'total_coins': str,
    'parent_hash': str,
    'account_hash': str,
    'transaction_hash': str,
    'num_transactions': int,
    '_inserted_at': datetime
}

TX_SCHEMA_FIELDS = {
    'TransactionType': str,
    'from_address': Optional[str],
    'hash': str,
    'to_address': Optional[str],
    'Fee': Optional[str],
    'Flags': Optional[int],
    'DestinationTag': Optional[int],
    'SourceTag': Optional[int],
    'InvoiceID': Optional[str],
    'Sequence': Optional[int],
    'SigningPubKey': Optional[str],
    'TxnSignature': Optional[str],
    'Amount': Any,
    'Paths': Optional[list],
    'LastLedgerSequence': Optional[int],
    'OfferSequence': Optional[int],
    'TakerGets': Any,
    'TakerPays': Any,
    'OwnerNode': Optional[str],
    'PreviousTxnID': Optional[str],
    'PreviousTxnLgrSeq': Optional[int],
    'block_number': int,
    'block_hash': str,
    'ledger_close_time': int,
    'ledger_close_time_human': str,
    'status': str
}

META_MEMOS_SCHEMA_FIELDS = {
    'block_number': int,
    'block_hash': str,
    'transaction_hash': str,
    'memos': Optional[list],
    'meta': Optional[dict],
    'ledger_close_time_iso': str,
    '_inserted_at': datetime
}
# --- END USER PROVIDED FORMAT ---

# --- MongoDB Setup and Indexing ---
def setup_mongodb_indexes():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        db[LEDGER_COLLECTION].create_index([("block_number", ASCENDING)], unique=True)
        db[LEDGER_COLLECTION].create_index([("close_time", ASCENDING)])
        logger.info(f"Indexes ensured for {LEDGER_COLLECTION} collection.")

        db[TX_COLLECTION].create_index([("hash", ASCENDING)], unique=True, sparse=True)
        db[TX_COLLECTION].create_index([("block_number", ASCENDING)])
        db[TX_COLLECTION].create_index([("from_address", ASCENDING)], sparse=True)
        db[TX_COLLECTION].create_index([("to_address", ASCENDING)], sparse=True)
        db[TX_COLLECTION].create_index([("TransactionType", ASCENDING)])
        db[TX_COLLECTION].create_index([("ledger_close_time", ASCENDING)])
        logger.info(f"Indexes ensured for {TX_COLLECTION} collection.")

        db[META_MEMOS_COLLECTION].create_index([("transaction_hash", ASCENDING)], unique=True, sparse=True)
        db[META_MEMOS_COLLECTION].create_index([("block_number", ASCENDING)])
        logger.info(f"Indexes ensured for {META_MEMOS_COLLECTION} collection.")
        
        db[FAILED_TX_COLLECTION].create_index([("timestamp", ASCENDING)])
        db[FAILED_TX_COLLECTION].create_index([("block_number", ASCENDING)], sparse=True)
        logger.info(f"Indexes ensured for {FAILED_TX_COLLECTION} collection.")
        client.close()
    except Exception as e:
        logger.error(f"Failed to setup MongoDB indexes: {e}")

# --- XRPL Client Connection ---
async def connect_xrpl() -> AsyncWebsocketClient:
    for server in SERVERS:
        for attempt in range(MAX_RETRIES):
            try:
                client = AsyncWebsocketClient(server)
                await asyncio.wait_for(client.open(), timeout=REQUEST_TIMEOUT)
                logger.info(f"Connected to XRPL server: {server}")
                return client
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to {server}: {e}")
                if attempt == MAX_RETRIES - 1: break
        logger.warning(f"Failed to connect to {server} after {MAX_RETRIES} attempts. Trying next server...")
    raise Exception("All XRPL servers failed to connect after multiple retries.")

# --- Data Processing ---
def convert_xrpl_timestamp_to_iso(xrpl_timestamp: int) -> str:
    return datetime.fromtimestamp(xrpl_timestamp + 946684800, timezone.utc).isoformat()

def validate_and_convert_type(value: Any, expected_type: Any, field_name: str, context_id_for_log: str) -> Any:
    if value is None:
        return None

    base_type = expected_type
    is_optional_or_union = hasattr(expected_type, '__origin__') and expected_type.__origin__ in (Optional, Union)

    if is_optional_or_union:
        args = getattr(expected_type, '__args__', ())
        non_none_types = [t for t in args if t is not type(None)]
        if not non_none_types: # Should not happen for valid Optional/Union
            logger.warning(f"Field '{field_name}' in {context_id_for_log}: Type {expected_type} has no non-None args.")
            return value # Fallback
        if len(non_none_types) == 1:
            base_type = non_none_types[0]
        else: # Multiple types in Union (e.g., Union[int, str])
            for t_option in non_none_types:
                if isinstance(value, t_option):
                    return value # Already correct type
            # If not an instance of any, will try to convert to the first one as a guess
            base_type = non_none_types[0]


    if base_type is Any:
        return value
    if isinstance(value, base_type):
        return value
    
    try:
        if base_type is int: return int(value)
        if base_type is str: return str(value)
        if base_type is float: return float(value)
        if base_type is bool:
            if isinstance(value, str):
                if value.lower() == 'true': return True
                if value.lower() == 'false': return False
            return bool(value)
        
        logger.warning(
            f"Type mismatch for field '{field_name}' in {context_id_for_log}. "
            f"Expected {base_type.__name__ if hasattr(base_type, '__name__') else base_type}, got {type(value).__name__}: {value}. No specific conversion applied."
        )
        return value # Fallback if no conversion rule
    except (ValueError, TypeError) as e:
        logger.error(
            f"Conversion failed for field '{field_name}' in {context_id_for_log}. "
            f"Expected {base_type.__name__ if hasattr(base_type, '__name__') else base_type}, got {type(value).__name__}: {value}. Error: {e}. Setting to None."
        )
        return None

def process_transactions(
    ledger_block_number: int, ledger_block_hash: str, ledger_timestamp_raw: int, transactions_raw: List[Dict]
) -> tuple[List[Dict], List[Dict]]:
    if not transactions_raw: return [], []

    ledger_close_time_iso = convert_xrpl_timestamp_to_iso(ledger_timestamp_raw)
    processed_txs_list = []
    meta_memos_list = []

    for raw_tx_data in transactions_raw:
        tx_json_content = raw_tx_data.get("tx_json", {})
        meta_data_content = raw_tx_data.get("meta", {})
        current_tx_hash = raw_tx_data.get("hash")
        log_context_id = current_tx_hash or f"tx_in_ledger_{ledger_block_number}"

        processed_tx_doc = {field: None for field in TX_SCHEMA_FIELDS}
        processed_tx_doc.update({
            "block_number": ledger_block_number,
            "block_hash": ledger_block_hash,
            "ledger_close_time": ledger_timestamp_raw,
            "ledger_close_time_human": ledger_close_time_iso,
            "hash": current_tx_hash,
            "status": meta_data_content.get("TransactionResult", "unknown") # Default to unknown
        })
        
        # Ensure status is string
        processed_tx_doc["status"] = str(processed_tx_doc["status"])


        for raw_key, raw_value in tx_json_content.items():
            target_key = FIELD_RENAMES.get(raw_key, raw_key)
            if target_key in TX_SCHEMA_FIELDS:
                expected_type = TX_SCHEMA_FIELDS[target_key]
                if target_key in ["Amount", "TakerGets", "TakerPays"] and isinstance(raw_value, (str, int)):
                    processed_tx_doc[target_key] = str(raw_value) # Store XRP drops as string
                else:
                    processed_tx_doc[target_key] = validate_and_convert_type(raw_value, expected_type, target_key, log_context_id)
        
        # Final validation pass for all fields in schema
        for field, expected_type_in_schema in TX_SCHEMA_FIELDS.items():
            current_val = processed_tx_doc.get(field) # Use .get() as it might be missing if not in tx_json
            # If a field was populated directly (like block_number, hash, status), re-validate its type
            validated_value = validate_and_convert_type(current_val, expected_type_in_schema, field, log_context_id)
            processed_tx_doc[field] = validated_value # Update with validated (possibly type-converted) value


        processed_txs_list.append(processed_tx_doc)

        meta_memo_doc = {field: None for field in META_MEMOS_SCHEMA_FIELDS}
        meta_memo_doc.update({
            "block_number": ledger_block_number,
            "block_hash": ledger_block_hash,
            "transaction_hash": current_tx_hash,
            "memos": tx_json_content.get("Memos"), # Will be None if not present, validate_and_convert handles
            "meta": meta_data_content,
            "ledger_close_time_iso": ledger_close_time_iso,
            '_inserted_at': datetime.now(timezone.utc)
        })

        for field, expected_type_in_schema in META_MEMOS_SCHEMA_FIELDS.items():
            current_val = meta_memo_doc.get(field)
            validated_value = validate_and_convert_type(current_val, expected_type_in_schema, field, log_context_id)
            meta_memo_doc[field] = validated_value

        meta_memos_list.append(meta_memo_doc)

    return processed_txs_list, meta_memos_list

# --- MongoDB Save Logic ---
def save_to_mongo(collection_name: str, data: List[Dict]) -> None:
    if not data: return
    mongo_client = None
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        collection = db[collection_name]

        if collection_name == LEDGER_COLLECTION:
            for item in data:
                collection.update_one({"block_number": item.get("block_number")}, {"$set": item}, upsert=True)
        elif collection_name in [TX_COLLECTION, META_MEMOS_COLLECTION]:
            try:
                collection.insert_many(data, ordered=False)
            except BulkWriteError as bwe:
                failed_ops = 0
                if bwe.details and 'writeErrors' in bwe.details:
                    failed_ops = len(bwe.details['writeErrors'])
                logger.warning(f"BulkWriteError for {collection_name}: {failed_ops} errors. Details: {bwe.details}")
                # Log simplified error entry
                failed_collection = db[FAILED_TX_COLLECTION]
                failed_entry = {
                    "original_collection": collection_name,
                    "error_message": str(bwe),
                    "timestamp": datetime.now(timezone.utc),
                    "block_number": data[0].get("block_number") if data else None,
                    "details_summary": bwe.details
                }
                failed_collection.insert_one(failed_entry)
            except Exception as e:
                logger.error(f"Failed to save to {collection_name} (non-BulkWriteError). Data logged to {FAILED_TX_COLLECTION}: {e}")
                failed_collection = db[FAILED_TX_COLLECTION]
                failed_entry = {
                    "original_collection": collection_name,
                    "error_message": str(e),
                    "timestamp": datetime.now(timezone.utc),
                    "block_number": data[0].get("block_number") if data else None,
                    "sample_tx_hash": data[0].get("hash") if data and collection_name == TX_COLLECTION else (data[0].get("transaction_hash") if data else None)
                }
                failed_collection.insert_one(failed_entry)
        logger.info(f"Attempted save for {len(data)} documents to {collection_name}.")
    except Exception as e:
        logger.error(f"General error saving to {collection_name}: {e}")
    finally:
        if mongo_client: mongo_client.close()

async def _process_single_ledger_with_transactions(client: AsyncWebsocketClient, ledger_idx_to_fetch: int) -> None:
    for attempt in range(MAX_RETRIES):
        try:
            request = Ledger(ledger_index=str(ledger_idx_to_fetch), transactions=True, expand=True)
            response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
            ledger_content = response.result.get('ledger', {})
            
            if not ledger_content:
                logger.warning(f"No ledger content for {ledger_idx_to_fetch}, attempt {attempt + 1}.")
                if attempt < MAX_RETRIES -1: await asyncio.sleep(1)
                continue

            raw_tx_list = ledger_content.pop('transactions', [])
            ledger_close_time_raw = int(ledger_content.get('close_time', 0))
            
            ledger_doc_to_save = {field: None for field in LEDGER_SCHEMA_FIELDS} # Initialize
            # Populate with known values first
            ledger_doc_to_save.update({
                'block_number': int(ledger_content.get('ledger_index', 0)),
                'block_hash': ledger_content.get('ledger_hash'),
                'close_time': ledger_close_time_raw,
                'close_time_human': convert_xrpl_timestamp_to_iso(ledger_close_time_raw),
                'total_coins': str(ledger_content.get('total_coins', "0")),
                'parent_hash': ledger_content.get('parent_hash'),
                'account_hash': ledger_content.get('account_hash'),
                'transaction_hash': ledger_content.get('transaction_hash'),
                'num_transactions': len(raw_tx_list),
                '_inserted_at': datetime.now(timezone.utc)
            })
            
            # Validate and convert types for ledger_doc_to_save
            for field, expected_type in LEDGER_SCHEMA_FIELDS.items():
                current_value = ledger_doc_to_save.get(field) # Use .get() as some fields might not come from ledger_content directly
                context_log_id = f"ledger_{ledger_doc_to_save.get('block_number', ledger_idx_to_fetch)}"
                validated_value = validate_and_convert_type(current_value, expected_type, field, context_log_id)
                ledger_doc_to_save[field] = validated_value

            save_to_mongo(LEDGER_COLLECTION, [ledger_doc_to_save])
            logger.info(f"Processed ledger {ledger_doc_to_save['block_number']}")

            if raw_tx_list:
                processed_tx_docs, meta_memos_docs = process_transactions(
                    ledger_doc_to_save['block_number'],
                    ledger_doc_to_save['block_hash'],
                    ledger_doc_to_save['close_time'],
                    raw_tx_list
                )
                if processed_tx_docs: save_to_mongo(TX_COLLECTION, processed_tx_docs)
                if meta_memos_docs: save_to_mongo(META_MEMOS_COLLECTION, meta_memos_docs)
            else:
                logger.info(f"No transactions for block {ledger_doc_to_save['block_number']}")
            return
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching ledger {ledger_idx_to_fetch}, attempt {attempt + 1}/{MAX_RETRIES}.")
        except Exception as e:
            logger.error(f"Error fetching/processing ledger {ledger_idx_to_fetch} (Attempt {attempt + 1}/{MAX_RETRIES}): {e}. Re-raising.")
            if client and client.is_open(): await client.close()
            raise # Re-raise to be caught by the main loop
            
        if attempt == MAX_RETRIES - 1:
            logger.error(f"Failed to fetch/process ledger {ledger_idx_to_fetch} after {MAX_RETRIES} attempts. Skipping.")
            # Optionally, log this failure to FAILED_TX_COLLECTION or a dedicated "failed_ledgers" collection
            return

async def get_latest_validated_ledger_index(client: AsyncWebsocketClient) -> int:
    try:
        request = Ledger(ledger_index="validated")
        response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
        return int(response.result.get('ledger_index', 0))
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting latest validated ledger index.")
        return 0
    except Exception as e:
        logger.error(f"Failed to get latest validated ledger index: {e}")
        return 0

async def fetch_current_transactions_loop() -> None:
    last_processed_ledger_index = 0
    client: Optional[AsyncWebsocketClient] = None
    setup_mongodb_indexes()

    logger.info("Starting continuous fetching of XRPL transactions (New Format)...")
    try:
        # Attempt to get last processed ledger from DB to resume, if desired
        # mongo_temp_client = MongoClient(MONGO_URI)
        # db_temp = mongo_temp_client[DB_NAME]
        # last_ledger_in_db = db_temp[LEDGER_COLLECTION].find_one(sort=[("block_number", -1)])
        # if last_ledger_in_db and 'block_number' in last_ledger_in_db:
        #     last_processed_ledger_index = last_ledger_in_db['block_number']
        #     logger.info(f"Resuming from last processed ledger in DB: {last_processed_ledger_index}")
        # mongo_temp_client.close()

        client = await connect_xrpl()
        initial_latest_ledger = await get_latest_validated_ledger_index(client)
        if initial_latest_ledger > 0:
            if last_processed_ledger_index == 0 : # If not resuming from DB
                 # Start N ledgers behind the current latest to ensure stability and catch up
                 # Or simply start from initial_latest_ledger - 1 if catching up rapidly is fine
                 start_offset = 5 
                 last_processed_ledger_index = max(0, initial_latest_ledger - start_offset) 
            logger.info(f"Initial last processed ledger set to: {last_processed_ledger_index} (Latest validated: {initial_latest_ledger})")
        else:
            logger.warning("Could not get initial latest ledger index. Will retry in loop.")
            if client and client.is_open(): await client.close()
            client = None
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


        while True:
            if client is None or not client.is_open():
                logger.info("Client disconnected. Attempting to reconnect...")
                try:
                    client = await connect_xrpl()
                    logger.info("Successfully reconnected.")
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}. Retrying in {RECONNECT_DELAY_SECONDS}s...")
                    if client and client.is_open(): await client.close() # Ensure closed
                    client = None
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
                    continue

            current_latest_validated_ledger = await get_latest_validated_ledger_index(client)

            if current_latest_validated_ledger == 0: # Failed to get latest
                logger.warning("Still unable to get latest ledger index. Client might be having issues.")
                if client and client.is_open(): await client.close()
                client = None # Force reconnect next iteration
                await asyncio.sleep(POLLING_INTERVAL_SECONDS) # Wait before retrying connection
                continue

            if current_latest_validated_ledger > last_processed_ledger_index:
                logger.info(f"New ledgers detected. From {last_processed_ledger_index + 1} to {current_latest_validated_ledger}")
                for ledger_to_process in range(last_processed_ledger_index + 1, current_latest_validated_ledger + 1):
                    logger.info(f"Requesting ledger: {ledger_to_process}")
                    try:
                        await _process_single_ledger_with_transactions(client, ledger_to_process)
                        last_processed_ledger_index = ledger_to_process
                    except Exception as e: # Catch errors from _process_single_ledger specifically
                        logger.error(f"Error processing ledger {ledger_to_process}: {e}. Client connection may be reset.")
                        # _process_single_ledger should close client on critical error.
                        # Mark client as None to trigger reconnection in the main loop.
                        if client and client.is_open(): await client.close()
                        client = None 
                        break # Break from processing more ledgers in this batch
                
                if client is None: # If connection was lost during batch
                    logger.info("Client connection lost during ledger batch. Will attempt reconnect.")
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS) # Wait before trying reconnect cycle
                    continue # Restart outer while loop to trigger reconnection

            else:
                logger.info(f"No new ledgers since {last_processed_ledger_index}. Latest: {current_latest_validated_ledger}. Polling in {POLLING_INTERVAL_SECONDS}s.")

            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt. Shutting down...")
    except Exception as e:
        logger.critical(f"Unhandled critical error in main loop: {e}", exc_info=True)
    finally:
        if client and client.is_open():
            await client.close()
            logger.info("XRPL client connection closed.")
        logger.info("Script shutdown.")

if __name__ == "__main__":
    try:
        asyncio.run(fetch_current_transactions_loop())
    except KeyboardInterrupt:
        logger.info("Exiting due to KeyboardInterrupt.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Script failed to run: {e}", exc_info=True)
        sys.exit(1)