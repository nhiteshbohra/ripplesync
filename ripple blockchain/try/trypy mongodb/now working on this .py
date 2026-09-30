import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any
from multiprocessing import Pool, current_process
import math
import time

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xrpl_data7"
LEDGER_COLLECTION = "ledgers"
TX_COLLECTION = "transactions"
META_MEMOS_COLLECTION = "meta_memos"
SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233"
]
MAX_LEDGER_RANGE = 100  # Increased for larger batches
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
BATCH_CONCURRENCY = 4  # Increased for more concurrent async tasks
NUM_PROCESSES = 8  # Increased for more parallel processes

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Global variables for MongoDB client within each worker process
# These will be initialized once per process
mongo_client = None
db = None

# Schema definitions (for validation/type hinting purposes)
LEDGER_SCHEMA_FIELDS = {
    'ledger_index': int,
    'ledger_hash': str,
    'close_time': int,
    'close_time_human': str,
    'total_coins': int,
    'parent_hash': str,
    'account_hash': str,
    'transaction_hash': str,
    '_inserted_at': datetime
}

# These are the fields we expect directly at the top level of the transaction document
TX_SCHEMA_FIELDS = {
    'TransactionType': str,
    'Account': str,
    'hash': str,
    'Destination': str,
    'Fee': int,
    'Flags': int,
    'DestinationTag': int,
    'SourceTag': int,
    'InvoiceID': str,
    'Sequence': int,
    'SigningPubKey': str,
    'TxnSignature': str,
    'Amount': Any, # Amount can be object or string
    'Paths': Any,
    'LastLedgerSequence': int,
    'OfferSequence': int,
    'TakerGets': Any,
    'TakerPays': Any,
    'OwnerNode': str,
    'PreviousTxnID': str,
    'PreviousTxnLgrSeq': int,
}

META_MEMOS_SCHEMA_FIELDS = {
    'ledger_index': int,
    'ledger_hash': str,
    'transaction_hash': str,
    'memos': list,
    'meta': dict, # Added the 'meta' field to the schema
    'close_time_iso': str,
    '_inserted_at': datetime
}

# Get user input for ledger range
def get_ledger_range() -> tuple[int, int]:
    """
    Prompts the user to enter the start and end ledger numbers,
    and validates the input.
    """
    while True:
        try:
            start_ledger = int(input("Enter the start ledger number: ").strip())
            end_ledger = int(input("Enter the end ledger number: ").strip())
            
            if start_ledger < 32570:
                logger.error("Ledger numbers must be greater than 32570")
                continue
            if start_ledger > end_ledger:
                logger.error("Start ledger must be less than or equal to end ledger")
                continue
            if end_ledger - start_ledger + 1 > MAX_LEDGER_RANGE:
                logger.warning(f"Ledger range exceeds {MAX_LEDGER_RANGE}. Will process in batches")
                
            return start_ledger, end_ledger
        except ValueError:
            logger.error("Please enter valid integer numbers for ledger range")

# Connect to XRP Ledger server with fallback
async def connect_xrpl() -> AsyncWebsocketClient:
    """
    Attempts to connect to an XRPL server from the SERVERS list with retries.
    Returns an AsyncWebsocketClient instance upon successful connection.
    Raises an Exception if all connection attempts fail.
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

# Fetch ledger data (without transactions)
async def fetch_ledger(client: AsyncWebsocketClient, ledger_index: int) -> Dict[str, Any]:
    """
    Fetches basic ledger metadata for a given ledger_index from the XRPL.
    Excludes transaction data to keep the initial fetch light.
    """
    for attempt in range(MAX_RETRIES):
        try:
            request = Ledger(
                ledger_index=str(ledger_index),
                transactions=False,  # Exclude transactions
                expand=False
            )
            response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
            return response.result.get('ledger', {})
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to fetch ledger {ledger_index}: {e}")
            if attempt == MAX_RETRIES - 1:
                return {}
    return {}

# Fetch transactions separately
async def fetch_transactions(client: AsyncWebsocketClient, ledger_index: int) -> List[Dict]:
    """
    Fetches detailed transaction data for a given ledger_index from the XRPL.
    """
    for attempt in range(MAX_RETRIES):
        try:
            request = Ledger(
                ledger_index=str(ledger_index),
                transactions=True,
                expand=True
            )
            response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
            return response.result.get('ledger', {}).get('transactions', [])
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to fetch transactions for ledger {ledger_index}: {e}")
            if attempt == MAX_RETRIES - 1:
                return []
    return []

# Process transaction data (flatten and include meta field in meta_memos)
def process_transactions(ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Processes raw transaction data, flattening it and extracting memo and meta information.
    Returns a tuple of lists: (processed_transactions, meta_memos).
    """
    if not transactions:
        return [], []

    close_time_iso = datetime.fromtimestamp(close_time + 946684800, timezone.utc).isoformat() if close_time else ""

    processed_txs = []
    meta_memos = []
    
    for tx in transactions:
        # Start with common fields and then flatten the transaction details
        processed_tx = {
            "ledger_index": ledger_index,
            "ledger_hash": ledger_hash,
            "close_time_iso": close_time_iso,
            "status": "success" if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else "failed"
        }
        
        # Flatten all fields from the XRPL transaction object directly into processed_tx
        # Exclude 'meta' as it's processed separately for memos and full meta
        for k, v in tx.items():
            if k != 'meta':
                processed_tx[k] = v
        
        # Validate and convert specific transaction fields based on TX_SCHEMA_FIELDS
        for field, field_type in TX_SCHEMA_FIELDS.items():
            if field in processed_tx and processed_tx[field] is not None:
                try:
                    processed_tx[field] = field_type(processed_tx[field])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid {field} format in transaction {processed_tx.get('hash', 'N/A')}. Expected {field_type.__name__}, got {type(processed_tx[field]).__name__}: {processed_tx[field]}")
        
        processed_txs.append(processed_tx)
        
        # Process memos and include the full 'meta' field
        meta_memo_data = {
            "ledger_index": ledger_index,
            "ledger_hash": ledger_hash,
            "transaction_hash": tx.get("hash", ""),
            "memos": tx.get("Memos", []),
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

# Save data to MongoDB - now uses the pre-initialized 'db' object
def save_to_mongo(collection_name: str, data: Any) -> None:
    """
    Saves data to the specified MongoDB collection.
    Uses the global 'db' object which is initialized once per process.
    """
    if not data:
        return
    
    # Ensure db is initialized (should be by worker_init)
    if db is None:
        logger.error(f"MongoDB client not initialized in process {current_process().name}. Cannot save to {collection_name}.")
        return

    try:
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
                                logger.warning(f"Invalid {field} format in ledger {item.get('ledger_index', '')}")
                    validated_item['_inserted_at'] = datetime.now(timezone.utc)
                    collection.update_one(
                        {"ledger_index": validated_item.get("ledger_index")},
                        {"$set": validated_item},
                        upsert=True
                    )
                logger.info(f"Saved/Updated {len(data)} ledgers to {collection_name} in process {current_process().name}")
            elif isinstance(data, dict):
                # Single ledger update
                validated_data = {}
                for field, field_type in LEDGER_SCHEMA_FIELDS.items():
                    if field in data:
                        try:
                            validated_data[field] = field_type(data[field]) if data[field] is not None else None
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid {field} format in ledger {data.get('ledger_index', '')}")
                validated_data['_inserted_at'] = datetime.now(timezone.utc)
                collection.update_one(
                    {"ledger_index": validated_data.get("ledger_index")},
                    {"$set": validated_data},
                    upsert=True
                )
                logger.info(f"Saved/Updated ledger {validated_data.get('ledger_index')} to {collection_name} in process {current_process().name}")
        elif collection_name in [TX_COLLECTION, META_MEMOS_COLLECTION]:
            if isinstance(data, list) and data:
                collection.insert_many(data, ordered=False)
                logger.info(f"Saved {len(data)} documents to {collection_name} in process {current_process().name}")
    except Exception as e:
        logger.error(f"Failed to save to {collection_name} in process {current_process().name}: {e}")

# Initializer function for multiprocessing pool workers
def worker_init():
    """
    Initializes the MongoDB client and database connection for each worker process.
    This function is called once per worker when the pool starts.
    """
    global mongo_client, db
    if mongo_client is None:
        try:
            mongo_client = MongoClient(MONGO_URI)
            db = mongo_client[DB_NAME]
            logger.info(f"MongoDB client initialized for process {current_process().name}")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB client in process {current_process().name}: {e}")
            sys.exit(1) # Exit worker if DB connection fails

# --- Phase 1: Fetch and Store Ledgers Only ---
async def _process_only_ledger_batch(client: AsyncWebsocketClient, start_ledger: int, end_ledger: int) -> None:
    """
    Asynchronously fetches ledger metadata for a given range and saves them.
    """
    ledgers_to_save = []
    for ledger_index in range(start_ledger, end_ledger + 1):
        logger.info(f"Fetching ledger metadata for {ledger_index} in process {current_process().name}")
        ledger_data = await fetch_ledger(client, ledger_index)
        if ledger_data:
            ledgers_to_save.append(ledger_data)
    save_to_mongo(LEDGER_COLLECTION, ledgers_to_save)

def _worker_fetch_only_ledgers(batch_range: tuple) -> None:
    """
    Worker function for multiprocessing pool to fetch only ledger metadata.
    Each worker runs its own asyncio event loop.
    """
    start_ledger, end_ledger = batch_range
    logger.info(f"Process {current_process().name} for ledger metadata: {start_ledger} to {end_ledger} started")
    
    async def run_batch():
        client = None
        try:
            client = await connect_xrpl()
            semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
            async def process_with_semaphore(start: int, end: int):
                async with semaphore:
                    logger.info(f"Fetching ledger metadata sub-batch: {start} to {end} in process {current_process().name}")
                    await _process_only_ledger_batch(client, start, end)
            
            # Split the batch_range into smaller sub-ranges for concurrent async calls
            sub_ranges = [(i, min(i + MAX_LEDGER_RANGE - 1, end_ledger))
                         for i in range(start_ledger, end_ledger + 1, MAX_LEDGER_RANGE)]
            await asyncio.gather(*[process_with_semaphore(start, end) for start, end in sub_ranges])
        finally:
            if client:
                await client.close()
    
    asyncio.run(run_batch())
    logger.info(f"Process {current_process().name} for ledger metadata: {start_ledger} to {end_ledger} completed")

# --- Phase 2: Fetch and Store Transactions for Existing Ledgers ---
async def _process_transactions_for_ledger(client: AsyncWebsocketClient, ledger_doc: Dict[str, Any]) -> None:
    """
    Asynchronously fetches and processes transactions for a single ledger document.
    """
    ledger_index = ledger_doc.get('ledger_index')
    ledger_hash = ledger_doc.get('ledger_hash', '')
    close_time = ledger_doc.get('close_time', 0)

    if not ledger_index:
        logger.error(f"Missing ledger_index in document: {ledger_doc} in process {current_process().name}")
        return

    logger.info(f"Fetching transactions for ledger {ledger_index} in process {current_process().name}")
    tx_data = await fetch_transactions(client, ledger_index)
    if tx_data:
        tx_processed, mm_data = process_transactions(ledger_index, ledger_hash, close_time, tx_data)
        save_to_mongo(TX_COLLECTION, tx_processed)
        save_to_mongo(META_MEMOS_COLLECTION, mm_data)

def _worker_fetch_and_process_transactions(ledger_docs_batch: List[Dict]) -> None:
    """
    Worker function for multiprocessing pool to fetch and process transactions.
    Each worker runs its own asyncio event loop.
    """
    logger.info(f"Process {current_process().name} for transactions (batch size {len(ledger_docs_batch)}) started")

    async def run_batch():
        client = None
        try:
            client = await connect_xrpl()
            semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
            async def process_with_semaphore(ledger_doc: Dict[str, Any]):
                async with semaphore:
                    await _process_transactions_for_ledger(client, ledger_doc)
            
            await asyncio.gather(*[process_with_semaphore(doc) for doc in ledger_docs_batch])
        finally:
            if client:
                await client.close()
    
    asyncio.run(run_batch())
    logger.info(f"Process {current_process().name} for transactions (batch size {len(ledger_docs_batch)}) completed")


# Main function to handle bulk fetch with timing
def bulk_fetch_ledgers(start_ledger: int, end_ledger: int) -> None:
    """
    Orchestrates the two-phase bulk fetching process:
    1. Fetches ledger metadata.
    2. Fetches transaction data for the stored ledgers.
    """
    start_time = time.time()
    try:
        total_ledgers = end_ledger - start_ledger + 1
        
        # --- Phase 1: Fetch and Store Ledger Metadata ---
        logger.info(f"Phase 1: Starting to fetch ledger metadata for {total_ledgers} ledgers.")
        ledgers_per_process = math.ceil(total_ledgers / NUM_PROCESSES)
        ranges_for_ledgers = [
            (start_ledger + i * ledgers_per_process, 
             min(start_ledger + (i + 1) * ledgers_per_process - 1, end_ledger))
            for i in range(NUM_PROCESSES)
            if start_ledger + i * ledgers_per_process <= end_ledger
        ]
        
        if ranges_for_ledgers:
            # Initialize MongoDB client in each worker process once
            with Pool(processes=NUM_PROCESSES, initializer=worker_init) as pool:
                pool.map(_worker_fetch_only_ledgers, ranges_for_ledgers)
            logger.info("Phase 1: Completed fetching and storing ledger metadata.")
        else:
            logger.warning("No ledger ranges to process for metadata.")

        # --- Phase 2: Fetch and Store Transactions for Stored Ledgers ---
        logger.info("Phase 2: Starting to fetch and store transactions for previously fetched ledgers.")
        
        # Connect to MongoDB in the main process to retrieve ledger documents
        # This is outside the pool, so it's a separate connection.
        main_mongo_client = MongoClient(MONGO_URI)
        main_db = main_mongo_client[DB_NAME]
        ledgers_collection = main_db[LEDGER_COLLECTION]

        # Retrieve only necessary fields for transaction fetching from MongoDB
        # Using a projection to minimize data transfer
        all_fetched_ledgers = list(ledgers_collection.find(
            {"ledger_index": {"$gte": start_ledger, "$lte": end_ledger}},
            {"ledger_index": 1, "ledger_hash": 1, "close_time": 1, "_id": 0}
        ))
        main_mongo_client.close() # Close the main process's connection

        if all_fetched_ledgers:
            logger.info(f"Found {len(all_fetched_ledgers)} ledgers in DB for transaction processing.")
            # Distribute these ledgers into batches for transaction processing
            transactions_per_process = math.ceil(len(all_fetched_ledgers) / NUM_PROCESSES)
            transaction_batches = [
                all_fetched_ledgers[i:i + transactions_per_process]
                for i in range(0, len(all_fetched_ledgers), transactions_per_process)
            ]

            # Initialize MongoDB client in each worker process once
            with Pool(processes=NUM_PROCESSES, initializer=worker_init) as pool:
                pool.map(_worker_fetch_and_process_transactions, transaction_batches)
            logger.info("Phase 2: Completed fetching and storing transactions.")
        else:
            logger.warning("No ledgers found in DB to process transactions for.")
        
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Bulk fetch completed in {execution_time:.2f} seconds")

    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.error(f"Bulk fetch failed after {execution_time:.2f} seconds: {e}")
        sys.exit(1)

# Main execution
if __name__ == "__main__":
    try:
        start_ledger, end_ledger = get_ledger_range()
        bulk_fetch_ledgers(start_ledger, end_ledger)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
