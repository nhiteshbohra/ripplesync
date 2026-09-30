import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any
from multiprocessing import Pool
import math
import time

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xrpl_data_4"
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

# Schema definitions
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
}

META_MEMOS_SCHEMA_FIELDS = {
    'ledger_index': int,
    'ledger_hash': str,
    'transaction_hash': str,
    'memos': list,
    'close_time_iso': str,
    '_inserted_at': datetime
}

# Get user input for ledger range
def get_ledger_range() -> tuple[int, int]:
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

# Process transaction data (exclude meta field)
def process_transactions(ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    if not transactions:
        return [], []

    close_time_iso = datetime.fromtimestamp(close_time + 946684800, timezone.utc).isoformat() if close_time else ""

    processed_txs = []
    meta_memos = []
    
    for tx in transactions:
        # Process transaction data (excluding meta)
        processed_tx = {
            "ledger_index": ledger_index,
            "ledger_hash": ledger_hash,
            "close_time_iso": close_time_iso,
            "tx_json": {k: v for k, v in tx.items() if k != 'meta'},  # Exclude meta
            "date": close_time_iso,
            "status": "success" if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else "failed"
        }
        
        # Validate transaction fields
        for field, field_type in TX_SCHEMA_FIELDS.items():
            if field in tx:
                try:
                    processed_tx[field] = field_type(tx[field]) if tx[field] is not None else None
                except (ValueError, TypeError):
                    logger.warning(f"Invalid {field} format in transaction {tx.get('hash', '')}")
        
        processed_txs.append(processed_tx)
        
        # Process memos only
        meta_memo_data = {
            "ledger_index": ledger_index,
            "ledger_hash": ledger_hash,
            "transaction_hash": tx.get("hash", ""),
            "memos": tx.get("Memos", []),
            "close_time_iso": close_time_iso,
            "_inserted_at": datetime.now(timezone.utc)
        }
        
        # Validate memos fields
        for field, field_type in META_MEMOS_SCHEMA_FIELDS.items():
            if field in meta_memo_data and field != '_inserted_at':
                try:
                    meta_memo_data[field] = field_type(meta_memo_data[field]) if meta_memo_data[field] is not None else None
                except (ValueError, TypeError):
                    logger.warning(f"Invalid {field} format in memos for transaction {tx.get('hash', '')}")
        
        meta_memos.append(meta_memo_data)
    
    return processed_txs, meta_memos

# Save data to MongoDB
def save_to_mongo(collection_name: str, data: Any) -> None:
    if not data:
        return
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        collection = db[collection_name]
        if isinstance(data, list) and data:
            collection.insert_many(data, ordered=False)
            logger.info(f"Saved {len(data)} documents to {collection_name}")
        elif isinstance(data, dict):
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
            logger.info(f"Saved ledger {validated_data.get('ledger_index')} to {collection_name}")
        mongo_client.close()
    except Exception as e:
        logger.error(f"Failed to save to {collection_name}: {e}")

# Process a batch of ledgers
async def process_ledger_batch(client: AsyncWebsocketClient, start_ledger: int, end_ledger: int) -> None:
    ledgers = []
    transactions = []
    meta_memos = []

    for ledger_index in range(start_ledger, end_ledger + 1):
        logger.info(f"Fetching ledger {ledger_index}")
        ledger_data = await fetch_ledger(client, ledger_index)
        if ledger_data:
            ledgers.append(ledger_data)
            ledger_hash = ledger_data.get("ledger_hash", "")
            close_time = ledger_data.get("close_time", 0)
            tx_data = await fetch_transactions(client, ledger_index)
            if tx_data:
                tx_processed, mm_data = process_transactions(ledger_index, ledger_hash, close_time, tx_data)
                transactions.extend(tx_processed)
                meta_memos.extend(mm_data)

    save_to_mongo(LEDGER_COLLECTION, ledgers)
    save_to_mongo(TX_COLLECTION, transactions)
    save_to_mongo(META_MEMOS_COLLECTION, meta_memos)

# Worker function for each process
def process_batch(batch: tuple) -> None:
    start_ledger, end_ledger = batch
    logger.info(f"Process {batch} started for ledgers {start_ledger} to {end_ledger}")
    
    async def run_batch():
        client = await connect_xrpl()
        try:
            semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
            async def process_with_semaphore(start: int, end: int):
                async with semaphore:
                    logger.info(f"Processing sub-batch: {start} to {end}")
                    await process_ledger_batch(client, start, end)
            
            sub_ranges = [(i, min(i + MAX_LEDGER_RANGE - 1, end_ledger))
                         for i in range(start_ledger, end_ledger + 1, MAX_LEDGER_RANGE)]
            await asyncio.gather(*[process_with_semaphore(start, end) for start, end in sub_ranges])
        finally:
            await client.close()
    
    asyncio.run(run_batch())
    logger.info(f"Process {batch} completed")

# Main function to handle bulk fetch with timing
def bulk_fetch_ledgers(start_ledger: int, end_ledger: int) -> None:
    start_time = time.time()
    try:
        total_ledgers = end_ledger - start_ledger + 1
        ledgers_per_process = math.ceil(total_ledgers / NUM_PROCESSES)
        ranges = [
            (start_ledger + i * ledgers_per_process, 
             min(start_ledger + (i + 1) * ledgers_per_process - 1, end_ledger))
            for i in range(NUM_PROCESSES)
            if start_ledger + i * ledgers_per_process <= end_ledger
        ]

        logger.info(f"Distributing {len(ranges)} batches across {NUM_PROCESSES} processes")
        
        with Pool(processes=NUM_PROCESSES) as pool:
            pool.map(process_batch, ranges)
        
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