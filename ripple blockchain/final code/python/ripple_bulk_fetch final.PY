import asyncio
import logging
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models.requests import Ledger
from pymongo import MongoClient
from datetime import datetime, timezone
import sys
from typing import List, Dict, Any

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xrpl_data"
LEDGER_COLLECTION = "ledgers"
TX_COLLECTION = "transactions"
SERVERS = [
    "wss://s2.ripple.com",
    "wss://xrplcluster.com",
    "wss://s.altnet.rippletest.net:51233"  # Fallback testnet
]
MAX_LEDGER_RANGE = 50
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10  # seconds
BATCH_CONCURRENCY = 2

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MongoDB setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

# Schema definitions
CURRENCY_FIELDS = [
    'Amount', 'TakerGets', 'TakerPays', 'SendMax', 
    'LimitAmount', 'SendMin', 'DeliverMin', 'NFTokenBrokerFee'
]

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
    # Add other fields as needed
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

# Fetch ledger data
async def fetch_ledger(client: AsyncWebsocketClient, ledger_index: int) -> Dict[str, Any]:
    for attempt in range(MAX_RETRIES):
        try:
            request = Ledger(
                ledger_index=str(ledger_index),
                transactions=True,
                expand=True
            )
            response = await asyncio.wait_for(client.request(request), timeout=REQUEST_TIMEOUT)
            return response.result.get('ledger', {})
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to fetch ledger {ledger_index}: {e}")
            if attempt == MAX_RETRIES - 1:
                return {}
    return {}

# Calculate total fee burned
def calculate_fee_burned(transactions: List[Dict]) -> int:
    return sum(int(tx.get("Fee", 0)) for tx in transactions if tx.get("Fee"))

# Process transaction data
def process_transactions(ledger: Dict) -> List[Dict]:
    transactions = ledger.get("transactions", [])
    if not transactions:
        return []

    ledger_index = int(ledger.get("ledger_index", 0))
    ledger_hash = ledger.get("ledger_hash", "")
    close_time = ledger.get("close_time", 0)
    close_time_iso = datetime.fromtimestamp(close_time + 946684800, timezone.utc).isoformat() if close_time else ""

    processed_txs = []
    for tx in transactions:
        processed_tx = {
            "ledger_index": ledger_index,
            "ledger_hash": ledger_hash,
            "close_time_iso": close_time_iso,
            "tx_json": tx,
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
    
    return processed_txs

# Save data to MongoDB
def save_to_mongo(collection_name: str, data: Any) -> None:
    if not data:
        return
    try:
        collection = db[collection_name]
        if isinstance(data, list) and data:
            collection.insert_many(data, ordered=False)
            logger.info(f"Saved {len(data)} documents to {collection_name}")
        elif isinstance(data, dict):
            # Validate ledger fields
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
    except Exception as e:
        logger.error(f"Failed to save to {collection_name}: {e}")

# Process a batch of ledgers
async def process_ledger_batch(client: AsyncWebsocketClient, start_ledger: int, end_ledger: int) -> None:
    ledgers = []
    transactions = []

    for ledger_index in range(start_ledger, end_ledger + 1):
        logger.info(f"Fetching ledger {ledger_index}")
        ledger_data = await fetch_ledger(client, ledger_index)
        if ledger_data:
            ledgers.append(ledger_data)
            tx_data = process_transactions(ledger_data)
            transactions.extend(tx_data)

    save_to_mongo(LEDGER_COLLECTION, ledgers)
    save_to_mongo(TX_COLLECTION, transactions)

# Main function to handle bulk fetch
async def bulk_fetch_ledgers(start_ledger: int, end_ledger: int) -> None:
    if end_ledger - start_ledger + 1 > MAX_LEDGER_RANGE:
        logger.warning(f"Ledger range exceeds {MAX_LEDGER_RANGE}. Processing in batches")

    client = await connect_xrpl()
    try:
        # Split into batches
        ranges = [
            (i, min(i + MAX_LEDGER_RANGE - 1, end_ledger))
            for i in range(start_ledger, end_ledger + 1, MAX_LEDGER_RANGE)
        ]

        # Process batches with concurrency limit
        semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
        async def process_with_semaphore(batch: tuple) -> None:
            async with semaphore:
                logger.info(f"Processing batch: {batch[0]} to {batch[1]}")
                await process_ledger_batch(client, batch[0], batch[1])

        await asyncio.gather(*[process_with_semaphore(batch) for batch in ranges])
        logger.info("Bulk fetch completed successfully")
    except Exception as e:
        logger.error(f"Bulk fetch failed: {e}")
    finally:
        await client.close()
        mongo_client.close()
        logger.info("Disconnected from XRPL and MongoDB")

# Main execution
if __name__ == "__main__":
    try:
        start_ledger, end_ledger = get_ledger_range()
        asyncio.run(bulk_fetch_ledgers(start_ledger, end_ledger))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)