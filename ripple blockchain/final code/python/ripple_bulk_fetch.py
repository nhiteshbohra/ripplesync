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

# Get user input for ledger range
def get_ledger_range() -> tuple[int, int]:
    while True:
        try:
            start_ledger = input("Enter the start ledger number: ").strip()
            end_ledger = input("Enter the end ledger number: ").strip()
            
            start_ledger = int(start_ledger)
            end_ledger = int(end_ledger)
            
            if start_ledger < 32750 or end_ledger < 32570:
                logger.error("Ledger numbers must be greater than.")
                continue
            if start_ledger > end_ledger:
                logger.error("Start ledger must be less than or equal to end ledger.")
                continue
                
            if end_ledger - start_ledger + 1 > MAX_LEDGER_RANGE:
                logger.warning(f"Ledger range exceeds {MAX_LEDGER_RANGE}. Will process in batches.")
                
            return start_ledger, end_ledger
        except ValueError:
            logger.error("Please enter valid integer numbers for ledger range.")

# Connect to XRP Ledger server with fallback
async def connect_xrpl() -> AsyncWebsocketClient:
    for server in SERVERS:
        try:
            client = AsyncWebsocketClient(server)
            await client.open()
            logger.info(f"Connected to XRPL server: {server}")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to {server}: {e}")
    raise Exception("All XRPL servers failed to connect")

# Fetch ledger data
async def fetch_ledger(client: AsyncWebsocketClient, ledger_index: int, retries: int = 0) -> Dict[str, Any]:
    try:
        request = Ledger(
            ledger_index=ledger_index,
            transactions=True,
            expand=True
        )
        response = await client.request(request)
        ledger = response.result.get("ledger")
        if not ledger:
            raise ValueError("No ledger data returned")

        return {
            "ledger_index": int(ledger["ledger_index"]),
            "ledger_hash": ledger["ledger_hash"],
            "parent_hash": ledger["parent_hash"],
            "txs_hash": ledger["transaction_hash"],
            "properties": ledger,
            "closed_on": ledger["close_time"],
            "total_coins": int(ledger["total_coins"]),
            "txs_included": len(ledger.get("transactions", [])),
            "fee_burned": calculate_fee_burned(ledger.get("transactions", [])),
            "timestamp": datetime.fromtimestamp(ledger["close_time"] + 946684800, timezone.utc).isoformat()
        }
    except Exception as e:
        if retries < MAX_RETRIES:
            logger.warning(f"Retrying ledger {ledger_index} ({retries + 1}/{MAX_RETRIES})")
            await asyncio.sleep(1)
            return await fetch_ledger(client, ledger_index, retries + 1)
        logger.error(f"Failed to fetch ledger {ledger_index}: {str(e)}")
        return None

# Calculate total fee burned
def calculate_fee_burned(transactions: List[Dict]) -> int:
    return sum(int(tx.get("Fee", 0)) for tx in transactions if tx.get("Fee"))

# Process transaction data
def process_transactions(ledger: Dict) -> List[Dict]:
    transactions = ledger.get("transactions", [])
    if not transactions:
        return []

    ledger_index = int(ledger["ledger_index"])
    ledger_hash = ledger["ledger_hash"]
    close_time_iso = datetime.fromtimestamp(ledger["close_time"] + 946684800, timezone.utc).isoformat()

    return [
        {
            "close_time_iso": close_time_iso,
            "tx_json": tx,
            "date": close_time_iso,
            "status": "success" if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else "failed"
        }
        for index, tx in enumerate(transactions)
    ]

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
            collection.update_one(
                {"ledger_index": data["ledger_index"]},
                {"$set": data},
                upsert=True
            )
            logger.info(f"Saved ledger {data['ledger_index']} to {collection_name}")
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
            tx_data = process_transactions(ledger_data["properties"])
            transactions.extend(tx_data)

    save_to_mongo(LEDGER_COLLECTION, ledgers)
    save_to_mongo(TX_COLLECTION, transactions)

# Main function to handle bulk fetch
async def bulk_fetch_ledgers(start_ledger: int, end_ledger: int) -> None:
    if end_ledger - start_ledger + 1 > MAX_LEDGER_RANGE:
        logger.warning(f"Ledger range exceeds {MAX_LEDGER_RANGE}. Processing in batches.")

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