import asyncio
import json
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import uuid
import aiohttp
from aiohttp import ClientSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MongoDB client with connection pooling
mongo_client = MongoClient('mongodb://localhost:27017', maxPoolSize=50)
db = mongo_client['ripple']
transactions_collection = db['transaction hash data fetch']
ledgers_collection = db['ledgers hash data fetch']

# Rate limiting parameters
RATE_LIMIT = 20  # requests per second
RETRY_ATTEMPTS = 3
INITIAL_BACKOFF = 1  # seconds
MAX_TRANSACTION_RETRIES = 3  # Max retries for failed transactions

def process_transaction_data(data):
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            if k in ("Amount", "DeliverMin") and isinstance(v, str) and v.isdigit():
                xrp_value = float(v) / 1_000_000
                new_data[k] = xrp_value
                new_data[f"{k}_currency"] = "XRP"
            elif k == "Fee" and isinstance(v, str) and v.isdigit():
                new_data[k] = v
            elif k == "TakerPays" and isinstance(v, str) and v.isdigit():
                xrp_value = float(v) / 1_000_000
                new_data[k] = v
                new_data["TakerPays_converted"] = xrp_value
                new_data["TakerPays_currency"] = "XRP"
            elif k == "date" and isinstance(v, int):
                ripple_epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)
                utc_datetime = ripple_epoch + timedelta(seconds=v)
                new_data[k] = utc_datetime.isoformat()
            else:
                new_data[k] = process_transaction_data(v)
        return new_data
    elif isinstance(data, list):
        return [process_transaction_data(item) for item in data]
    elif isinstance(data, float):
        return float(data)
    else:
        return data

async def rate_limited_request(semaphore, session, request_data, url, retry_count=0):
    async with semaphore:
        try:
            async with session.ws_connect(url, heartbeat=30.0, timeout=aiohttp.ClientTimeout(total=30)) as websocket:
                await websocket.send_json(request_data)
                async for msg in websocket:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        return json.loads(msg.data)
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.error("WebSocket closed unexpectedly")
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"WebSocket error: {websocket.exception()}")
                        break
        except aiohttp.ClientResponseError as e:
            if e.status == 429 and retry_count < RETRY_ATTEMPTS:
                backoff = INITIAL_BACKOFF * (2 ** retry_count)
                logger.warning(f"429 error, retrying after {backoff}s")
                await asyncio.sleep(backoff)
                return await rate_limited_request(semaphore, session, request_data, url, retry_count + 1)
            logger.error(f"Request error: {e}")
            raise
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            if retry_count < RETRY_ATTEMPTS:
                backoff = INITIAL_BACKOFF * (2 ** retry_count)
                logger.warning(f"Retrying after {backoff}s due to error: {e}")
                await asyncio.sleep(backoff)
                return await rate_limited_request(semaphore, session, request_data, url, retry_count + 1)
            raise

async def get_transaction_count(ledger_index, session):
    url = "wss://s2.ripple.com"
    semaphore = asyncio.Semaphore(RATE_LIMIT)
    request_data = {
        "command": "ledger",
        "ledger_index": ledger_index,
        "transactions": True,
        "expand": False,
        "owner_funds": False,
        "api_version": 2
    }
    try:
        response_data = await rate_limited_request(semaphore, session, request_data, url)
        if 'result' in response_data and 'ledger' in response_data['result']:
            ledger_data = response_data['result']['ledger']
            ledger_info = {
                '_id': str(uuid.uuid4()),
                'ledger_index': ledger_index,
                'ledger_hash': ledger_data.get('ledger_hash'),
                'close_time': ledger_data.get('close_time_human'),
                'transaction_count': len(ledger_data.get('transactions', [])),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            ledgers_collection.insert_one(ledger_info)
            return ledger_data.get('transactions', [])
        else:
            logger.error(f"Error fetching ledger {ledger_index}: {response_data.get('error_message', 'Unknown error')}")
            return []
    except Exception as e:
        logger.error(f"Error in get_transaction_count: {e}")
        return []

async def fetch_transaction_details(tx_hash, success_counter, failure_counter, session, retry_attempt=0):
    url = "wss://xrplcluster.com"
    semaphore = asyncio.Semaphore(RATE_LIMIT)
    request_data = {
        "command": "tx",
        "transaction": tx_hash,
        "api_version": 2
    }
    try:
        response_data = await rate_limited_request(semaphore, session, request_data, url)
        response_data.update({
            "api_version": 2,
            "status": "success" if response_data.get('result', {}).get('validated') else "error",
            "type": "response",
            "warnings": [
                {
                    "id": 2001,
                    "message": "This is a clio server. clio only serves validated data."
                }
            ]
        })
        if 'result' in response_data and response_data['result'].get('validated'):
            success_counter[0] += 1
            return process_transaction_data(response_data)
        else:
            logger.error(f"Error fetching transaction {tx_hash}: {response_data.get('error_message', 'Unknown error')}")
            failure_counter[0] += 1
            return None
    except Exception as e:
        logger.error(f"Error fetching transaction {tx_hash} (attempt {retry_attempt + 1}): {e}")
        failure_counter[0] += 1
        return None

def save_to_mongodb(transactions):
    try:
        for tx in transactions:
            tx['_id'] = str(uuid.uuid4())
        transactions_collection.insert_many(transactions)
    except Exception as e:
        logger.error(f"MongoDB error: {e}")

async def fetch_ledger_transactions(ledger_index):
    async with ClientSession() as session:
        transactions = await get_transaction_count(ledger_index, session)
        if not transactions:
            logger.info(f"No transactions found in ledger {ledger_index}")
            return

        logger.info(f"Found {len(transactions)} transactions in ledger {ledger_index}")

        success_counter = [0]
        failure_counter = [0]
        transaction_data_list = []
        retry_queue = [(tx_hash, 0) for tx_hash in transactions]  # (tx_hash, retry_count)

        # Batch processing with retries
        batch_size = 50
        while retry_queue:
            current_batch = retry_queue[:batch_size]
            retry_queue = retry_queue[batch_size:]

            tasks = [
                fetch_transaction_details(tx_hash, success_counter, failure_counter, session, retry_count)
                for tx_hash, retry_count in current_batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (tx_hash, retry_count), result in zip(current_batch, results):
                if isinstance(result, Exception) or result is None:
                    if retry_count + 1 < MAX_TRANSACTION_RETRIES:
                        logger.info(f"Retrying transaction {tx_hash} (attempt {retry_count + 2})")
                        retry_queue.append((tx_hash, retry_count + 1))
                    else:
                        logger.error(f"Transaction {tx_hash} failed after {MAX_TRANSACTION_RETRIES} attempts")
                else:
                    transaction_data_list.append((tx_hash, result))

        transaction_data_list.sort(key=lambda x: x[1].get('Sequence', float('inf')))

        valid_transactions = [tx_data for _, tx_data in transaction_data_list]
        if valid_transactions:
            logger.info(f"Saving {len(valid_transactions)} transactions to MongoDB")
            save_to_mongodb(valid_transactions)

        logger.info(f"Successful: {success_counter[0]}, Failed: {failure_counter[0]}")

if __name__ == "__main__":
    try:
        ledger_index = input("Enter ledger index: ").strip()
        if ledger_index != "validated" and not ledger_index.isdigit():
            logger.error("Invalid ledger index. Use a numeric value or 'validated'.")
        else:
            asyncio.run(fetch_ledger_transactions(ledger_index))
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    except Exception as e:
        logger.error(f"Error running script: {e}")
    finally:
        mongo_client.close()