# Secure and Optimized Ripple Blockchain Transaction Fetcher

import asyncio
import websockets
import json
from pymongo import MongoClient, errors
from datetime import datetime, timezone, timedelta
import uuid
import itertools
import ssl

# Initialize MongoDB client once
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['ripple']
collection = db['transactions']

# Define WebSocket servers
SERVERS = ["wss://xrplcluster.com", "wss://s2.ripple.com"]
server_cycle = itertools.cycle(SERVERS)

# SSL context for secure WebSocket connections
ssl_context = ssl.create_default_context()

async def validate_ledger_index(index):
    if index == "validated" or index.isdigit():
        return index
    raise ValueError("Invalid ledger index. Must be a number or 'validated'.")

async def safe_websocket_connect(url):
    try:
        return await websockets.connect(url, ssl=ssl_context, ping_interval=20, ping_timeout=60)
    except Exception as e:
        print(f"Error connecting to {url}: {e}")
        return None

async def get_transaction_count(ledger_index):
    server = next(server_cycle)
    print(f"Fetching ledger {ledger_index} from {server}")
    websocket = await safe_websocket_connect(server)
    if not websocket:
        print("Failed to establish WebSocket connection.")
        return []
    try:
        async with websocket:
            request_data = {
                "command": "ledger",
                "ledger_index": ledger_index,
                "transactions": True,
                "expand": False,
                "owner_funds": False,
                "api_version": 2
            }
            await websocket.send(json.dumps(request_data))
            response = await websocket.recv()
            response_data = json.loads(response)
            if 'result' in response_data and 'ledger' in response_data['result']:
                return response_data['result']['ledger'].get('transactions', [])
            print(f"No transactions found: {response_data.get('error_message', 'Unknown error')}")
            return []
    except Exception as e:
        print(f"WebSocket error in get_transaction_count: {e}")
        return []

async def fetch_transaction_details(tx_hash, success_counter, failure_counter, semaphore):
    async with semaphore:
        server = next(server_cycle)
        print(f"Fetching transaction {tx_hash} from {server}")
        websocket = await safe_websocket_connect(server)
        if not websocket:
            print("Failed to establish WebSocket connection.")
            failure_counter[0] += 1
            return None
        try:
            async with websocket:
                request_data = {
                    "command": "tx",
                    "transaction": tx_hash,
                    "api_version": 2
                }
                await websocket.send(json.dumps(request_data))
                response = await websocket.recv()
                response_data = json.loads(response)
                if 'result' in response_data and response_data['result'].get('validated'):
                    success_counter[0] += 1
                    return response_data
                print(f"Transaction {tx_hash} not validated: {response_data.get('error_message', 'Unknown error')}")
                failure_counter[0] += 1
                return None
        except Exception as e:
            print(f"WebSocket error for transaction {tx_hash}: {e}")
            failure_counter[0] += 1
            return None

async def fetch_ledger_transactions(ledger_index):
    try:
        ledger_index = await validate_ledger_index(ledger_index)
    except ValueError as e:
        print(e)
        return

    transactions = await get_transaction_count(ledger_index)
    if not transactions:
        print(f"No transactions found in ledger {ledger_index}")
        return

    print(f"Found {len(transactions)} transactions in ledger {ledger_index}")
    transaction_data_list = []
    semaphore = asyncio.Semaphore(5)
    success_counter = [0]
    failure_counter = [0]

    tasks = [fetch_transaction_details(tx_hash, success_counter, failure_counter, semaphore) for tx_hash in transactions]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for tx_hash, result in zip(transactions, results):
        if isinstance(result, Exception) or result is None:
            continue
        transaction_data_list.append(result)

    if transaction_data_list:
        save_to_mongodb(transaction_data_list)
    print(f"Successfully fetched {success_counter[0]} transactions, {failure_counter[0]} failed.")

if __name__ == "__main__":
    ledger_index = input("Enter ledger index: ").strip()
    asyncio.run(fetch_ledger_transactions(ledger_index))
