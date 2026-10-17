import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import uuid
import itertools

# Initialize MongoDB client once
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['ripple']
collection = db['transactions']

# Define WebSocket servers
SERVERS = ["wss://xrplcluster.com", "wss://s2.ripple.com"]
server_cycle = itertools.cycle(SERVERS)  # Cycle through servers

def process_transaction_data(data):
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            if k in ("Amount", "DeliverMin") and isinstance(v, str) and v.isdigit():
                xrp_value = float(v) / 1_000_000
                new_data[k] = xrp_value
                new_data[f"{k}_currency"] = "XRP"
            elif k == "Fee" and isinstance(v, str) and v.isdigit():
                new_data[k] = v  # Keep Fee as string
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

async def get_transaction_count(ledger_index):
    server = next(server_cycle)  # Select next server
    print(f"Fetching ledger {ledger_index} from {server}")
    try:
        async with websockets.connect(server, ping_interval=20, ping_timeout=60) as websocket:
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
                transactions = response_data['result']['ledger'].get('transactions', [])
                return transactions
            else:
                print(f"Error fetching ledger {ledger_index} from {server}: {response_data.get('error_message', 'Unknown error')}")
                return []
    except Exception as e:
        print(f"WebSocket error in get_transaction_count from {server}: {e}")
        return []

async def fetch_transaction_details(tx_hash, success_counter, failure_counter, semaphore, max_retries=3):
    async with semaphore:  # Limit concurrent requests
        for attempt in range(max_retries):
            server = next(server_cycle)  # Select next server
            print(f"Fetching transaction {tx_hash} from {server} (Attempt {attempt + 1}/{max_retries})")
            try:
                async with websockets.connect(server, ping_interval=20, ping_timeout=60) as websocket:
                    request_data = {
                        "command": "tx",
                        "transaction": tx_hash,
                        "api_version": 2
                    }
                    await websocket.send(json.dumps(request_data))
                    response = await websocket.recv()
                    response_data = json.loads(response)
                    # Add response metadata
                    response_data.update({
                        "api_version": 2,
                        "status": "success" if response_data.get('result', {}).get('validated') else "error",
                        "type": "response",
                        "warnings": [
                            {
                                "id": 2001,
                                "message": "This is a clio server. clio only serves validated data. If you want to talk to rippled, include 'ledger_index':'current' in your request"
                            }
                        ]
                    })
                    if 'result' in response_data and response_data['result'].get('validated'):
                        success_counter[0] += 1
                        return process_transaction_data(response_data)
                    else:
                        print(f"Error fetching transaction {tx_hash} from {server}: {response_data.get('error_message', 'Unknown error')}")
                        failure_counter[0] += 1
                        return None
            except Exception as e:
                if "HTTP 429" in str(e):
                    if attempt < max_retries - 1:
                        delay = 0.5 * (2 ** attempt)  # Faster backoff: 0.5s, 1s, 2s
                        print(f"Rate limit hit for {tx_hash} on {server}, retrying after {delay} seconds...")
                        await asyncio.sleep(delay)
                        continue
                print(f"WebSocket error for transaction {tx_hash} from {server}: {e}")
                failure_counter[0] += 1
                return None
        print(f"Max retries reached for transaction {tx_hash}")
        failure_counter[0] += 1
        return None

def save_to_mongodb(transaction_data_list):
    if not transaction_data_list:
        return
    try:
        # Add unique IDs for MongoDB
        for data in transaction_data_list:
            data['_id'] = str(uuid.uuid4())
        collection.insert_many(transaction_data_list)
        print(f"Saved {len(transaction_data_list)} transactions to MongoDB")
    except Exception as e:
        print(f"MongoDB error: {e}")

async def fetch_ledger_transactions(ledger_index):
    if ledger_index != "validated" and not ledger_index.isdigit():
        print("Invalid ledger index. Use a numeric value or 'validated'.")
        return

    transactions = await get_transaction_count(ledger_index)
    if not transactions:
        print(f"No transactions found in ledger {ledger_index}")
        return

    print(f"Found {len(transactions)} transactions in ledger {ledger_index}")
    try:
        user_input = input("Do you want to fetch and process these transactions? (yes/no): ").strip().lower()
    except EOFError:
        print("Input interrupted. Defaulting to 'no'.")
        user_input = 'no'

    if user_input not in ('yes', 'no'):
        print("Invalid input. Defaulting to 'no'.")
        user_input = 'no'

    if user_input == 'no':
        return

    success_counter = [0]
    failure_counter = [0]
    transaction_data_list = []
    semaphore = asyncio.Semaphore(5)  # Allow up to 5 concurrent requests

    # Fetch transactions concurrently with semaphore
    tasks = [fetch_transaction_details(tx_hash, success_counter, failure_counter, semaphore) for tx_hash in transactions]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Collect valid results
    for tx_hash, result in zip(transactions, results):
        if isinstance(result, Exception) or result is None:
            continue
        transaction_data_list.append((tx_hash, result))

    # Sort transactions by Sequence
    transaction_data_list.sort(key=lambda x: x[1].get('Sequence', float('inf')))

    # Save to MongoDB in batch
    if transaction_data_list:
        batch_data = [data for _, data in transaction_data_list]
        save_to_mongodb(batch_data)

    # Log processed transactions
    for i, (tx_hash, _) in enumerate(transaction_data_list, 1):
        print(f"Processed transaction {i}/{len(transaction_data_list)}: {tx_hash}")

    print(f"Completed: Successful: {success_counter[0]}, Failed: {failure_counter[0]}")

if __name__ == "__main__":
    try:
        ledger_index = input("Enter ledger index: ").strip()
    except EOFError:
        print("Input interrupted. Exiting.")
        ledger_index = None

    if ledger_index:
        try:
            asyncio.run(fetch_ledger_transactions(ledger_index))
        except KeyboardInterrupt:
            print("Script interrupted by user.")
        except Exception as e:
            print(f"Error running script: {e}")
        finally:
            mongo_client.close()    