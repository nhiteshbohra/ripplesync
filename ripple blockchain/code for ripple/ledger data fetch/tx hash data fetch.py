import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import uuid

# Initialize MongoDB client once
mongo_client = MongoClient('mongodb://localhost:27017')
db = mongo_client['ripple']
collection = db['transactions_tx_hash']

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

async def fetch_transaction_details(tx_hash, success_counter, failure_counter):
    url = "wss://xrplcluster.com"
    try:
        async with websockets.connect(url) as websocket:
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
                print(f"Error fetching transaction {tx_hash}: {response_data.get('error_message', 'Unknown error')}")
                failure_counter[0] += 1
                return None
    except Exception as e:
        print(f"WebSocket error for transaction {tx_hash}: {e}")
        failure_counter[0] += 1
        return None

def save_to_mongodb(transaction_data):
    try:
        transaction_data['_id'] = str(uuid.uuid4())  # Add unique ID for MongoDB
        collection.insert_one(transaction_data)
        print("Transaction saved to MongoDB")
    except Exception as e:
        print(f"MongoDB error: {e}")

async def fetch_transaction_by_hash(tx_hash):
    if not tx_hash or len(tx_hash) != 64 or not all(c in '0123456789ABCDEFabcdef' for c in tx_hash):
        print("Invalid transaction hash. It must be a 64-character hexadecimal string.")
        return

    success_counter = [0]
    failure_counter = [0]

    print(f"Fetching transaction: {tx_hash}")
    transaction_data = await fetch_transaction_details(tx_hash, success_counter, failure_counter)

    if transaction_data:
        print("Transaction fetched successfully")
        try:
            user_input = input("Do you want to save this transaction to MongoDB? (yes/no): ").strip().lower()
        except EOFError:
            print("Input interrupted. Defaulting to 'no'.")
            user_input = 'no'

        if user_input not in ('yes', 'no'):
            print("Invalid input. Defaulting to 'no'.")
            user_input = 'no'

        if user_input == 'yes':
            save_to_mongodb(transaction_data)

    print(f"Successful: {success_counter[0]}, Failed: {failure_counter[0]}")

if __name__ == "__main__":
    try:
        tx_hash = input("Enter transaction hash: ").strip()
    except EOFError:
        print("Input interrupted. Exiting.")
        tx_hash = None

    if tx_hash:
        try:
            asyncio.run(fetch_transaction_by_hash(tx_hash))
        except KeyboardInterrupt:
            print("Script interrupted by user.")
        except Exception as e:
            print(f"Error running script: {e}")
        finally:
            mongo_client.close()