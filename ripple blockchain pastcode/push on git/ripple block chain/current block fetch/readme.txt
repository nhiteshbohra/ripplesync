XRPL Data Grabber
This tool helps you fetch and save XRP Ledger (XRPL) transaction data into your MongoDB database.

What's Inside (Key Files)
current transaction fetch.py: For real-time updates. Continuously gets the newest XRPL data.

BULK LEDGERS FETCH.py: For large historical updates. Fetches many ledgers at once.

SINGLE LEDGER FETCH.py: For specific updates. Gets data for one particular ledger.

Quick Setup
Install:

pip install -r requirements.txt

Add your MongoDB and XRPL server details there (e.g., MONGO_URI="mongodb://localhost:27017/").

Control current transaction fetch.py:

Use process_status.txt: Set current_process=1 to run, current_process=0 to stop.

How to Run
For current transaction fetch.py:

python "current transaction fetch.py"

For others: Run them directly and check their file for specific options (like ledger numbers).