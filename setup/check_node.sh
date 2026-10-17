#!/usr/bin/env bash
# Show xrpld sync progress via the ingest package (needs the venv and .env).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source .venv/bin/activate
python - <<'PY'
import asyncio, os
from dotenv import load_dotenv
from xrpl_ingest.source import WsSource
load_dotenv(os.path.join(os.getcwd(), ".env"))

async def main():
    s = WsSource([u.strip() for u in os.environ.get("SERVERS", "ws://127.0.0.1:6006").replace("\n", ",").split(",") if u.strip()],
                 request_timeout=10, connect_retries=1)
    try:
        info = await s.server_info()
    finally:
        await s.close()
    print("server_state     :", info.get("server_state"))
    print("network_id       :", info.get("network_id"))
    print("complete_ledgers :", info.get("complete_ledgers"))
    print("validated seq    :", info.get("validated_ledger", {}).get("seq"))
    print("build_version    :", info.get("build_version"))

asyncio.run(main())
PY
