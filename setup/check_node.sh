#!/usr/bin/env bash
# Show xrpld sync progress via the ingest package (needs the venv and .env).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source .venv/bin/activate
python - <<'PY'
import asyncio, os, re
from dotenv import load_dotenv
from xrpl_ingest.source import WsSource
load_dotenv(os.path.join(os.getcwd(), ".env"))

# Same parsing as xrpl_ingest/config.py (Config.from_env needs MONGO_URI, which this check does not).
servers = [u for u in re.split(r"[,\s]+", (os.environ.get("SERVERS") or "").strip("\"' ")) if u]
net_raw = (os.environ.get("EXPECTED_NETWORK_ID") or "").strip().lower()
network_id = None if net_raw in ("none", "off", "any") else int(net_raw or 0)

async def main():
    s = WsSource(servers or ["ws://127.0.0.1:6006"], request_timeout=10, connect_retries=1,
                 expected_network_id=network_id)
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
