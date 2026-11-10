"""Quick plumbing check: real status + doctor --node against Clio + local Mongo."""
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reviews._clio_driver import (
    EVIDENCE, MONGO_URI, SERVERS, check_mongo, make_env, node_ready, node_snapshot, run_cli,
    save, stamp,
)

db = "xrpl_ingest_plumb_" + uuid.uuid4().hex[:8]
base = EVIDENCE / ("base_plumb_" + db)
base.mkdir(exist_ok=True)
result = {"started_utc": stamp(), "database": db, "servers": SERVERS, "checks": []}
try:
    check_mongo(MONGO_URI)
    env = make_env(db, base, SERVERS)
    info = node_snapshot()
    result["node"] = {
        "server_state": info.get("server_state"),
        "clio_version": info.get("clio_version"),
        "validated_seq": (info.get("validated_ledger") or {}).get("seq"),
        "age": (info.get("validated_ledger") or {}).get("age"),
        "complete": info.get("complete_ledgers"),
        "ready": node_ready(info),
    }
    print("=== node snapshot ===")
    print(json.dumps(result["node"], indent=2))

    for label, args in (("status", ("status",)), ("doctor_node", ("doctor", "--node"))):
        code, log = run_cli(env, *args, timeout=120)
        result["checks"].append({"name": label, "exit": code, "log_tail": log[-1500:]})
        print(f"\n=== {label} ===")
        print("exit =", code)
        print(log[-1500:])
    result["outcome"] = "passed" if (
        result["node"]["ready"] and all(check["exit"] == 0 for check in result["checks"])
    ) else "failed"
except BaseException as exc:
    result.update(outcome="failed", error=f"{type(exc).__name__}: {exc}")
    raise
finally:
    result["finished_utc"] = stamp()
    save(f"plumb_{db}.json", result)
