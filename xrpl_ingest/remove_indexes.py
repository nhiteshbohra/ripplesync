"""Remove only the six retired index names: python -m xrpl_ingest.remove_indexes."""
import logging
import os

from .config import Config, redact_url
from .errors import ConfigError
from .store import MongoStore
from .support import files
from pymongo.errors import PyMongoError

log = logging.getLogger(__name__)
REMOVED_INDEXES = frozenset({
    "from_address_1", "kind_1_start_1_close_time_1", "TransactionType_1",
    "close_time_1", "to_address_1", "affected_accounts_1",
})


def remove_indexes(db):
    """Inspect every collection; drop exact names only, leaving documents untouched."""
    removed = []
    for collection in db.list_collection_names():
        coll = db[collection]
        for name in sorted(REMOVED_INDEXES.intersection(coll.index_information())):
            coll.drop_index(name)
            removed.append((collection, name))
            log.info("Removed index %s.%s.%s", db.name, collection, name)
        remaining = REMOVED_INDEXES.intersection(coll.index_information())
        if remaining:
            raise RuntimeError(f"Retired indexes remain in {collection}: {sorted(remaining)}")
    log.info("Verified database %s: %d indexes removed; no retired indexes remain", db.name, len(removed))
    return removed


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        env = files.load_env()
        cfg = Config.from_env(base_dir=files.base_dir(os.environ, env))
        store = MongoStore.from_config(cfg)
        try:
            remove_indexes(store.ledgers.database)
        finally:
            store.close()
    except (ConfigError, PyMongoError, RuntimeError) as exc:
        log.error("%s", redact_url(str(exc)))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
