"""Local synthetic-node benchmark; writes only to disposable MongoDB databases.

Run: python setup/benchmark_ingest.py --ledgers 300 --transactions 20
Uses the project's existing fake node and verifies stored data after each timed run.
This measures the pipeline on this machine, not full-history production throughput.
"""
import argparse
import asyncio
import json
import logging
import multiprocessing
import statistics
import sys
import threading
import time
import uuid
from dataclasses import asdict
from pathlib import Path

from pymongo.monitoring import CommandListener

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

from fake_xrpld import FakeXrpld  # noqa: E402
from xrpl_ingest.backfill import ChunkResult, process_chunk  # noqa: E402
from xrpl_ingest.config import Config, FIRST_LEDGER  # noqa: E402
from xrpl_ingest.fetcher import RetryPolicy  # noqa: E402
from xrpl_ingest.source import WsSource  # noqa: E402
from xrpl_ingest.store import MongoStore  # noqa: E402
from xrpl_ingest.transform import parse_ledger_result  # noqa: E402


class WriteCounter(CommandListener):
    def __init__(self):
        self.count = 0
        self._lock = threading.Lock()

    def started(self, event):
        if event.command_name == "update":
            with self._lock:
                self.count += 1

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


async def reference_chunk(source, store, lo, hi, concurrency):
    """Previous steady-state fetch -> inline transform -> per-ledger save flow.

    Synthetic requests always succeed; production retry handling is tested separately.
    """
    indices = iter(range(lo, hi + 1))

    async def worker():
        for index in indices:
            raw = await source.get_ledger(index)
            bundle = parse_ledger_result(index, raw)
            await asyncio.to_thread(store.save_ledger_bundle, *bundle)

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    await asyncio.to_thread(store.mark_chunk_done, lo, hi, hi - lo + 1, 0)
    return ChunkResult((lo, hi), ok=hi - lo + 1)


def serve_node(lo, hi, transactions, sender, stop):
    # A real source is another process/machine. Do not let the synthetic server's JSON
    # encoding compete with ingestion for the same Python GIL and distort the comparison.
    with FakeXrpld(lo, hi, big=dict.fromkeys(range(lo, hi + 1), transactions)) as node:
        sender.send(node.url)
        sender.close()
        stop.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledgers", type=int, default=300)
    parser.add_argument("--transactions", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--compare", action="store_true", help="alternate the previous steady-state flow and pipeline")
    args = parser.parse_args()
    if min(args.ledgers, args.concurrency, args.repeats) < 1 or args.transactions < 0:
        parser.error("ledgers, concurrency and repeats must be positive; transactions cannot be negative")
    logging.basicConfig(level=logging.WARNING)
    lo, hi = FIRST_LEDGER, FIRST_LEDGER + args.ledgers - 1
    rates = {"reference": [], "pipeline": []}
    print("Synthetic node in a separate process + real local MongoDB; not a production throughput forecast.")
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    stop = ctx.Event()
    node = ctx.Process(target=serve_node, args=(lo, hi, args.transactions, sender, stop))
    node.start()
    sender.close()
    try:
        if not receiver.poll(15):
            raise RuntimeError("synthetic node did not start")
        url = receiver.recv()
        for repeat in range(args.repeats * (2 if args.compare else 1)):
            mode = "reference" if args.compare and repeat % 2 == 0 else "pipeline"
            cfg = Config(mongo_uri="mongodb://127.0.0.1:27017", db_name=f"xrpl_ingest_bench_{uuid.uuid4().hex}")
            writes = WriteCounter()
            store = MongoStore.from_config(cfg, serverSelectionTimeoutMS=2000, event_listeners=[writes])
            try:
                store.ensure_indexes()

                async def run():
                    source = WsSource([url], expected_network_id=None)
                    try:
                        if mode == "reference":
                            return await reference_chunk(source, store, lo, hi, args.concurrency)
                        return await process_chunk(source, store, (lo, hi), concurrency=args.concurrency,
                                                   retry=RetryPolicy(), stop_check=lambda: False)
                    finally:
                        await source.close()

                started = time.perf_counter()
                result = asyncio.run(run())
                elapsed = time.perf_counter() - started
                assert result.ok == args.ledgers and result.failed == 0 and result.completed
                assert store.ledgers.count_documents({}) == args.ledgers
                assert store.txs.count_documents({}) == args.ledgers * args.transactions
                assert store.metas.count_documents({}) == args.ledgers * args.transactions
                assert not store.find_gaps(lo, hi) and not store.tx_count_mismatches(lo, hi)
                assert store.done_chunks(lo, hi) == {(lo, hi)} and store.count_failed() == 0
                rate = args.ledgers / elapsed
                rates[mode].append(rate)
                print(json.dumps({"run": len(rates[mode]), "mode": mode, "seconds": round(elapsed, 3),
                                  "ledgers_per_second": round(rate, 1),
                                  "transactions_per_second": round(rate * args.transactions, 1),
                                  "mongo_write_commands": writes.count,
                                  "result": asdict(result)}))
            finally:
                assert cfg.db_name.startswith("xrpl_ingest_bench_")
                store.client.drop_database(cfg.db_name)
                store.close()
    finally:
        stop.set()
        node.join(10)
        if node.is_alive():
            node.terminate()
            node.join(5)
        receiver.close()
    for mode, values in rates.items():
        if values:
            print(f"{mode} median: {statistics.median(values):.1f} ledgers/s; all runs verified.")
    if args.compare:
        print(f"Pipeline/reference: {statistics.median(rates['pipeline']) / statistics.median(rates['reference']):.2f}x")


if __name__ == "__main__":
    main()
