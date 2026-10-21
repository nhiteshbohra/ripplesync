import asyncio
import threading

import pytest
from bson.errors import InvalidDocument
from pymongo.errors import AutoReconnect

from conftest import FAST_RETRY, NOW, FakeSource, make_result, no_sleep
from xrpl_ingest import backfill, fetcher
from xrpl_ingest.transform import parse_ledger_result


def test_batch_save_partial_failure_has_no_completion_markers_and_replays(store, monkeypatch):
    bundles = [parse_ledger_result(n, make_result(n), NOW) for n in (10, 11)]
    real = store.metas.bulk_write

    def partial(ops, **kwargs):
        real(ops[:1], **kwargs)
        raise AutoReconnect("lost acknowledgment after partial metadata write")

    monkeypatch.setattr(store.metas, "bulk_write", partial)
    with pytest.raises(AutoReconnect):
        store.save_ledger_bundles(bundles)
    assert store.txs.count_documents({}) == 4
    assert store.metas.count_documents({}) == 1
    assert store.ledgers.count_documents({}) == 0
    monkeypatch.setattr(store.metas, "bulk_write", real)
    store.save_ledger_bundles(bundles)
    store.save_ledger_bundles(bundles)
    assert store.ledgers.count_documents({}) == 2
    assert store.txs.count_documents({}) == store.metas.count_documents({}) == 4


def test_pipeline_overlaps_fetching_and_writing_but_bounds_prefetch(store, monkeypatch):
    monkeypatch.setattr(backfill, "WRITE_WORKERS", 1)
    started, release = threading.Event(), threading.Event()
    sizes = []
    real = store.save_ledger_bundles
    src = FakeSource()

    def slow_save(bundles):
        sizes.append(len(bundles))
        started.set()
        assert release.wait(5), "test failed to release the writer"
        real(bundles)

    monkeypatch.setattr(store, "save_ledger_bundles", slow_save)

    async def run():
        task = asyncio.create_task(backfill.process_chunk(
            src, store, (10, 29), concurrency=3, retry=FAST_RETRY,
            stop_check=lambda: False, sleep=no_sleep))
        try:
            assert await asyncio.to_thread(started.wait, 2)
            # One writer batch, three queued ledgers, and three producer-held ledgers.
            bound = sizes[0] + 6
            for _ in range(200):
                if len(src.calls) == bound:
                    break
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.02)
            assert len(src.calls) == bound  # fetching overlaps the blocked write, then stops
            assert not store.done_chunks(10, 29)
        finally:
            release.set()
        result = await asyncio.wait_for(task, 5)
        assert result.ok == 20 and result.failed == 0

    asyncio.run(run())
    assert max(sizes) > 1 and max(sizes) <= backfill.WRITE_BATCH_LEDGERS
    assert sum(sizes) == 20
    assert store.done_chunks(10, 29) == {(10, 29)}
    assert store.txs.count_documents({}) == store.metas.count_documents({}) == 40


def test_bad_ledger_in_batch_is_isolated_without_refetching_neighbours(store, monkeypatch):
    monkeypatch.setattr(backfill, "WRITE_WORKERS", 1)
    real = store.save_ledger_bundles
    rejected_batches = []

    def reject_one(bundles):
        if any(b[0]["block_number"] == 13 for b in bundles):
            rejected_batches.append(len(bundles))
            raise InvalidDocument("bad document")
        real(bundles)

    # Deterministic preparation lets the queue fill before the writer runs.
    async def prepare(source, index, retry, **kwargs):
        return parse_ledger_result(index, await source.get_ledger(index), NOW)

    monkeypatch.setattr(store, "save_ledger_bundles", reject_one)
    monkeypatch.setattr(backfill, "prepare_ledger", prepare)
    src = FakeSource()
    result = asyncio.run(backfill.process_chunk(
        src, store, (10, 19), concurrency=4, retry=FAST_RETRY,
        stop_check=lambda: False, sleep=no_sleep))
    assert (result.ok, result.failed, result.unrecorded) == (9, 1, 0)
    assert max(rejected_batches) > 1  # exercised batch failure followed by individual saves
    assert sorted(src.calls) == list(range(10, 20))
    assert store.list_failed() == [13]
    assert store.ledgers.count_documents({}) == 9
    assert store.done_chunks(10, 19) == {(10, 19)}


def test_pipeline_database_outage_leaves_chunk_unmarked_without_refetching(store, monkeypatch):
    def down(*args):
        raise AutoReconnect("database unavailable")

    monkeypatch.setattr(store, "save_ledger_bundles", down)
    monkeypatch.setattr(store, "record_failure", down)
    src = FakeSource()
    result = asyncio.run(backfill.process_chunk(
        src, store, (10, 14), concurrency=3, retry=FAST_RETRY,
        stop_check=lambda: False, sleep=no_sleep))
    assert (result.ok, result.failed, result.unrecorded) == (0, 5, 5)
    assert sorted(src.calls) == list(range(10, 15))
    assert not store.done_chunks(10, 14)


def test_transform_does_not_block_async_reader_tasks(store, monkeypatch):
    release = threading.Event()
    real = fetcher.parse_ledger_result

    def transform(*args):
        assert release.wait(2), "event loop could not run while transforming"
        return real(*args)

    monkeypatch.setattr(fetcher, "parse_ledger_result", transform)

    async def run():
        source = FakeSource({10: [make_result(10, 64)]})
        task = asyncio.create_task(fetcher.ingest_ledger(source, store, 10, FAST_RETRY))
        try:
            await asyncio.sleep(0.02)
        finally:
            release.set()
        assert (await task).ok

    asyncio.run(run())


def test_cancelled_pipeline_cleans_up_its_tasks_and_never_marks_done(store):
    async def run():
        started = asyncio.Event()

        class WaitingSource(FakeSource):
            async def get_ledger(self, index):
                started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(backfill.process_chunk(
            WaitingSource(), store, (10, 19), concurrency=3, retry=FAST_RETRY,
            stop_check=lambda: False))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert asyncio.all_tasks() == {asyncio.current_task()}

    asyncio.run(run())
    assert not store.done_chunks(10, 19)
