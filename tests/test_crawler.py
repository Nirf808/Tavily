import time
import asyncio
import pytest

from simulators.datastore import DataStore
from simulators.models import PageRecord
from simulators.http_sim import HttpResponse
from orchestration import telemetry

from core.crawler import Crawler


@pytest.mark.asyncio
async def test_fetch_failure_increments_pages_failed():
    ds = DataStore()
    # reset telemetry
    telemetry.tracker.pages_failed = 0
    telemetry.tracker.pages_fetched = 0

    worker = Crawler(worker_id=0, pages_topic=ds.kafka.pages_topic, datastore=ds)

    # override http_simulator.fetch to return failure instantly
    async def fake_fetch(url: str):
        return HttpResponse(success=False)

    worker.http_simulator.fetch = fake_fetch

    rec = PageRecord(url='http://example.com/', domain='example.com', next_time_to_fetch=time.time())

    await worker._fetch_and_process(rec, time.time())

    assert telemetry.tracker.pages_failed == 1
    assert telemetry.tracker.pages_fetched == 0
    # no s3 writes
    assert len(ds.s3.raw_content) == 0


@pytest.mark.asyncio
async def test_successful_fetch_updates_counters_and_discovers_urls():
    ds = DataStore()
    telemetry.tracker.pages_failed = 0
    telemetry.tracker.pages_fetched = 0
    telemetry.tracker.new_pages_fetched = 0

    worker = Crawler(worker_id=0, pages_topic=ds.kafka.pages_topic, datastore=ds)

    async def fake_fetch(url: str):
        return HttpResponse(success=True, content_hash='ch1', discovered_urls=['http://a/x'])

    worker.http_simulator.fetch = fake_fetch

    rec = PageRecord(url='http://example.com/p', domain='example.com', next_time_to_fetch=time.time(), is_first_fetch=True)

    await worker._fetch_and_process(rec, time.time())

    assert telemetry.tracker.pages_fetched == 1
    assert telemetry.tracker.new_pages_fetched == 1
    assert len(ds.s3.raw_content) == 1
    # discovered urls should be pushed into kafka url_processor_topic
    assert ds.kafka.url_processor_topic.qsize() == 1
