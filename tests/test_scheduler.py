import time
import asyncio
import pytest

from simulators.datastore import DataStore
from simulators.models import PageRecord

import core.scheduler as scheduler


@pytest.mark.asyncio
async def test_fetch_ready_domains_filters_by_time():
    ds = DataStore()
    now = time.time()

    # add two domains: one ready, one in the future
    await ds.redis.domains_queue.zadd('ready.example', now - 10)
    await ds.redis.domains_queue.zadd('future.example', now + 1000)

    ready = await scheduler.fetch_ready_domains(ds, now, batch_size=10)
    # Should only return the ready domain
    domains = [d for _, d in ready]
    assert 'ready.example' in domains
    assert 'future.example' not in domains


@pytest.mark.asyncio
async def test_route_urls_to_workers_respects_rps_and_removes_pages():
    ds = DataStore()
    now = time.time()
    domain = 'example.com'

    ds.redis.domain_states[domain] = {'allowed_requests_per_second': 2}

    # create three ready pages (only 2 should be routed)
    recs = []
    for i in range(3):
        r = PageRecord(url=f'http://{domain}/p{i}', domain=domain, next_time_to_fetch=now)
        recs.append(r)
        await ds.redis.pages_queues[domain].zadd(r.url, r.next_time_to_fetch, metadata=r)

    routed = await scheduler.route_urls_to_workers(domain, ds.redis.domain_states[domain], ds, now)

    assert routed == 2

    # Ensure routed pages were produced into kafka (pending_records > 0)
    assert ds.kafka.pending_records() >= 2

    # Ensure remaining pages in redis is 1
    remaining = await ds.redis.pages_queues[domain].zrange_with_scores(0, 10)
    assert len(remaining) == 1
