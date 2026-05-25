import asyncio
import time
from datetime import datetime

import pytest

from simulators.datastore import DataStore
from simulators.models import PageRecord

from core.dispatcher import Dispatcher, dispatcher_loop


@pytest.mark.asyncio
async def test_dispatcher_promotes_time_bucket_into_redis_pages_and_domains():
    ds = DataStore()
    now = time.time()
    hour_str = datetime.fromtimestamp(now).strftime('%Y-%m-%d %H')

    # Create a PageRecord in Cassandra time bucket
    url = 'http://example.com/page'
    rec = PageRecord(url=url, next_time_to_fetch=now, domain='example.com')
    # Put a source_of_truth record with a priority to verify sync
    ds.cassandra.source_of_truth[url] = PageRecord(url=url, priority_score=5.0)
    ds.cassandra.time_buckets[hour_str].append(rec)

    # run dispatcher loop briefly
    dispatcher = Dispatcher(ds)
    task = asyncio.create_task(dispatcher_loop(ds, dispatcher))
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # After dispatcher run, pages queue should contain the record
    pages = await ds.redis.pages_queues['example.com'].zrange_with_scores(0, 10, with_metadata=True)
    assert any(item[1] == 'http://example.com/page' for item in pages)

    # domains_queue should have an entry for the domain
    domains = await ds.redis.domains_queue.zrange_with_scores(0, 10)
    domain_names = [d for _, d in domains]
    assert 'example.com' in domain_names
