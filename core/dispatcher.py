import asyncio
import time
from urllib.parse import urlparse
from datetime import datetime
from simulators import DataStore, PageRecord
from core.dispatcher_mode import DispatcherMode

DISPATCHER_MODE_DELAYS = {
    DispatcherMode.FAST: 1.0,
    DispatcherMode.MEDIUM: 3.0,
    DispatcherMode.SLOW: 10.0,
}


def get_dispatcher_delay(datastore: DataStore) -> float:
    return DISPATCHER_MODE_DELAYS.get(datastore.dispatcher_mode, DISPATCHER_MODE_DELAYS[DispatcherMode.MEDIUM])


async def dispatcher_loop(datastore: DataStore):
    """Simulates transitioning URL schedules from Cassandra into Redis."""
    while True:
        current_time = time.time()
        current_hour_str = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H')

        page_records_to_dispatch = datastore.cassandra.time_buckets.pop(current_hour_str, [])

        domain_min_fetch = {}

        for record in page_records_to_dispatch:
            url = record.url
            nxt_fetch = record.next_time_to_fetch or current_time
            domain = urlparse(url).netloc
            record.domain = domain
            _sync_priority_from_source_of_truth(record, datastore)

            await datastore.redis.pages_queues[domain].zadd(url, nxt_fetch, metadata=record)

            if domain not in domain_min_fetch or nxt_fetch < domain_min_fetch[domain]:
                domain_min_fetch[domain] = nxt_fetch

        for domain, min_nxt_fetch in domain_min_fetch.items():
            await datastore.redis.update_domain_queue(domain, min_nxt_fetch)

        await asyncio.sleep(get_dispatcher_delay(datastore))


def _sync_priority_from_source_of_truth(record: PageRecord, datastore: DataStore) -> None:
    source_record = datastore.cassandra.source_of_truth.get(record.url)
    if source_record is None:
        return
    record.priority_score = source_record.priority_score
