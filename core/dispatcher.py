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


class Dispatcher:
    def __init__(self, datastore: DataStore, mode: DispatcherMode = DispatcherMode.FAST) -> None:
        self.datastore = datastore
        self.mode = mode

    def get_mode(self) -> DispatcherMode:
        return self.mode

    def set_mode(self, mode: DispatcherMode) -> None:
        self.mode = mode

    def get_delay(self) -> float:
        return DISPATCHER_MODE_DELAYS.get(self.mode, DISPATCHER_MODE_DELAYS[DispatcherMode.MEDIUM])

    async def run(self) -> None:
        while True:
            await self.dispatch_once()
            await asyncio.sleep(self.get_delay())

    async def dispatch_once(self) -> None:
        current_time = time.time()
        current_hour_str = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H')

        page_records_to_dispatch = self.datastore.cassandra.time_buckets.pop(current_hour_str, [])

        domain_min_fetch = {}

        for record in page_records_to_dispatch:
            url = record.url
            nxt_fetch = record.next_time_to_fetch or current_time
            domain = urlparse(url).netloc
            record.domain = domain
            _sync_priority_from_source_of_truth(record, self.datastore)

            await self.datastore.redis.pages_queues[domain].zadd(url, nxt_fetch, metadata=record)

            if domain not in domain_min_fetch or nxt_fetch < domain_min_fetch[domain]:
                domain_min_fetch[domain] = nxt_fetch

        for domain, min_nxt_fetch in domain_min_fetch.items():
            await self.datastore.redis.update_domain_queue(domain, min_nxt_fetch)


async def dispatcher_loop(datastore: DataStore, dispatcher: Dispatcher | None = None):
    """Simulates transitioning URL schedules from Cassandra into Redis."""
    active_dispatcher = dispatcher or Dispatcher(datastore)
    await active_dispatcher.run()


def _sync_priority_from_source_of_truth(record: PageRecord, datastore: DataStore) -> None:
    source_record = datastore.cassandra.source_of_truth.get(record.url)
    if source_record is None:
        return
    record.priority_score = source_record.priority_score
