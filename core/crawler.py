import asyncio
import time
from datetime import datetime
from typing import Dict, List
from simulators import DataStore, TargetServerSimulator, PageRecord
from orchestration.telemetry import tracker
from consts import REDIS_KEY_NEXT_ALLOWED_TIME, REDIS_KEY_ALLOWED_RPS, DEFAULT_CRAWLER_ALLOWED_RPS, PAGES_FETCH_BATCH_SIZE

LOG_MISSING_RATE_LIMIT = "[Crawler] Missing rate limit for {domain}, fetching robots.txt"
LOG_RATE_LIMIT_BLOCK = "[Crawler] Rate limit reached for {domain}, skipping fetch"


class Crawler:
    def __init__(self, worker_id: int, pages_topic, datastore: DataStore):
        self.worker_id = worker_id
        self.pages_topic = pages_topic
        self.datastore = datastore
        self.http_simulator = TargetServerSimulator()
        self.domain_active_requests: Dict[str, int] = {}

    async def run(self) -> None:
        while True:
            records = await self.pages_topic.fetch(self.worker_id, PAGES_FETCH_BATCH_SIZE)
            await self._process_records(records)
            self._ack_records(len(records))

    async def _process_records(self, records: List[PageRecord]) -> None:
        tasks = [self.process_record(record) for record in records]
        await asyncio.gather(*tasks)

    def _ack_records(self, count: int) -> None:
        for _ in range(count):
            self.pages_topic.task_done(self.worker_id)

    async def process_record(self, record: PageRecord) -> None:
        current_time = time.time()
        await self._ensure_rate_limit(record.domain, current_time)
        if not self._is_request_allowed(record.domain):
            print(LOG_RATE_LIMIT_BLOCK.format(domain=record.domain))
            return
        self._record_request_start(record.domain)
        try:
            await self._fetch_and_process(record, current_time)
        finally:
            self._record_request_end(record.domain)

    async def _ensure_rate_limit(self, domain: str, current_time: float) -> None:
        domain_state = self._get_domain_state(domain)
        if self._has_rate_limit(domain_state):
            return
        await self._fetch_robots_txt(domain, current_time)

    async def _fetch_robots_txt(self, domain: str, current_time: float) -> None:
        robot_url = f"http://{domain}/robots.txt"
        print(LOG_MISSING_RATE_LIMIT.format(domain=domain))
        response = await self.http_simulator.fetch(robot_url)
        allowed_rps = response.robots_rps or DEFAULT_CRAWLER_ALLOWED_RPS
        domain_state = self._get_domain_state(domain)
        domain_state.update(
            {
                REDIS_KEY_NEXT_ALLOWED_TIME: current_time,
                REDIS_KEY_ALLOWED_RPS: allowed_rps,
            }
        )
        self._set_domain_state(domain, domain_state)

    def _is_request_allowed(self, domain: str) -> bool:
        allowed_rps = self._get_allowed_rps(domain)
        active_requests = self.domain_active_requests.get(domain, 0)
        return active_requests < allowed_rps

    def _record_request_start(self, domain: str) -> None:
        self.domain_active_requests[domain] = self.domain_active_requests.get(domain, 0) + 1

    def _record_request_end(self, domain: str) -> None:
        if domain in self.domain_active_requests:
            self.domain_active_requests[domain] = max(0, self.domain_active_requests[domain] - 1)

    def _get_allowed_rps(self, domain: str) -> int:
        domain_state = self._get_domain_state(domain)
        return int(domain_state.get(REDIS_KEY_ALLOWED_RPS, DEFAULT_CRAWLER_ALLOWED_RPS))

    def _get_domain_state(self, domain: str) -> dict:
        return self.datastore.redis.domain_states.get(domain, {})

    def _set_domain_state(self, domain: str, domain_state: dict) -> None:
        self.datastore.redis.domain_states[domain] = domain_state

    def _has_rate_limit(self, domain_state: dict) -> bool:
        return REDIS_KEY_ALLOWED_RPS in domain_state

    async def _fetch_and_process(self, record: PageRecord, current_time: float) -> None:
        response = await self.http_simulator.fetch(record.url)
        if not response.success:
            tracker.pages_failed += 1
            return
        tracker.pages_fetched += 1
        self._handle_content_freshness(record, response.content_hash, current_time)
        await self._handle_discovered_urls(response.discovered_urls)
        self._write_to_s3(record.url, response.content_hash, current_time)
        self._enqueue_refetch(record)

    def _enqueue_refetch(self, record: PageRecord) -> None:
        # Enqueue the completed record back to Cassandra time buckets for the future run
        target_hour_str = datetime.fromtimestamp(record.next_time_to_fetch).strftime('%Y-%m-%d %H')
        self.datastore.cassandra.time_buckets[target_hour_str].append(record)
        self.datastore.cassandra.source_of_truth[record.url] = record

    def _handle_content_freshness(self, record: PageRecord, content_hash: str, current_time: float) -> None:
        if record.is_first_fetch:
            tracker.new_pages_fetched += 1
            self._handle_first_fetch(record, content_hash)
        else:
            tracker.refetched_pages += 1
            self._handle_subsequent_fetch(record, content_hash, current_time)
        record.content_hash = content_hash
        record.last_fetched_time = current_time
        
        weight = 0.5
        record.next_time_to_fetch = current_time + record.update_frequency - (weight * record.priority_score)

    def _handle_first_fetch(self, record: PageRecord, content_hash: str) -> None:
        if self.datastore.elasticsearch.is_duplicate(content_hash):
            record.priority_score *= 0.1
            record.update_frequency *= 2.0
        else:
            self.datastore.elasticsearch.add_hash(content_hash)
        record.is_first_fetch = False

    def _handle_subsequent_fetch(self, record: PageRecord, content_hash: str, current_time: float) -> None:
        if record.content_hash == content_hash:
            record.update_frequency *= 1.5
            return
        if record.next_time_to_fetch and current_time < record.next_time_to_fetch:
            record.update_frequency = max(300.0, record.update_frequency * 0.8)

    async def _handle_discovered_urls(self, discovered_urls: list) -> None:
        if discovered_urls:
            await self.datastore.kafka.url_processor_topic.put(discovered_urls)

    def _write_to_s3(self, url: str, content_hash: str, current_time: float) -> None:
        payload = {"url": url, "payload": "<html>dummy</html>", "ts": current_time, "hash": content_hash}
        self.datastore.s3.raw_content.append(payload)
