import time
from datetime import datetime
from urllib.parse import urlparse
from simulators import DataStore, PageRecord
from consts import (
    REDIS_KEY_NEXT_ALLOWED_TIME,
    REDIS_KEY_ALLOWED_RPS,
    DEFAULT_URL_PRIORITY_SCORE,
    DEFAULT_URL_UPDATE_FREQUENCY,
    DEFAULT_URL_FIRST_FETCH,
    DEFAULT_URL_NEXT_FETCH_DELAY,
    DEFAULT_DOMAIN_ALLOWED_RPS,
)

KEY_NEXT_ALLOWED_TIME = REDIS_KEY_NEXT_ALLOWED_TIME
KEY_ALLOWED_RPS = REDIS_KEY_ALLOWED_RPS
DEFAULT_PRIORITY_SCORE = DEFAULT_URL_PRIORITY_SCORE
DEFAULT_UPDATE_FREQUENCY = DEFAULT_URL_UPDATE_FREQUENCY
DEFAULT_FIRST_FETCH = DEFAULT_URL_FIRST_FETCH
DEFAULT_NEXT_FETCH_DELAY = DEFAULT_URL_NEXT_FETCH_DELAY
DEFAULT_ALLOWED_RPS = DEFAULT_DOMAIN_ALLOWED_RPS


async def url_process_loop(datastore: DataStore) -> None:
    """Resolves and isolates genuinely new URLs, protecting DB from heavy duplicate writes."""
    while True:
        url_batch = await datastore.kafka.url_processor_topic.get()
        _process_batch(url_batch, datastore)
        datastore.kafka.url_processor_topic.task_done()


def _process_batch(url_batch: list, datastore: DataStore) -> None:
    for url in url_batch:
        _record_encounter(url, datastore)
        if _is_new_url(url, datastore):
            _handle_new_url(url, datastore)


def _record_encounter(url: str, datastore: DataStore) -> None:
    datastore.s3.encounter_logs.append({"url": url, "ts": time.time()})


def _is_new_url(url: str, datastore: DataStore) -> bool:
    if url in datastore.redis.seen_urls_bloom:
        return False
    datastore.redis.seen_urls_bloom.add(url)
    return True


def _handle_new_url(url: str, datastore: DataStore) -> None:
    record = _build_record(url)
    _store_record(record, datastore)
    _enqueue_record(record, datastore)
    _ensure_domain_state(record.domain, datastore)


def _build_record(url: str) -> PageRecord:
    domain = urlparse(url).netloc
    return PageRecord(
        url=url,
        domain=domain,
        priority_score=DEFAULT_PRIORITY_SCORE,
        update_frequency=DEFAULT_UPDATE_FREQUENCY,
        is_first_fetch=DEFAULT_FIRST_FETCH,
        next_time_to_fetch=time.time() + DEFAULT_NEXT_FETCH_DELAY,
    )


def _store_record(record: PageRecord, datastore: DataStore) -> None:
    datastore.cassandra.source_of_truth[record.url] = record


def _enqueue_record(record: PageRecord, datastore: DataStore) -> None:
    # Use the record's next_time_to_fetch to correctly bucket it in the future
    target_hour_str = datetime.fromtimestamp(record.next_time_to_fetch).strftime('%Y-%m-%d %H')
    datastore.cassandra.time_buckets[target_hour_str].append(record)


def _ensure_domain_state(domain: str, datastore: DataStore) -> None:
    if domain in datastore.redis.domain_states:
        return
    datastore.redis.domain_states[domain] = {
        KEY_NEXT_ALLOWED_TIME: 0.0,
        KEY_ALLOWED_RPS: DEFAULT_ALLOWED_RPS,
    }
