import asyncio
import random
import time
from urllib.parse import urlparse
from datetime import datetime
from typing import List
from simulators.models import PageRecord

from simulators.datastore import DataStore
from consts import (
    INITIAL_DOMAIN_COUNT,
    INITIAL_URLS_PER_DOMAIN,
    NEW_DOMAINS_PER_SECOND,
    NEW_URLS_PER_DOMAIN,
    PROB_NEW_URL,
    DEFAULT_DOMAIN_ALLOWED_RPS,
    REDIS_KEY_NEXT_ALLOWED_TIME,
    REDIS_KEY_ALLOWED_RPS,
)


class DataGenerator:
    def __init__(self):
        self.master_urls: List[str] = []
        self.next_domain_index: int = INITIAL_DOMAIN_COUNT
        self.next_url_index: int = INITIAL_DOMAIN_COUNT * INITIAL_URLS_PER_DOMAIN

    def generate_seeds_and_init(self, datastore: DataStore) -> DataStore:
        self.master_urls.clear()
        self.next_domain_index = INITIAL_DOMAIN_COUNT
        self.next_url_index = INITIAL_DOMAIN_COUNT * INITIAL_URLS_PER_DOMAIN
        current_time = time.time()
        current_hour_str = self._current_hour_str(current_time)
        domains = self._build_initial_domains()
        self._seed_domains(domains, datastore, current_time, current_hour_str)
        return datastore

    async def domain_growth_loop(self, datastore: DataStore) -> None:
        while True:
            self._add_new_domains(datastore, NEW_DOMAINS_PER_SECOND)
            await asyncio.sleep(1.0)

    def _current_hour_str(self, current_time: float) -> str:
        return datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H')

    def _build_initial_domains(self) -> List[str]:
        return [self._build_domain_name(i) for i in range(INITIAL_DOMAIN_COUNT)]

    def _build_domain_name(self, index: int) -> str:
        return f"domain{chr(97 + (index % 26))}{index}.com"

    def _seed_domains(self, domains: List[str], datastore: DataStore, current_time: float, current_hour_str: str) -> None:
        for domain in domains:
            self._ensure_domain_state(domain, datastore)
            self._seed_domain_urls(domain, INITIAL_URLS_PER_DOMAIN, datastore, current_time, current_hour_str)

    def _seed_domain_urls(
        self,
        domain: str,
        count: int,
        datastore: DataStore,
        current_time: float,
        current_hour_str: str,
    ) -> None:
        for _ in range(count):
            url = self._create_url(domain)
            self._append_url(url, datastore, current_time, current_hour_str)

    def _create_url(self, domain: str) -> str:
        url = f"http://{domain}/page_{self.next_url_index}"
        self.next_url_index += 1
        return url

    def _append_url(self, url: str, datastore: DataStore, current_time: float, current_hour_str: str) -> None:
        self.master_urls.append(url)
        record = self._build_record(url, current_time)
        datastore.cassandra.source_of_truth[url] = record
        datastore.cassandra.time_buckets[current_hour_str].append(record)

    def _build_record(self, url: str, current_time: float) -> PageRecord:
        domain = urlparse(url).netloc
        return PageRecord(
            url=url,
            priority_score=1.0,
            update_frequency=10.0,
            is_first_fetch=True,
            next_time_to_fetch=current_time,
            domain=domain,
        )

    def _ensure_domain_state(self, domain: str, datastore: DataStore) -> None:
        if domain in datastore.redis.domain_states:
            return
        datastore.redis.domain_states[domain] = {
            REDIS_KEY_NEXT_ALLOWED_TIME: 0.0,
            REDIS_KEY_ALLOWED_RPS: DEFAULT_DOMAIN_ALLOWED_RPS,
        }

    def _add_new_domains(self, datastore: DataStore, count: int) -> None:
        current_time = time.time()
        current_hour_str = self._current_hour_str(current_time)
        for _ in range(count):
            domain = self._next_domain_name()
            self._ensure_domain_state(domain, datastore)
            self._seed_domain_urls(domain, NEW_URLS_PER_DOMAIN, datastore, current_time, current_hour_str)

    def _next_domain_name(self) -> str:
        domain = self._build_domain_name(self.next_domain_index)
        self.next_domain_index += 1
        return domain

    def content_generator(self) -> List[str]:
        return [
            self._choose_url(10),
            self._choose_url(100),
            self._choose_url(0),
            self._choose_url(0),
            self._choose_url(0),
        ]

    def _get_prob_new_url(self) -> float:
        return PROB_NEW_URL * (0.9999 ** len(self.master_urls))

    def _choose_url(self, limit: int) -> str:
        if self.master_urls and random.random() < self._get_prob_new_url():
            return self._create_new_master_url()
        return self._random_existing_url(limit)

    def _create_new_master_url(self) -> str:
        domain = self._random_existing_domain()
        url = self._create_url(domain)
        self.master_urls.append(url)
        return url

    def _random_existing_domain(self) -> str:
        sample_url = random.choice(self.master_urls)
        return urlparse(sample_url).netloc

    def _random_existing_url(self, limit: int) -> str:
        if not self.master_urls:
            return "http://domain0.com/page_0"
        if limit <= 0:
            return random.choice(self.master_urls)
        capped = min(limit, len(self.master_urls))
        return random.choice(self.master_urls[:capped])
