import asyncio
from dataclasses import dataclass, field
from typing import List
from consts import INITIAL_WORKERS
from .models import PageRecord

class Topic:
    def __init__(self, num_partitions: int, init_num_clients: int):
        self.num_partitions = num_partitions
        self.client_queues = {i: asyncio.Queue() for i in range(init_num_clients)}
        self.partition_to_client = {}
        self._assign_partitions()

    @property
    def num_clients(self) -> int:
        return len(self.client_queues)

    def _assign_partitions(self):
        sorted_clients = sorted(self.client_queues.keys())
        client_count = len(sorted_clients)
        if client_count == 0:
            return
        self.partition_to_client = {}
        for i in range(self.num_partitions):
            client_id = sorted_clients[i % client_count]
            self.partition_to_client[i] = client_id

    def add_client(self, client_id: int):
        if client_id not in self.client_queues:
            self.client_queues[client_id] = asyncio.Queue()
            self._assign_partitions()

    async def put(self, partition_index: int, record: PageRecord):
        client_id = self.partition_to_client[partition_index]
        await self.client_queues[client_id].put(record)

    async def fetch(self, worker_id: int, num: int) -> List[PageRecord]:
        queue = self.client_queues[worker_id]
        
        # Block until at least one record is available
        records = [await queue.get()]
        
        # Get up to `num - 1` more if available immediately
        while len(records) < num and not queue.empty():
            records.append(queue.get_nowait())
            
        return records

    def task_done(self, worker_id: int):
        self.client_queues[worker_id].task_done()

@dataclass
class KafkaSimulator:
    pages_topic: Topic = field(default_factory=lambda: Topic(num_partitions=100, init_num_clients=INITIAL_WORKERS))
    # Ingestion topic for raw batch URL discovery
    url_processor_topic: asyncio.Queue = field(default_factory=asyncio.Queue)

    async def produce_fetch_request(self, domain: str, record: PageRecord):
        """Routes a PageRecord to the correct Kafka partition based on its domain."""
        partition_index = hash(domain) % self.pages_topic.num_partitions
        await self.pages_topic.put(partition_index, record)

    def pending_records(self) -> int:
        pending_pages = sum(queue.qsize() for queue in self.pages_topic.client_queues.values())
        pending_urls = self.url_processor_topic.qsize()
        return pending_pages + pending_urls
