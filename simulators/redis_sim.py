import asyncio
import collections
from dataclasses import dataclass, field
from typing import Dict, Set
from sortedcontainers import SortedSet

class AsyncZSet:
    def __init__(self):
        # The "Skip List": stores tuples of (score, key) for fast priority slicing
        self._sorted_set = SortedSet()
        # The "Hash Table": maps key -> score for fast O(1) lookups
        self._key_map = {}
        # Stores the JSON payload/metadata attached to the member
        self._metadata_map = {}
        # Protects the data structure from race conditions between async workers
        self._lock = asyncio.Lock()

    async def zadd(self, key: str, score: float, metadata: dict = None):
        async with self._lock:
            # If key already exists, remove the old score first to update it
            if key in self._key_map:
                old_score = self._key_map[key]
                self._sorted_set.remove((old_score, key))
            
            self._key_map[key] = score
            if metadata is not None:
                self._metadata_map[key] = metadata
            self._sorted_set.add((score, key))

    async def zrange(self, start: int, end: int) -> list:
        """Fetch the top N items without removing them."""
        async with self._lock:
            # Slicing a SortedSet is heavily optimized
            return [item[1] for item in self._sorted_set[start:end]]

    async def zrange_with_scores(self, start: int, end: int, max_score: float = None, with_metadata: bool = False) -> list:
        """Fetch the top N items with their scores, optionally up to a max_score."""
        async with self._lock:
            results = []
            for item in self._sorted_set[start:end]:
                if max_score is not None and item[0] > max_score:
                    break
                if with_metadata:
                    results.append((item[0], item[1], self._metadata_map.get(item[1], {})))
                else:
                    results.append((item[0], item[1]))
            return results

    async def zrem(self, *keys: str):
        """Delete specific keys instantly, regardless of their current score/rank."""
        async with self._lock:
            for key in keys:
                if key in self._key_map:
                    score = self._key_map.pop(key)
                    self._metadata_map.pop(key, None)
                    self._sorted_set.remove((score, key))

    def __len__(self):
        return len(self._sorted_set)

@dataclass
class RedisSimulator:
    # Key: domain, Value: dict (next_allowed_time_to_fetch, allowed_requests_per_second, crawl_delay)
    domain_states: Dict[str, dict] = field(default_factory=dict)
    
    # Priority Queue containing tuples: (score/next_time_to_fetch, domain)
    domains_queue: AsyncZSet = field(default_factory=AsyncZSet)
    
    # Key: domain, Value: PriorityQueue containing tuples: (score/next_time_to_fetch, url)
    pages_queues: Dict[str, AsyncZSet] = field(default_factory=lambda: collections.defaultdict(AsyncZSet))
    
    # Probabilistic duplicate tracking (simulated broadly with a standard set)
    seen_urls_bloom: Set[str] = field(default_factory=set)

    async def update_domain_queue(self, domain: str, min_next_time_to_fetch: float):
        """Simulates Lua script updating the aggregated domain queue"""
        domain_state = self.domain_states.get(domain, {})
        nxt_allowed = domain_state.get('next_allowed_time_to_fetch', 0.0)
        
        priority_score = max(min_next_time_to_fetch, nxt_allowed)
        await self.domains_queue.zadd(domain, priority_score)
