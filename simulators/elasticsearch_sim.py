from dataclasses import dataclass, field
from typing import Set

@dataclass
class ElasticsearchSimulator:
    # A set of known SimHashes
    known_hashes: Set[str] = field(default_factory=set)

    def is_duplicate(self, content_hash: str) -> bool:
        """
        Queries the simulated Elasticsearch to check if this content hash 
        (or a very similar one) already exists.
        """
        # In a real system, this would be an approximate nearest neighbor 
        # or Hamming distance query for SimHash.
        return content_hash in self.known_hashes

    def add_hash(self, content_hash: str):
        """Adds a new content hash to the index."""
        self.known_hashes.add(content_hash)
