import collections
from dataclasses import dataclass, field
from typing import Dict, List
from .models import PageRecord


@dataclass
class CassandraSimulator:
    # Key: URL, Value: PageRecord holding the source-of-truth metadata
    source_of_truth: Dict[str, PageRecord] = field(default_factory=dict)
    # Key: hour-string (e.g., '2023-10-25 14'), Value: list of PageRecords
    time_buckets: Dict[str, List[PageRecord]] = field(default_factory=lambda: collections.defaultdict(list))
