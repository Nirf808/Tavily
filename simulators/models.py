from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PageRecord:
    url: str
    content_hash: Optional[str] = None
    priority_score: float = 0.0
    update_frequency: float = 0.0
    is_first_fetch: bool = True
    next_time_to_fetch: float = 0.0
    domain: Optional[str] = None
    last_fetched_time: Optional[float] = None
    evaluated_update_time: Optional[float] = None
    state: Optional[str] = None
