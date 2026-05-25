from dataclasses import dataclass, field
from typing import List

@dataclass
class S3Simulator:
    raw_content: List[dict] = field(default_factory=list)
    encounter_logs: List[dict] = field(default_factory=list)
