import asyncio
import random
from typing import Optional
from dataclasses import dataclass, field
from generator import DataGenerator

@dataclass
class HttpResponse:
    success: bool
    content_hash: Optional[str] = None
    discovered_urls: list = field(default_factory=list)
    robots_rps: Optional[int] = None

class TargetServerSimulator:
    def __init__(self):
        self.generator = DataGenerator()

    async def fetch(self, url: str) -> HttpResponse:
        """Simulates an HTTP request to a target server."""
        await asyncio.sleep(random.uniform(1.0, 4.0))
        
        is_failure = random.random() < 0.20
        if is_failure:
            return HttpResponse(success=False)

        if url.endswith("/robots.txt") or url.endswith("robots.txt"):
            return HttpResponse(success=True, robots_rps=random.randint(1, 10))

        discovered_urls = self.generator.content_generator()
        # Simulate generating a simple content hash
        # In a real system this would be a SimHash
        content_hash = f"hash_{random.randint(1, 1000)}"
        
        return HttpResponse(
            success=True,
            content_hash=content_hash,
            discovered_urls=discovered_urls
        )
