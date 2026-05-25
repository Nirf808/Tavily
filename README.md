# Web Crawl Scheduler Architecture Simulator

Local Python simulator for a distributed crawl scheduler architecture.
It focuses on realistic control flow (dispatching, scheduling, fetching, deduping, and scaling), not production-grade performance.

## How To Run

1. Clone the repository.
2. Install dependencies.
3. Run the simulator.

```bash
git clone <repo-url>
cd TavilyPython
pip install -r requirement.txt
python main.py
```

Note: if your local repo uses `requirements.txt`, use that filename instead.

## Packages

### core/
Contains the crawl pipeline logic and control flow used by the simulator.

### simulators/
Contains simulators for external technologies (Redis, Kafka, Cassandra, S3, Elasticsearch, HTTP).
APIs are intentionally close to what real integrations would use, while implementations are lightweight simulations and not necessarily optimized.

### orchestration/
Contains orchestration logic such as telemetry and auto-scaling.

### generator.py
Generates simulation data such as domains, URLs, page records/content flow, and ongoing domain growth.

## Core Components

* **Dispatcher (`core/dispatcher.py`)**: Moves records from Cassandra time buckets into per-domain Redis queues and updates each domain's next eligible scheduling time.
* **Scheduler (`core/scheduler.py`)**: Pulls ready domains from Redis and routes eligible pages to Kafka while respecting per-domain fetch constraints.
* **Crawler (`core/crawler.py`)**: Simulates worker fetchers that consume from Kafka partitions, apply rate-limit checks, fetch page content, and emit discovered URLs.
* **URL Processor (`core/url_process.py`)**: Deduplicates discovered URLs, stores new page records, initializes domain state, and re-queues data for future dispatching.

## Technology Simulation Highlights

* **Accurate Redis Modeling**: The `AsyncZSet` class in `simulators/redis_sim.py` uses `sortedcontainers` to preserve realistic sorted-set behavior and complexity for scheduling operations.
* **Partitioned Kafka Topic**: The simulator models partitioned routing from domain to worker queues, preserving deterministic fan-out and isolation behavior used by crawler workers.
* **Elasticsearch Duplicate Simulation**: Content-hash duplicate tracking is simulated with a simplified in-memory structure to preserve architecture behavior without production indexing overhead.