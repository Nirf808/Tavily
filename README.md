# Web Crawl Scheduler Simulator

This repository is a local Python simulation of a crawl scheduler pipeline. It focuses on the control flow of dispatching, scheduling, fetching, deduping, and batch reprioritization rather than production-scale infrastructure.

## Run

### Run the simulator

```bash
git clone https://github.com/Nirf808/Tavily.git
cd TavilyPython
pip install -r requirement.txt
python main.py
```

`main.py` runs the full simulation end to end and prints logs from telemetry and the core components so you can see the system behavior in real time.

### Run the unit tests

```bash
pytest -q
```
## Project Structure

### `core/`
Contains the main crawl pipeline used by the simulator.

### `simulators/`
Contains lightweight stand-ins for Redis, Kafka, Cassandra, S3, Elasticsearch, and HTTP.

### `orchestration/`
Contains telemetry and scaling helpers.

### `tests/`
Contains unit tests for the scheduler, crawler, dispatcher, URL processor, Kafka routing, telemetry counters, and reproducible generator behavior.

## `asyncio` as the Simulation Backbone

`asyncio` is a good fit because this assignment is mostly I/O-style coordination. Workers spend most of their time waiting on simulated latency, not doing heavy CPU work. That makes an event loop a simple way to model many in-flight fetches, per-domain delays, and scheduling decisions without introducing threads or process management. For a local simulation, it keeps the code close to the mental model of a dispatcher, a queue, and asynchronous workers.

## Core Components

* **Dispatcher**: Promotes scheduled URLs into Redis and keeps domain state up to date.
* **Scheduler**: Enforces per-domain politeness and routes pages to Kafka.
* **Crawler**: Simulates fetch latency, random failure, freshness updates, and discovery.
* **URL Processor**: Deduplicates new URLs and stores first-seen pages.
* **Batch Processor**: Refreshes priority scores from historical encounter volume and defines 3 levels of priority.


## Technology Notes

* `AsyncZSet` in `simulators/redis_sim.py` models Redis sorted-set behavior for scheduling.
* The `orchestration/scaler.py` loop can scale crawler workers up or down when queues start filling up.
* `core/dispatcher_mode.py` lets the dispatcher fetch from time buckets faster or slower depending on system load.
* Kafka is partitioned by domain to keep worker routing deterministic.
* Elasticsearch is simulated with an in-memory duplicate check to keep the local demo lightweight.