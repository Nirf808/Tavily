import asyncio
from typing import List, Tuple
from simulators import DataStore
from core.dispatcher_mode import DispatcherMode
from core.batch_processor import batch_processor_loop
from core.dispatcher import dispatcher_loop
from core.scheduler import scheduler_loop
from core.crawler import Crawler
from core.url_process import url_process_loop
from orchestration.telemetry import telemetry_loop, tracker
from consts import INITIAL_WORKERS, MAX_WORKERS, SCALE_UP_THRESHOLD, SCALE_CHECK_INTERVAL, REDIS_PENDING_MEDIUM_THRESHOLD, REDIS_PENDING_SLOW_THRESHOLD


def resolve_dispatcher_mode(pending_pages: int) -> DispatcherMode:
    if pending_pages >= REDIS_PENDING_SLOW_THRESHOLD:
        return DispatcherMode.SLOW
    if pending_pages >= REDIS_PENDING_MEDIUM_THRESHOLD:
        return DispatcherMode.MEDIUM
    return DispatcherMode.FAST


def spawn_worker(datastore: DataStore, worker_id: int) -> asyncio.Task:
    crawler = Crawler(worker_id=worker_id, pages_topic=datastore.kafka.pages_topic, datastore=datastore)
    return asyncio.create_task(crawler.run())


def start_components(datastore: DataStore) -> Tuple[List[asyncio.Task], List[asyncio.Task]]:
    tasks = [
        asyncio.create_task(dispatcher_loop(datastore)),
        asyncio.create_task(scheduler_loop(datastore)),
        asyncio.create_task(url_process_loop(datastore)),
        asyncio.create_task(batch_processor_loop(datastore)),
        asyncio.create_task(telemetry_loop(datastore)),
    ]
    worker_tasks = [spawn_worker(datastore, i) for i in range(INITIAL_WORKERS)]
    tasks.extend(worker_tasks)
    return tasks, worker_tasks


def should_scale_up(pending_records: int, next_worker_id: int) -> bool:
    return pending_records >= SCALE_UP_THRESHOLD and next_worker_id < MAX_WORKERS


async def scaler_loop(datastore: DataStore) -> None:
    tasks, worker_tasks = start_components(datastore)
    next_worker_id = INITIAL_WORKERS
    while True:
        await asyncio.sleep(SCALE_CHECK_INTERVAL)
        previous_mode = datastore.dispatcher_mode
        next_mode = resolve_dispatcher_mode(tracker.pages_pending)
        if next_mode != previous_mode:
            datastore.dispatcher_mode = next_mode
            print(f"[State] Dispatcher mode: {previous_mode.value} -> {next_mode.value}")
        if should_scale_up(tracker.mq_pending, next_worker_id):
            datastore.kafka.pages_topic.add_client(next_worker_id)
            task = spawn_worker(datastore, next_worker_id)
            worker_tasks.append(task)
            tasks.append(task)
            print(f"[Scale] Spawning worker {next_worker_id} | Pending (MQ): {tracker.mq_pending}")
            next_worker_id += 1
