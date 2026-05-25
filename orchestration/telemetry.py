import asyncio
from simulators import DataStore

SLEEP_BETWEEN_SAMPLES = 3

class TelemetryTracker:
    def __init__(self) -> None:
        self.pages_fetched = 0
        self.new_pages_fetched = 0
        self.refetched_pages = 0
        self.pages_failed = 0
        self.pages_pending = 0
        self.mq_pending = 0

tracker = TelemetryTracker()

async def telemetry_loop(datastore: DataStore):
    """Periodically logs system health and throughput to the console."""
    while True:
        await asyncio.sleep(SLEEP_BETWEEN_SAMPLES)
        pages_pending = sum(len(q) for q in datastore.redis.pages_queues.values())
        mq_pending = datastore.kafka.pending_records()
        tracker.pages_pending = pages_pending
        tracker.mq_pending = mq_pending
        print(
            f"[Metrics] Fetched: {tracker.pages_fetched} (New: {tracker.new_pages_fetched}, Refetched: {tracker.refetched_pages}) | Failed: {tracker.pages_failed} | "
            f"Pending - Redis Sorted Set: {pages_pending} | Pending - Pages Topic: {mq_pending} | "
            f"Dispatcher: {datastore.dispatcher_mode.value} | S3 Raw Storage: {len(datastore.s3.raw_content)}"
        )
