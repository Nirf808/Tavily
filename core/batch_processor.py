import asyncio
from collections import Counter
from typing import Dict, List, Tuple

from consts import BATCH_PROCESS_INTERVAL
from simulators import DataStore


async def batch_processor_loop(datastore: DataStore) -> None:
    """Recomputes page priorities from S3 encounter logs every 30 seconds."""
    while True:
        print(f"[BatchProcessor] Running priority refresh | encounter_logs={len(datastore.s3.encounter_logs)}")
        await process_batch(datastore)
        await asyncio.sleep(BATCH_PROCESS_INTERVAL)


async def process_batch(datastore: DataStore) -> None:
    encounter_counts = _count_encounters(datastore.s3.encounter_logs)
    if not encounter_counts:
        return

    ranked_urls = _rank_urls(encounter_counts)
    _apply_priority_levels(datastore, ranked_urls)
    datastore.s3.encounter_logs.clear()


def _count_encounters(encounter_logs: List[dict]) -> Dict[str, int]:
    return Counter(log.get("url") for log in encounter_logs if log.get("url"))


def _rank_urls(encounter_counts: Dict[str, int]) -> List[Tuple[str, int]]:
    return sorted(encounter_counts.items(), key=lambda item: (item[1], item[0]))


def _apply_priority_levels(datastore: DataStore, ranked_urls: List[Tuple[str, int]]) -> None:
    total_urls = len(ranked_urls)
    lower_cutoff = total_urls // 3
    middle_cutoff = (2 * total_urls) // 3

    print(f"[BatchProcessor] Applying priorities to {total_urls} URLs")

    for index, (url, _) in enumerate(ranked_urls):
        record = datastore.cassandra.source_of_truth.get(url)
        if record is None:
            continue
        record.priority_score = _priority_for_index(index, lower_cutoff, middle_cutoff)

    print("[BatchProcessor] Priority refresh complete")


def _priority_for_index(index: int, lower_cutoff: int, middle_cutoff: int) -> int:
    if index < lower_cutoff:
        return 1
    if index < middle_cutoff:
        return 2
    return 3