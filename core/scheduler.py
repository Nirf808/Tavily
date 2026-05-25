import asyncio
import time
from simulators import DataStore
from consts import DOMAIN_QUEUE_BATCH_SIZE


async def process_domain_batch(datastore: DataStore):
    current_time = time.time()
    ready_domains = await fetch_ready_domains(datastore, current_time, DOMAIN_QUEUE_BATCH_SIZE)

    if not ready_domains:
        await asyncio.sleep(0.1)
        return

    await process_ready_domains(datastore, ready_domains, current_time)


async def fetch_ready_domains(datastore: DataStore, current_time: float, batch_size: int) -> list:
    return await datastore.redis.domains_queue.zrange_with_scores(0, batch_size, max_score=current_time)


async def process_ready_domains(datastore: DataStore, ready_domains: list, current_time: float):
    for _, domain in ready_domains:
        domain_state = datastore.redis.domain_states[domain]
        routed_count = await route_urls_to_workers(domain, domain_state, datastore, current_time)

        await handle_domain_post_routing(domain, domain_state, routed_count, datastore, current_time)


async def handle_domain_post_routing(domain: str, domain_state: dict, routed_count: int, datastore: DataStore, current_time: float):
    if routed_count > 0:
        domain_state['next_allowed_time_to_fetch'] = current_time + domain_state['allowed_requests_per_second']
        top_pages = await datastore.redis.pages_queues[domain].zrange_with_scores(0, 1)
        if top_pages:
            next_page_score = top_pages[0][0]
            new_score = max(next_page_score, domain_state['next_allowed_time_to_fetch'])
            await datastore.redis.domains_queue.zadd(domain, new_score)
        else:
            await datastore.redis.domains_queue.zrem(domain)


async def route_urls_to_workers(domain: str, domain_state: dict, datastore: DataStore, current_time: float) -> int:
    rps = domain_state['allowed_requests_per_second']

    ready_pages = await datastore.redis.pages_queues[domain].zrange_with_scores(0, rps, max_score=current_time, with_metadata=True)

    urls_to_remove = []
    for _, url, record in ready_pages:
        await datastore.kafka.produce_fetch_request(domain, record)
        urls_to_remove.append(url)

    if urls_to_remove:
        await datastore.redis.pages_queues[domain].zrem(*urls_to_remove)

    return len(urls_to_remove)


async def scheduler_loop(datastore: DataStore):
    """Pops domains ready to be fetched, validates politeness limits, and dispatches batches to Kafka."""
    while True:
        if len(datastore.redis.domains_queue) == 0:
            await asyncio.sleep(0.5)
            continue

        await process_domain_batch(datastore)
