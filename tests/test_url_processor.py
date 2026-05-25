from simulators.datastore import DataStore


def test_url_deduplication_writes_new_to_cassandra():
    ds = DataStore()

    urls = ['http://a.com/1', 'http://a.com/2', 'http://a.com/1']

    # process batch (synchronous internal function)
    from core import url_process
    url_process._process_batch(urls, ds)

    # encounter_logs should record all 3 encounters
    assert len(ds.s3.encounter_logs) == 3

    # cassandra.source_of_truth should contain the unique new URLs (2 unique)
    assert 'http://a.com/1' in ds.cassandra.source_of_truth
    assert 'http://a.com/2' in ds.cassandra.source_of_truth

    # time_buckets should have enqueued the new records
    # find any non-empty bucket
    found = any(len(bucket) > 0 for bucket in ds.cassandra.time_buckets.values())
    assert found
