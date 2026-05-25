from dataclasses import dataclass, field
from simulators.cassandra_sim import CassandraSimulator
from simulators.redis_sim import RedisSimulator
from simulators.kafka_sim import KafkaSimulator
from simulators.s3_sim import S3Simulator
from simulators.elasticsearch_sim import ElasticsearchSimulator

@dataclass
class DataStore:
    cassandra: CassandraSimulator = field(default_factory=CassandraSimulator)
    redis: RedisSimulator = field(default_factory=RedisSimulator)
    kafka: KafkaSimulator = field(default_factory=KafkaSimulator)
    s3: S3Simulator = field(default_factory=S3Simulator)
    elasticsearch: ElasticsearchSimulator = field(default_factory=ElasticsearchSimulator)
