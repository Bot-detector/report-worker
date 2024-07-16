from AioKafkaEngine import ConsumerEngine, ProducerEngine

from core.config import settings

consumer = ConsumerEngine(
    bootstrap_servers=[settings.KAFKA_HOST],
    group_id="report-worker",
    queue_size=10_000,
    report_interval=60,
)
producer = ProducerEngine(
    bootstrap_servers=[settings.KAFKA_HOST],
    report_interval=60,
    queue_size=100,
)
