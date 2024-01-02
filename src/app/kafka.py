import asyncio
import json
import logging
import time
from asyncio import Event, Queue

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger(__name__)


def log_speed(
    counter: int, start_time: float, _queue: Queue, topic: str, interval: int = 60
) -> tuple[float, int]:
    # Calculate the time elapsed since the function started
    delta_time = time.time() - start_time

    # Check if the specified interval has not elapsed yet
    if delta_time < interval:
        # Return the original start time and the current counter value
        return start_time, counter

    # Calculate the processing speed (messages per second)
    speed = counter / delta_time

    # Log the processing speed and relevant information
    log_message = (
        f"{topic=}, qsize={_queue.qsize()}, "
        f"processed {counter} in {delta_time:.2f} seconds, {speed:.2f} msg/sec"
    )
    logger.info(log_message)

    # Return the current time and reset the counter to zero
    return time.time(), 0


async def kafka_consumer(topic: str, group: str, bootstrap_servers: list[str]):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    return consumer


async def kafka_producer(bootstrap_servers: list[str]):
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
    )
    await producer.start()
    return producer


async def receive_messages(
    consumer: AIOKafkaConsumer,
    receive_queue: Queue,
    shutdown_event: Event,
    batch_size: int = 200,
):
    while not shutdown_event.is_set():
        batch = await consumer.getmany(timeout_ms=1000, max_records=batch_size)
        for tp, messages in batch.items():
            logger.info(f"Partition {tp}: {len(messages)} messages")
            await asyncio.gather(*[receive_queue.put(m.value) for m in messages])
            logger.info("done")
            await consumer.commit()

    logger.info("shutdown")


async def send_messages(
    topic: str,
    producer: AIOKafkaProducer,
    send_queue: Queue,
    shutdown_event: Event,
):
    start_time = time.time()
    messages_sent = 0

    while not shutdown_event.is_set():
        start_time, messages_sent = log_speed(
            counter=messages_sent,
            start_time=start_time,
            _queue=send_queue,
            topic=topic,
        )
        if send_queue.empty():
            await asyncio.sleep(1)
            continue

        message = await send_queue.get()
        await producer.send(topic, value=message)
        send_queue.task_done()

        messages_sent += 1

    logger.info("shutdown")
