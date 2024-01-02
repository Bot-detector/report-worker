import asyncio
import json
import uuid
from asyncio import Queue

from aiokafka import AIOKafkaConsumer


# Asynchronous function to create a Kafka consumer
async def kafka_consumer(topic: str, group: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9094"],
        group_id=group,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    return consumer


# Asynchronous function to receive messages from the Kafka consumer and put them in a queue
async def receive_messages(consumer: AIOKafkaConsumer, receive_queue: Queue):
    async for message in consumer:
        value = message.value
        await receive_queue.put(value)


# Function to save data as a JSON file
def save(data, file_name):
    try:
        # Open a JSON file and write the data
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Successfully converted list of dictionaries to JSON file: {file_name}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Asynchronous function to generate example data from the queue and save it as a JSON file
async def generate_example_data(queue: Queue):
    MAX_LEN = 1000
    count, empty_count = 0, 0
    data = []
    while True:
        if queue.empty():
            await asyncio.sleep(1)
            empty_count += 1
            if empty_count > 10:
                break
            continue

        item: dict = await queue.get()
        print(item)

        if item is None or count >= MAX_LEN:
            break

        data.append(item)

        if count % 100 == 0:
            print(f"{count}/{MAX_LEN}")

        queue.task_done()
        count += 1

    save(data, "kafka_data.json")


# Asynchronous main function coordinating the whole process
async def main():
    receive_queue = Queue(maxsize=100)
    print("group", f"test-{uuid.uuid4()}")
    consumer = await kafka_consumer(topic="report", group=f"test-{uuid.uuid4()}")

    asyncio.create_task(
        receive_messages(consumer=consumer, receive_queue=receive_queue)
    )
    await generate_example_data(receive_queue)

    await consumer.stop()


# Entry point of the program, running the main asynchronous function using asyncio
if __name__ == "__main__":
    asyncio.run(main())
