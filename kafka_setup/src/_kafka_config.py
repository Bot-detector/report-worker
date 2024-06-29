import os

from kafka.admin import KafkaAdminClient, NewTopic


def create_topics():
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create Kafka topics
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)

    topics = admin_client.list_topics()
    print("existing topics", topics)

    if not topics == []:
        admin_client.delete_topics(topics)

    res = admin_client.create_topics(
        [
            NewTopic(
                name="report",
                num_partitions=4,
                replication_factor=1,
            ),
        ]
    )

    print("created_topic", res)

    topics = admin_client.list_topics()
    print("all topics", topics)
    return
