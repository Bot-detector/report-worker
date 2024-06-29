import json
import os
import random

import _kafka_config
from faker import Faker
from kafka import KafkaProducer


def send_data(producer: KafkaProducer):
    example = {
        "reporter": "player1",
        "reported": "player2",
        "region_id": 14652,
        "x_coord": 3682,
        "y_coord": 3851,
        "z_coord": 0,
        "ts": 1704223737,
        "manual_detect": 0,
        "on_members_world": 1,
        "on_pvp_world": 0,
        "world_number": 324,
        "equipment": {
            "equip_head_id": 13592,
            "equip_amulet_id": None,
            "equip_torso_id": 13596,
            "equip_legs_id": 13598,
            "equip_boots_id": 13602,
            "equip_cape_id": 13594,
            "equip_hands_id": 13600,
            "equip_weapon_id": 1381,
            "equip_shield_id": None,
        },
        "equip_ge_value": 0,
    }

    # Generate fixed players with the same names and scores
    len_messages = 100_000
    players = [f"player{i}" for i in range(500)]
    faker = Faker()
    for i in range(len_messages):
        msg = {
            "reporter": random.choice(players),
            "reported": random.choice(players),
            "region_id": random.randint(10_000, 10_500),
            "x_coord": random.randint(0, 5000),
            "y_coord": random.randint(0, 5000),
            "z_coord": random.randint(0, 3),
            "ts": int(faker.date_time().timestamp()),
            "manual_detect": random.choice([0, 1]),
            "on_members_world": random.choice([0, 1]),
            "on_pvp_world": random.choice([0, 1]),
            "world_number": random.randint(300, 500),
            "equipment": {
                k: random.choice(
                    [None, *[random.randint(0, 20000) for _ in range(100)]]
                )
                for k in example["equipment"].keys()
            },
            "equip_ge_value": 0,
        }
        producer.send(topic="report", value=msg)
        print(i, msg)
    print("Data insertion completed.")


def main():
    _kafka_config.create_topics()
    # Get the Kafka broker address from the environment variable
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9094")

    # Create the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode(),
    )
    send_data(producer=producer)


if __name__ == "__main__":
    main()
