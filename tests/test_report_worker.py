from AioKafkaEngine import ProducerEngine

producer = ProducerEngine(
    bootstrap_servers="localhost:9092",
    report_interval=5,
    queue_size=100,
)


async def test_v1(producer: ProducerEngine):
    # Example usage:
    json_example_v1 = {
        "reporter": "Cyborger1",
        "reported": "Shpinki",
        "region_id": 14651,
        "x_coord": 3682,
        "y_coord": 3837,
        "z_coord": 0,
        "ts": 1704223741,
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
    await producer.send_queue.put(item=json_example_v1)


async def test_main():
    await producer.start_engine(topic="report")
    await test_v1(producer=producer)
    await producer.stop_engine()
