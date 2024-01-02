from typing import Optional

from pydantic import BaseModel


class Equipment(BaseModel):
    equip_head_id: int
    equip_amulet_id: Optional[int]
    equip_torso_id: int
    equip_legs_id: int
    equip_boots_id: int
    equip_cape_id: int
    equip_hands_id: int
    equip_weapon_id: int
    equip_shield_id: Optional[int]


class Report(BaseModel):
    reporter: str
    reported: str
    region_id: int
    x_coord: int
    y_coord: int
    z_coord: int
    ts: int
    manual_detect: int
    on_members_world: int
    on_pvp_world: int
    world_number: int
    equipment: Equipment
    equip_ge_value: int


# Example usage:
json_data = {
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

report_instance = Report(**json_data)
print(report_instance)
