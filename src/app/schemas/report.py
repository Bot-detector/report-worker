from typing import Optional

from pydantic import BaseModel


class Equipment(BaseModel):
    equip_head_id: Optional[int] = None
    equip_amulet_id: Optional[int] = None
    equip_torso_id: Optional[int] = None
    equip_legs_id: Optional[int] = None
    equip_boots_id: Optional[int] = None
    equip_cape_id: Optional[int] = None
    equip_hands_id: Optional[int] = None
    equip_weapon_id: Optional[int] = None
    equip_shield_id: Optional[int] = None


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
