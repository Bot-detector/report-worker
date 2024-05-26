import time
from datetime import datetime
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


class ReportInQueue(BaseModel):
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


class StgReportCreate(BaseModel):
    reportedID: int
    reportingID: int
    region_id: int
    x_coord: int
    y_coord: int
    z_coord: int
    timestamp: datetime
    manual_detect: Optional[bool] = None
    on_members_world: Optional[int] = None
    on_pvp_world: Optional[bool] = None
    world_number: Optional[int] = None
    equip_head_id: Optional[int] = None
    equip_amulet_id: Optional[int] = None
    equip_torso_id: Optional[int] = None
    equip_legs_id: Optional[int] = None
    equip_boots_id: Optional[int] = None
    equip_cape_id: Optional[int] = None
    equip_hands_id: Optional[int] = None
    equip_weapon_id: Optional[int] = None
    equip_shield_id: Optional[int] = None
    equip_ge_value: Optional[int] = None


class StgReportUpdate(BaseModel):
    reportedID: Optional[int] = None
    reportingID: Optional[int] = None
    region_id: Optional[int] = None
    x_coord: Optional[int] = None
    y_coord: Optional[int] = None
    z_coord: Optional[int] = None
    manual_detect: Optional[bool] = None
    on_members_world: Optional[int] = None
    on_pvp_world: Optional[bool] = None
    world_number: Optional[int] = None
    equip_head_id: Optional[int] = None
    equip_amulet_id: Optional[int] = None
    equip_torso_id: Optional[int] = None
    equip_legs_id: Optional[int] = None
    equip_boots_id: Optional[int] = None
    equip_cape_id: Optional[int] = None
    equip_hands_id: Optional[int] = None
    equip_weapon_id: Optional[int] = None
    equip_shield_id: Optional[int] = None
    equip_ge_value: Optional[int] = None


class StgReportInDB(StgReportCreate):
    ID: int
    created_at: datetime


class StgReport(StgReportInDB):
    pass


def convert_report_q_to_db(
    reported_id: int, reporting_id: int, report_in_queue: ReportInQueue
) -> StgReportCreate:
    gmt = time.gmtime(report_in_queue.ts)
    human_time = time.strftime("%Y-%m-%d %H:%M:%S", gmt)
    return StgReportCreate(
        reportedID=reported_id,
        reportingID=reporting_id,
        timestamp=human_time,
        region_id=report_in_queue.region_id,
        x_coord=report_in_queue.x_coord,
        y_coord=report_in_queue.y_coord,
        z_coord=report_in_queue.z_coord,
        manual_detect=bool(report_in_queue.manual_detect),
        on_members_world=report_in_queue.on_members_world,
        on_pvp_world=bool(report_in_queue.on_pvp_world),
        world_number=report_in_queue.world_number,
        equip_head_id=report_in_queue.equipment.equip_head_id,
        equip_amulet_id=report_in_queue.equipment.equip_amulet_id,
        equip_torso_id=report_in_queue.equipment.equip_torso_id,
        equip_legs_id=report_in_queue.equipment.equip_legs_id,
        equip_boots_id=report_in_queue.equipment.equip_boots_id,
        equip_cape_id=report_in_queue.equipment.equip_cape_id,
        equip_hands_id=report_in_queue.equipment.equip_hands_id,
        equip_weapon_id=report_in_queue.equipment.equip_weapon_id,
        equip_shield_id=report_in_queue.equipment.equip_shield_id,
        equip_ge_value=report_in_queue.equip_ge_value,
    )
