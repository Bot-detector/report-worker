import logging
import time
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Metadata(BaseModel):
    version: str


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


class BaseReport(BaseModel):
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


class ReportInQV1(BaseReport):
    reporter: str
    reported: str


class ReportInQV2(BaseReport):
    reporter_id: int
    reported_id: int


class KafkaReport(ReportInQV2):
    metadata: Metadata = Metadata(version="v2.0.0")


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
    if report_in_queue.ts > 1735736400:
        logger.warning(f"{report_in_queue.ts=} > 2025-01-01, {report_in_queue=}")
        return None
    gmt = time.gmtime(report_in_queue.ts)
    human_time = time.strftime("%Y-%m-%d %H:%M:%S", gmt)
    human_time = datetime.fromtimestamp(report_in_queue.ts)
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


def convert_stg_to_kafka_report(stg_report: StgReportCreate) -> KafkaReport:
    equipment = Equipment(
        equip_head_id=stg_report.equip_head_id,
        equip_amulet_id=stg_report.equip_amulet_id,
        equip_torso_id=stg_report.equip_torso_id,
        equip_legs_id=stg_report.equip_legs_id,
        equip_boots_id=stg_report.equip_boots_id,
        equip_cape_id=stg_report.equip_cape_id,
        equip_hands_id=stg_report.equip_hands_id,
        equip_weapon_id=stg_report.equip_weapon_id,
        equip_shield_id=stg_report.equip_shield_id,
    )

    return KafkaReport(
        region_id=stg_report.region_id,
        x_coord=stg_report.x_coord,
        y_coord=stg_report.y_coord,
        z_coord=stg_report.z_coord,
        ts=int(stg_report.timestamp.timestamp() * 1000),
        manual_detect=int(stg_report.manual_detect)
        if stg_report.manual_detect is not None
        else 0,
        on_members_world=stg_report.on_members_world
        if stg_report.on_members_world is not None
        else 0,
        on_pvp_world=int(stg_report.on_pvp_world)
        if stg_report.on_pvp_world is not None
        else 0,
        world_number=stg_report.world_number
        if stg_report.world_number is not None
        else 0,
        equipment=equipment,
        equip_ge_value=stg_report.equip_ge_value
        if stg_report.equip_ge_value is not None
        else 0,
        reporter_id=stg_report.reportingID,
        reported_id=stg_report.reportedID,
        metadata=Metadata(version="v2.0.0"),
    )
