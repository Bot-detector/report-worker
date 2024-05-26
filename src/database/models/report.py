from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Integer,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class StgReport(Base):
    __tablename__ = "stgReports"

    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    reportedID = Column(Integer, nullable=False)
    reportingID = Column(Integer, nullable=False)
    region_id = Column(Integer, nullable=False)
    x_coord = Column(Integer, nullable=False)
    y_coord = Column(Integer, nullable=False)
    z_coord = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, server_default=func.now())
    manual_detect = Column(Boolean)
    on_members_world = Column(Integer)
    on_pvp_world = Column(Boolean)
    world_number = Column(Integer)
    equip_head_id = Column(Integer)
    equip_amulet_id = Column(Integer)
    equip_torso_id = Column(Integer)
    equip_legs_id = Column(Integer)
    equip_boots_id = Column(Integer)
    equip_cape_id = Column(Integer)
    equip_hands_id = Column(Integer)
    equip_weapon_id = Column(Integer)
    equip_shield_id = Column(Integer)
    equip_ge_value = Column(BigInteger)


class Report(Base):
    __tablename__ = "Reports"

    ID = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    reportedID = Column(Integer, nullable=False)
    reportingID = Column(Integer, nullable=False)
    region_id = Column(Integer, nullable=False)
    x_coord = Column(Integer, nullable=False)
    y_coord = Column(Integer, nullable=False)
    z_coord = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, server_default=func.now())
    manual_detect = Column(Boolean)
    on_members_world = Column(Integer)
    on_pvp_world = Column(Boolean)
    world_number = Column(Integer)
    equip_head_id = Column(Integer)
    equip_amulet_id = Column(Integer)
    equip_torso_id = Column(Integer)
    equip_legs_id = Column(Integer)
    equip_boots_id = Column(Integer)
    equip_cape_id = Column(Integer)
    equip_hands_id = Column(Integer)
    equip_weapon_id = Column(Integer)
    equip_shield_id = Column(Integer)
    equip_ge_value = Column(BigInteger)
