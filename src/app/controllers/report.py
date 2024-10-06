import itertools
import logging

import sqlalchemy as sqla
from _cache import SimpleALRUCache
from app.controllers.db_handler import DatabaseHandler
from app.views.report import StgReportCreate, StgReportInDB
from database.database import model_to_dict
from database.models.report import Report as DBReport
from database.models.report import StgReport as DBSTGReport
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ReportController(DatabaseHandler):
    def __init__(self, session: AsyncSession):
        self.session = session
        self.cache = SimpleALRUCache(max_size=2000)

    async def get(
        self, reported_id: int, reporting_id: int, region_id: int
    ) -> StgReportInDB:
        report = await self.cache.get(key=(reported_id, reporting_id, reporting_id))

        if isinstance(report, StgReportInDB):
            return report

        sql = sqla.select(DBReport).where(
            sqla.and_(
                DBReport.reportedID == reported_id,
                DBReport.reportingID == reporting_id,
                DBReport.region_id == region_id,
            )
        )
        result = await self.session.execute(sql)
        data = result.scalars().all()
        report = StgReportInDB(**model_to_dict(data[0])) if data else None

        if isinstance(report, StgReportInDB):
            await self.cache.put(
                key=(reported_id, reporting_id, reporting_id),
                value=report,
            )
        return report

    async def insert(self, reports: list[StgReportCreate]) -> None:
        sql = sqla.insert(DBSTGReport).values([r.model_dump() for r in reports])
        await self.session.execute(sql)
        return

    async def insert_sighting(self, reports: list[StgReportCreate]) -> None:
        sightings = []
        for report in reports:
            sightings.append(
                {
                    "reporting_id": report.reportingID,
                    "reported_id": report.reportedID,
                    "manual_detect": 1 if report.manual_detect else 0,
                }
            )

        sql_create_temp_sighting = """
            CREATE TEMPORARY TABLE temp_sighting (
                reporting_id INT,
                reported_id INT,
                manual_detect TINYINT DEFAULT 0
            ) ENGINE=MEMORY;
        """
        sql_temp_sighting = """
            INSERT INTO temp_sighting (reporting_id, reported_id, manual_detect)
            VALUES (:reporting_id, :reported_id, :manual_detect)
        """
        sql_insert_sighting = """
            INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
            SELECT DISTINCT reporting_id, reported_id, manual_detect FROM temp_sighting ts
            WHERE NOT EXISTS (
                SELECT 1 FROM report_sighting rs
                WHERE 1
                    AND ts.reporting_id = rs.reporting_id
                    AND ts.reported_id = rs.reported_id
                    AND ts.manual_detect = rs.manual_detect
            );
        """
        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_sighting;"))
        # logger.debug("Dropped previous temp table")

        await self.session.execute(sqla.text(sql_create_temp_sighting))
        # logger.debug("Created temp table")

        await self.session.execute(sqla.text(sql_temp_sighting), sightings)
        # logger.debug("Inserted into temp table")

        await self.session.execute(sqla.text(sql_insert_sighting))
        # logger.debug("Inserted into main table")

        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_sighting;"))
        # logger.debug("Cleaned up temp table")

    async def insert_gear(self, reports: list[StgReportCreate]) -> None:
        sets = []
        keys = (
            "equip_head_id",
            "equip_amulet_id",
            "equip_torso_id",
            "equip_legs_id",
            "equip_boots_id",
            "equip_cape_id",
            "equip_hands_id",
            "equip_weapon_id",
            "equip_shield_id",
        )
        for report in reports:
            _report = report.model_dump()
            sets.append({k: v for k, v in _report.items() if k in keys})

        sql_create_temp_gear = """
            CREATE TEMPORARY TABLE temp_gear (
                `equip_head_id` SMALLINT,
                `equip_amulet_id` SMALLINT,
                `equip_torso_id` SMALLINT,
                `equip_legs_id` SMALLINT,
                `equip_boots_id` SMALLINT,
                `equip_cape_id` SMALLINT,
                `equip_hands_id` SMALLINT,
                `equip_weapon_id` SMALLINT,
                `equip_shield_id` SMALLINT
            ) ENGINE=MEMORY;
        """
        sql_temp_gear = """
            INSERT INTO temp_gear (
                equip_head_id,
                equip_amulet_id,
                equip_torso_id,
                equip_legs_id,
                equip_boots_id,
                equip_cape_id,
                equip_hands_id,
                equip_weapon_id,
                equip_shield_id
            )
            VALUES (
                :equip_head_id,
                :equip_amulet_id,
                :equip_torso_id,
                :equip_legs_id,
                :equip_boots_id,
                :equip_cape_id,
                :equip_hands_id,
                :equip_weapon_id,
                :equip_shield_id
            )
        """
        sql_insert_gear = """
            INSERT INTO report_gear (
                equip_head_id,
                equip_amulet_id,
                equip_torso_id,
                equip_legs_id,
                equip_boots_id,
                equip_cape_id,
                equip_hands_id,
                equip_weapon_id,
                equip_shield_id
            )
            SELECT DISTINCT
                tg.equip_head_id,
                tg.equip_amulet_id,
                tg.equip_torso_id,
                tg.equip_legs_id,
                tg.equip_boots_id,
                tg.equip_cape_id,
                tg.equip_hands_id,
                tg.equip_weapon_id,
                tg.equip_shield_id
            FROM temp_gear tg
            WHERE NOT EXISTS (
                SELECT 1
                FROM report_gear rg
                WHERE tg.equip_head_id = rg.equip_head_id
                AND tg.equip_amulet_id = rg.equip_amulet_id
                AND tg.equip_torso_id = rg.equip_torso_id
                AND tg.equip_legs_id = rg.equip_legs_id
                AND tg.equip_boots_id = rg.equip_boots_id
                AND tg.equip_cape_id = rg.equip_cape_id
                AND tg.equip_hands_id = rg.equip_hands_id
                AND tg.equip_weapon_id = rg.equip_weapon_id
                AND tg.equip_shield_id = rg.equip_shield_id
            );
        """
        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_gear;"))
        # logger.debug("Dropped previous temp table")

        await self.session.execute(sqla.text(sql_create_temp_gear))
        # logger.debug("Created temp table")

        await self.session.execute(sqla.text(sql_temp_gear), sets)
        # logger.debug("Inserted into temp table")

        await self.session.execute(sqla.text(sql_insert_gear))
        # logger.debug("Inserted into main table")

        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_gear;"))
        # logger.debug("Cleaned up temp table")

    async def insert_location(self, reports: list[StgReportCreate]) -> None:
        locations = []
        keys = [
            "region_id",
            "x_coord",
            "y_coord",
            "z_coord",
        ]
        for report in reports:
            data = report.model_dump()
            locations.append({k: v for k, v in data.items() if k in keys})

        sql_create_temp_location = """
            CREATE TEMPORARY TABLE temp_location (
                `region_id` MEDIUMINT UNSIGNED NOT NULL,
                `x_coord` MEDIUMINT UNSIGNED NOT NULL,
                `y_coord` MEDIUMINT UNSIGNED NOT NULL,
                `z_coord` MEDIUMINT UNSIGNED NOT NULL
            ) ENGINE=MEMORY;
        """
        sql_temp_location = """
            INSERT INTO temp_location (region_id, x_coord, y_coord, z_coord)
            VALUES (:region_id, :x_coord, :y_coord, :z_coord)
        """
        sql_insert_location = """
            INSERT INTO report_location (region_id, x_coord, y_coord, z_coord)
            SELECT DISTINCT region_id, x_coord, y_coord, z_coord FROM temp_location tl
            WHERE NOT EXISTS (
                SELECT 1 FROM report_location rl
                WHERE 1
                    AND tl.region_id = rl.region_id
                    AND tl.x_coord = rl.x_coord
                    AND tl.y_coord = rl.y_coord
                    AND tl.z_coord = rl.z_coord
            );
        """
        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_location;"))
        # logger.debug("Dropped previous temp table")

        await self.session.execute(sqla.text(sql_create_temp_location))
        # logger.debug("Created temp table")

        await self.session.execute(sqla.text(sql_temp_location), locations)
        # logger.debug("Inserted into temp table")

        await self.session.execute(sqla.text(sql_insert_location))
        # logger.debug("Inserted into main table")

        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_location;"))
        # logger.debug("Cleaned up temp table")

    async def insert_report(self, reports: list[StgReportCreate]) -> None:
        _reports = []
        sighting_keys = (
            "reportingID",
            "reportedID",
            "manual_detect",
        )
        gear_keys = (
            "equip_head_id",
            "equip_amulet_id",
            "equip_torso_id",
            "equip_legs_id",
            "equip_boots_id",
            "equip_cape_id",
            "equip_hands_id",
            "equip_weapon_id",
            "equip_shield_id",
        )
        location_keys = (
            "region_id",
            "x_coord",
            "y_coord",
            "z_coord",
        )
        report_keys = (
            "timestamp",
            "on_members_world",
            "on_pvp_world",
            "world_number",
        )
        keys = [*sighting_keys, *gear_keys, *location_keys, *report_keys]

        for report in reports:
            data = report.model_dump()
            _reports.append({k: v for k, v in data.items() if k in keys})

        # temp table with all unique fields
        sql_create_temp_report = """
            CREATE TEMPORARY TABLE temp_report (
                /*sighting*/
                reporting_id INT,
                reported_id INT,
                manual_detect TINYINT DEFAULT 0,
                /*gear*/
                `equip_head_id` SMALLINT,
                `equip_amulet_id` SMALLINT,
                `equip_torso_id` SMALLINT,
                `equip_legs_id` SMALLINT,
                `equip_boots_id` SMALLINT,
                `equip_cape_id` SMALLINT,
                `equip_hands_id` SMALLINT,
                `equip_weapon_id` SMALLINT,
                `equip_shield_id` SMALLINT,
                /*location*/
                `region_id` MEDIUMINT UNSIGNED NOT NULL,
                `x_coord` MEDIUMINT UNSIGNED NOT NULL,
                `y_coord` MEDIUMINT UNSIGNED NOT NULL,
                `z_coord` MEDIUMINT UNSIGNED NOT NULL,
                /*report*/
                `reported_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `on_members_world` TINYINT(1) DEFAULT NULL,
                `on_pvp_world` TINYINT(1) DEFAULT NULL,
                `world_number` SMALLINT UNSIGNED DEFAULT NULL
            ) ENGINE=MEMORY;
        """

        sql_temp_report = """
            INSERT INTO temp_report (
                /*sighting*/
                reporting_id,
                reported_id,
                manual_detect,
                /*gear*/
                equip_head_id,
                equip_amulet_id,
                equip_torso_id,
                equip_legs_id,
                equip_boots_id,
                equip_cape_id,
                equip_hands_id,
                equip_weapon_id,
                equip_shield_id,
                /*location*/
                region_id,
                x_coord,
                y_coord,
                z_coord,
                /*report*/
                reported_at,
                on_members_world,
                on_pvp_world,
                world_number
            )
            VALUES (
                :reportingID,
                :reportedID,
                :manual_detect,
                :equip_head_id,
                :equip_amulet_id,
                :equip_torso_id,
                :equip_legs_id,
                :equip_boots_id,
                :equip_cape_id,
                :equip_hands_id,
                :equip_weapon_id,
                :equip_shield_id,
                :region_id,
                :x_coord,
                :y_coord,
                :z_coord,
                :timestamp,
                :on_members_world,
                :on_pvp_world,
                :world_number
            );

        """
        sql_insert_report = """
            INSERT INTO report (
                report_sighting_id,
                report_location_id,
                report_gear_id,
                reported_at,
                on_members_world,
                on_pvp_world,
                world_number,
                region_id
            )
            SELECT
                rs.report_sighting_id,
                rl.report_location_id,
                rg.report_gear_id,
                tr.reported_at,
                tr.on_members_world,
                tr.on_pvp_world,
                tr.world_number,
                tr.region_id
            FROM temp_report tr
            JOIN report_sighting rs
                ON rs.reporting_id = tr.reporting_id
                AND rs.reported_id = tr.reported_id
            JOIN report_location rl
                ON rl.region_id = tr.region_id
                AND rl.x_coord = tr.x_coord
                AND rl.y_coord = tr.y_coord
                AND rl.z_coord = tr.z_coord
            JOIN report_gear rg
                ON rg.equip_head_id = tr.equip_head_id
                AND rg.equip_amulet_id = tr.equip_amulet_id
                AND rg.equip_torso_id = tr.equip_torso_id
                AND rg.equip_legs_id = tr.equip_legs_id
                AND rg.equip_boots_id = tr.equip_boots_id
                AND rg.equip_cape_id = tr.equip_cape_id
                AND rg.equip_hands_id = tr.equip_hands_id
                AND rg.equip_weapon_id = tr.equip_weapon_id
                AND rg.equip_shield_id = tr.equip_shield_id
            ;
        """
        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_report;"))
        # logger.debug("Dropped previous temp table")

        await self.session.execute(sqla.text(sql_create_temp_report))
        # logger.debug("Created temp table")

        await self.session.execute(sqla.text(sql_temp_report), _reports)
        # logger.debug("Inserted into temp table")

        await self.session.execute(sqla.text(sql_insert_report))
        # logger.debug("Inserted into main table")

        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_report;"))
        # logger.debug("Cleaned up temp table")

    async def get_or_insert(self):
        raise NotImplementedError()
