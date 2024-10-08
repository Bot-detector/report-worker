import logging

import sqlalchemy as sqla
from _cache import SimpleALRUCache
from app.controllers.db_handler import DatabaseHandler
from app.views.report import StgReportCreate, StgReportInDB
from database.database import model_to_dict
from database.models.report import Report as DBReport
from database.models.report import StgReport as DBSTGReport
from sqlalchemy import TextClause
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

    def _parse_reports(self, reports: list[StgReportCreate]) -> list[dict]:
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
        return [{k: v for k, v in r.model_dump().items() if k in keys} for r in reports]

    def _create_temp_report(self) -> TextClause:
        return sqla.text(
            """
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
                `on_members_world` TINYINT DEFAULT NULL,
                `on_pvp_world` TINYINT DEFAULT NULL,
                `world_number` SMALLINT UNSIGNED DEFAULT NULL
            ) ENGINE=MEMORY;
        """
        )

    def _insert_temp_report(self) -> TextClause:
        return sqla.text(
            """
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
        )

    def _insert_sighting(self) -> TextClause:
        return sqla.text(
            """
            INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
            SELECT DISTINCT tr.reporting_id, tr.reported_id, tr.manual_detect FROM temp_report tr
            WHERE NOT EXISTS (
                SELECT 1 FROM report_sighting rs
                WHERE 1
                    AND tr.reporting_id = rs.reporting_id
                    AND tr.reported_id = rs.reported_id
                    AND tr.manual_detect = rs.manual_detect
            );
        """
        )

    def _insert_gear(self) -> TextClause:
        return sqla.text(
            """
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
                tr.equip_head_id,
                tr.equip_amulet_id,
                tr.equip_torso_id,
                tr.equip_legs_id,
                tr.equip_boots_id,
                tr.equip_cape_id,
                tr.equip_hands_id,
                tr.equip_weapon_id,
                tr.equip_shield_id
            FROM temp_report tr
            WHERE NOT EXISTS (
                SELECT
                    1
                FROM report_gear rg
                WHERE tr.equip_head_id = rg.equip_head_id
                AND tr.equip_amulet_id = rg.equip_amulet_id
                AND tr.equip_torso_id = rg.equip_torso_id
                AND tr.equip_legs_id = rg.equip_legs_id
                AND tr.equip_boots_id = rg.equip_boots_id
                AND tr.equip_cape_id = rg.equip_cape_id
                AND tr.equip_hands_id = rg.equip_hands_id
                AND tr.equip_weapon_id = rg.equip_weapon_id
                AND tr.equip_shield_id = rg.equip_shield_id
            );
        """
        )

    def _insert_location(self) -> TextClause:
        return sqla.text(
            """
            INSERT INTO report_location (region_id, x_coord, y_coord, z_coord)
            SELECT DISTINCT tr.region_id, tr.x_coord, tr.y_coord, tr.z_coord FROM temp_report tr
            WHERE NOT EXISTS (
                SELECT 1 FROM report_location rl
                WHERE 1
                    AND tr.region_id = rl.region_id
                    AND tr.x_coord = rl.x_coord
                    AND tr.y_coord = rl.y_coord
                    AND tr.z_coord = rl.z_coord
            );
            """
        )

    def _insert_report(self) -> TextClause:
        return sqla.text(
            """
                INSERT IGNORE INTO report (
                    report_sighting_id,
                    report_location_id,
                    report_gear_id,
                    reported_at,
                    on_members_world,
                    on_pvp_world,
                    world_number,
                    region_id
                )
                SELECT DISTINCT
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
                WHERE NOT EXISTS (
                    SELECT 1 FROM report rp
                    WHERE 1
                        AND rs.report_sighting_id = rp.report_sighting_id
                        AND rl.report_location_id = rp.report_location_id
                        AND tr.region_id = rp.region_id
                )
                ;
            """
        )

    async def insert_report(self, reports: list[StgReportCreate]) -> None:
        _reports = self._parse_reports(reports=reports)
        sql_create_temp_report = self._create_temp_report()
        sql_insert_temp_report = self._insert_temp_report()
        sql_insert_sighting = self._insert_sighting()
        sql_insert_gear = self._insert_gear()
        sql_insert_location = self._insert_location()
        sql_insert_report = self._insert_report()

        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_report;"))
        await self.session.execute(sql_create_temp_report)
        await self.session.execute(sql_insert_temp_report, params=_reports)
        await self.session.execute(sql_insert_sighting)
        await self.session.execute(sql_insert_gear)
        await self.session.execute(sql_insert_location)
        await self.session.execute(sql_insert_report)
        await self.session.execute(sqla.text("DROP TABLE IF EXISTS temp_report;"))

    async def get_or_insert(self):
        raise NotImplementedError()
