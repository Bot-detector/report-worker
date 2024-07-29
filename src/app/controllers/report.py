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

    async def get_or_insert(self):
        raise NotImplementedError()
