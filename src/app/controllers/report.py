import sqlalchemy as sqla
from sqlalchemy.ext.asyncio import AsyncSession

from _cache import SimpleALRUCache
from app.controllers.db_handler import DatabaseHandler
from app.views.report import StgReportCreate, StgReportInDB
from database.database import model_to_dict
from database.models.report import Report as DBReport
from database.models.report import StgReport as DBSTGReport


class ReportController(DatabaseHandler):
    def __init__(self, session: AsyncSession):
        self.session = session
        self.cache = SimpleALRUCache(max_size=2000)

    async def get(
        self, reported_id: int, reporting_id: int, region_id: int
    ) -> StgReportInDB:
        report = self.cache.get(key=(reported_id, reporting_id, reporting_id))

        if report is not None:
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
        self.cache.put(key=(reported_id, reporting_id, reporting_id), value=report)
        return report

    async def insert(self, reports: list[StgReportCreate]) -> None:
        sql = sqla.insert(DBSTGReport).values([r.model_dump() for r in reports])
        await self.session.execute(sql)
        return

    async def get_or_insert(self):
        raise NotImplementedError()
