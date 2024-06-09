import sqlalchemy as sqla
from app.controllers.db_handler import DatabaseHandler
from app.views.report import (
    StgReportCreate,
    StgReportInDB,
)
from async_lru import alru_cache
from database.database import model_to_dict
from database.models.report import Report as DBReport
from database.models.report import StgReport as DBSTGReport
from sqlalchemy.ext.asyncio import AsyncSession


class ReportController(DatabaseHandler):
    def __init__(self, session: AsyncSession):
        self.session = session

    @alru_cache(maxsize=2048)
    async def get(
        self, reported_id: int, reporting_id: int, region_id: int
    ) -> StgReportInDB:
        sql = sqla.select(DBReport).where(
            sqla.and_(
                DBReport.reportedID == reported_id,
                DBReport.reportingID == reporting_id,
                DBReport.region_id == region_id,
            )
        )
        result = await self.session.execute(sql)
        data = result.scalars().all()
        return StgReportInDB(**model_to_dict(data[0])) if data else None

    async def insert(self, reports: list[StgReportCreate]) -> None:
        sql = sqla.insert(DBSTGReport).values([r.model_dump() for r in reports])
        await self.session.execute(sql)
        return

    async def get_or_insert(self):
        raise NotImplementedError()
