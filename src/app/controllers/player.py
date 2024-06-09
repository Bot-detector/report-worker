import sqlalchemy as sqla
from app.controllers.db_handler import DatabaseHandler
from app.views.player import PlayerCreate, PlayerInDB
from async_lru import alru_cache
from database.database import model_to_dict
from database.models.player import Player as DBPlayer
from sqlalchemy.ext.asyncio import AsyncSession


class PlayerController(DatabaseHandler):
    def __init__(self, session: AsyncSession):
        self.session = session

    def sanitize_name(self, player_name: str) -> str:
        return player_name.lower().replace("_", " ").replace("-", " ").strip()

    @alru_cache(maxsize=2048)
    async def get(self, player_name: str) -> PlayerInDB:
        player_name = self.sanitize_name(player_name)
        sql = sqla.select(DBPlayer).where(DBPlayer.name == player_name)
        result = await self.session.execute(sql)
        data = result.scalars().all()
        return PlayerInDB(**model_to_dict(data[0])) if data else None

    async def insert(self, player: PlayerCreate) -> PlayerInDB:
        player.name = self.sanitize_name(player.name)
        sql = sqla.insert(DBPlayer).values(player.model_dump()).prefix_with("IGNORE")
        await self.session.execute(sql)
        return await self.get(player_name=player.name)

    async def get_or_insert(self, player_name: str) -> PlayerInDB:
        player_name = self.sanitize_name(player_name)
        player = await self.get(player_name=player_name)
        if player is None:
            player = await self.insert(PlayerCreate(name=player_name))
        return player
