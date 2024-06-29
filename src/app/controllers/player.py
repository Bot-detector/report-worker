import logging

import sqlalchemy as sqla
from _cache import SimpleALRUCache
from app.controllers.db_handler import DatabaseHandler
from app.views.player import PlayerCreate, PlayerInDB
from database.database import model_to_dict
from database.models.player import Player as DBPlayer
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PlayerController(DatabaseHandler):
    def __init__(
        self,
        session: AsyncSession = None,
        cache: SimpleALRUCache = SimpleALRUCache(),
    ):
        self.session = session
        self.cache = cache

    def sanitize_name(self, player_name: str) -> str:
        return player_name.lower().replace("_", " ").replace("-", " ").strip()

    async def update_session(self, session: AsyncSession):
        self.session = session

    async def get(self, player_name: str) -> PlayerInDB:
        player_name = self.sanitize_name(player_name)

        sql = sqla.select(DBPlayer).where(DBPlayer.name == player_name)
        result = await self.session.execute(sql)
        data = result.scalars().all()

        return PlayerInDB(**model_to_dict(data[0])) if data else None

    async def get_cache(self, player_name: str) -> PlayerInDB:
        player_name = self.sanitize_name(player_name)
        player = await self.cache.get(key=player_name)

        if isinstance(player, PlayerInDB):
            if self.cache.hits % 100 == 0 and self.cache.hits > 0:
                logger.info(f"hits: {self.cache.hits}, misses: {self.cache.misses}")
            return player

        player = await self.get(player_name=player_name)

        if isinstance(player, PlayerInDB):
            await self.cache.put(key=player_name, value=player)

        return player

    async def insert(self, player: PlayerCreate) -> PlayerInDB:
        player.name = self.sanitize_name(player.name)
        sql = sqla.insert(DBPlayer).values(player.model_dump()).prefix_with("IGNORE")
        await self.session.execute(sql)
        return await self.get(player_name=player.name)

    async def get_or_insert(self, player_name: str, cached=True) -> PlayerInDB:
        player_name = self.sanitize_name(player_name)

        if cached:
            player = await self.get_cache(player_name=player_name)
        else:
            player = await self.get(player_name=player_name)

        if player is None:
            player = await self.insert(PlayerCreate(name=player_name))

        return player
