from core.config import settings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create an async SQLAlchemy engine
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_timeout=settings.POOL_TIMEOUT,
    pool_recycle=settings.POOL_RECYCLE,
    echo=(settings.ENV != "PRD"),
)

# Create a session factory
SessionFactory = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,  # Use AsyncSession for asynchronous operations
    autocommit=False,
    autoflush=False,
)


# async def get_session() -> AsyncSession:
#     async with SessionFactory() as session:
#         yield session
async def get_session() -> AsyncSession:
    return SessionFactory()


def model_to_dict(model):
    """Converts an SQLAlchemy model instance to a dictionary."""
    return {c.name: getattr(model, c.name) for c in model.__table__.columns}


Base = declarative_base()
