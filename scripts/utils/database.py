from functools import lru_cache
from typing import Type

import sqlalchemy as sa
from sqlalchemy import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio.session import (
    _AsyncSessionContextManager as AsyncSessionContextManager,
)
from sqlalchemy.orm import DeclarativeBase

from .models import SQLBase


class DBManager:
    def __init__(
        self, model_base: Type[DeclarativeBase], db_url: str | URL, **kwargs
    ) -> None:
        self.model_base = model_base
        if not isinstance(db_url, URL):
            db_url = sa.make_url(db_url)

        # https://stackoverflow.com/a/68005839/10891464
        if db_url.get_dialect() == postgresql.dialect:
            db_url = db_url.update_query_dict({"prepared_statement_cache_size": "0"})

        isolation_level = kwargs.get("isolation_level", None)
        if isolation_level is not None:
            kwargs["isolation_level"] = "REPEATABLE READ"

        self.db_url = db_url
        self.engine = create_async_engine(db_url, **kwargs)
        self.sessionmaker = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autobegin=False,
        )

    async def drop_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.model_base.metadata.drop_all)

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.model_base.metadata.create_all)

    def get_session(self):
        """Returns a session"""
        return self.sessionmaker()

    def begin(self) -> AsyncSessionContextManager[AsyncSession]:
        """Returns a AsyncSession with a transaction started. Commits and closes"""
        return self.sessionmaker.begin()


@lru_cache
def get_db():
    db = DBManager(model_base=SQLBase, db_url="sqlite+aiosqlite:///:memory:")
    return db
