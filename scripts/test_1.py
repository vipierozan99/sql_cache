import asyncio

import pandas as pd
import sqlalchemy as sa
from rich import print  # noqa: F401
from utils.database import AsyncSession, get_db
from utils.models import MyRecord, MyUser

from sql_cache import SQLCache


async def populate_db(session: AsyncSession):
    orms = []
    for i in range(3):
        u = MyUser(name=f"user {i}")
        orms.append(u)
        for j in range(12):
            orms.append(MyRecord(user_id=u.id, data=f"record {j}"))

    session.add_all(orms)


async def main():
    DB = get_db()
    await DB.drop_tables()
    await DB.create_tables()

    cached_db = SQLCache()

    async with DB.begin() as session:
        await populate_db(session)
        await session.flush()

        q = sa.select(MyUser)
        await cached_db.query_db(q, session)

        q = (
            sa.select(MyRecord)
            .join(MyUser)
            .where(sa.or_(MyUser.name == "user 1", MyUser.name == "user 2"))
        )
        db_df = await cached_db.query_db(q, session)

    q = q = sa.select(MyRecord, MyUser.name).join(MyUser).where(MyUser.name == "user 2")
    with cached_db.begin() as session:
        cache_df = pd.read_sql_query(q, session.connection())

    print(db_df)
    print(cache_df)

    print(db_df["user_id"].unique())
    print(cache_df["user_id"].unique())


if __name__ == "__main__":
    asyncio.run(main())
