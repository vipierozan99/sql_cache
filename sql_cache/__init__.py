import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.session import sessionmaker


async def query_to_df(query: sa.Selectable, session: AsyncSession):
    return await session.run_sync(
        lambda sync_sesssion, q: pd.read_sql_query(q, sync_sesssion.connection()), query
    )


def get_selected_tables(query: sa.Select) -> set[sa.Table]:
    return {c.table for c in query.selected_columns.values()}


class SQLCache:
    def __init__(self):
        self.conn = sa.create_engine("duckdb:///:memory:").connect()
        self.sessionmaker = sessionmaker(
            bind=self.conn,
        )
        self._df_cache: dict[str, pd.DataFrame] = {}

    async def query_db(self, query: sa.Select, session: AsyncSession):
        tables = get_selected_tables(query)
        assert len(tables) == 1, "Only single table queries are supported"
        df = await query_to_df(query, session)
        table = tables.pop()
        with self.begin() as cache_session:
            cache_session.execute(
                sa.text("register(:name, :df)"), {"name": table.name, "df": df}
            )

        self._df_cache[table.name] = df
        return df

    def __getitem__(self, table_name: str) -> pd.DataFrame:
        return self._df_cache[table_name]

    def begin(self):
        return self.sessionmaker.begin()
