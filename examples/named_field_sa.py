import asyncio
import datetime

import sqlalchemy as sa

from aiopg.sa import create_engine

metadata = sa.MetaData()

now = datetime.datetime.now

tbl = sa.Table(
    'tbl', metadata,
    sa.Column('MyIDField', sa.Integer, key='id', primary_key=True),
    sa.Column('NaMe', sa.String(255), key='name', default='default name'),
)


async def insert_tbl(conn, **kwargs):
    await conn.execute(tbl.insert().values(**kwargs))
    row = await (await conn.execute(tbl.select())).first()

    for name, val in kwargs.items():
        assert row[name] == val

    await conn.execute(sa.delete(tbl))


async def create_table(conn):
    await conn.execute('DROP TABLE IF EXISTS tbl')
    await conn.execute(
        'CREATE TABLE tbl ('
        '"MyIDField" INTEGER NOT NULL, '
        '"NaMe" VARCHAR(255), '
        'PRIMARY KEY ("MyIDField"))'
    )


async def go():
    async with create_engine(user='aiopg',
                             database='aiopg',
                             host='127.0.0.1',
                             password='passwd') as engine:
        async with engine.acquire() as conn:
            await create_table(conn)
            await insert_tbl(conn, id=1)
            await insert_tbl(conn, id=2, name='test')


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
