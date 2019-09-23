import asyncio
import datetime
import uuid

import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable

from aiopg.sa import create_engine

metadata = sa.MetaData()

now = datetime.datetime.now

tbl = sa.Table(
    'tbl', metadata,
    sa.Column('id', sa.Integer, autoincrement=True, primary_key=True),
    sa.Column('uuid', sa.String, default=lambda: str(uuid.uuid4())),
    sa.Column('name', sa.String(255), default='default name'),
    sa.Column('date', sa.DateTime, default=datetime.datetime.now),
    sa.Column('flag', sa.Integer, default=0),
    sa.Column('count_str', sa.Integer, default=sa.func.length('default')),
    sa.Column('is_active', sa.Boolean, default=True),
)


async def insert_tbl(conn, pk, **kwargs):
    await conn.execute(tbl.insert().values(**kwargs))
    row = await (await conn.execute(tbl.select())).first()

    assert row.id == pk

    for name, val in kwargs.items():
        assert row[name] == val

    await conn.execute(sa.delete(tbl))


async def create_table(conn):
    await conn.execute('DROP TABLE IF EXISTS tbl')
    await conn.execute(CreateTable(tbl))


async def go():
    async with create_engine(user='aiopg',
                             database='aiopg',
                             host='127.0.0.1',
                             password='passwd') as engine:
        async with engine.acquire() as conn:
            await create_table(conn)
        async with engine.acquire() as conn:
            await insert_tbl(conn, 1)
            await insert_tbl(conn, 2, name='test', is_active=False, date=now())


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
