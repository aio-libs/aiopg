import asyncio

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, ENUM, JSON
from sqlalchemy.sql.ddl import CreateTable

from aiopg.sa import create_engine

metadata = sa.MetaData()


class CustomStrList(sa.types.TypeDecorator):
    impl = sa.types.String

    def __init__(self, sep=',', *args, **kwargs):
        self._sep = sep
        self._args = args
        self._kwargs = kwargs
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        return ('{sep}'.format(sep=self._sep)).join(map(str, value))

    def process_result_value(self, value, dialect):
        if value is None:
            return value

        return value.split(self._sep)

    def copy(self):
        return CustomStrList(self._sep, *self._args, **self._kwargs)


tbl = sa.Table(
    'tbl', metadata,
    sa.Column('id', sa.Integer, autoincrement=True, primary_key=True),
    sa.Column('json', JSON, default=None),
    sa.Column('array_int', ARRAY(sa.Integer), default=list),
    sa.Column('enum', ENUM('f', 's', name='s_enum'), default='s'),
    sa.Column('custom_list', CustomStrList(), default=list),
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
    await conn.execute('DROP TYPE IF EXISTS s_enum CASCADE')
    await conn.execute("CREATE TYPE s_enum AS ENUM ('f', 's')")
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
            await insert_tbl(conn, 2, json={'data': 123})
            await insert_tbl(conn, 3, array_int=[1, 3, 4])
            await insert_tbl(conn, 4, enum='f')
            await insert_tbl(conn, 5, custom_list=['1', 'test', '4'])


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
