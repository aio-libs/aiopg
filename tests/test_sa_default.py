import asyncio
import datetime

import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable

meta = sa.MetaData()
tbl = sa.Table('sa_tbl4', meta,
               sa.Column('id', sa.Integer, nullable=False, primary_key=True),
               sa.Column('name', sa.String(255), nullable=False,
                         default='default test'),
               sa.Column('count', sa.Integer, default=100),
               sa.Column('date', sa.DateTime, default=datetime.datetime.now),
               sa.Column('is_active', sa.Boolean, default=True))


@asyncio.coroutine
def test_default_fields(make_engine, loop):
    engine = yield from make_engine()
    with (yield from engine) as conn:
        yield from conn.execute('DROP TABLE IF EXISTS sa_tbl4')
        yield from conn.execute(CreateTable(tbl))

        yield from conn.execute(sa.insert(tbl).values())

        res = yield from conn.execute(sa.select([tbl]))
        row = yield from res.fetchone()
        assert row.count == 100
        assert row.name == 'default test'
        assert row.is_active is True
        assert type(row.date) == datetime.datetime


@asyncio.coroutine
def test_default_fields_edit(make_engine, loop):
    engine = yield from make_engine()
    with (yield from engine) as conn:
        yield from conn.execute('DROP TABLE IF EXISTS sa_tbl4')
        yield from conn.execute(CreateTable(tbl))

        date = datetime.datetime.now()
        yield from conn.execute(sa.insert(tbl).values(
            name='edit name',
            is_active=False,
            date=date,
            count=1,
        ))

        res = yield from conn.execute(sa.select([tbl]))
        row = yield from res.fetchone()
        assert row.count == 1
        assert row.name == 'edit name'
        assert row.is_active is False
        assert row.date == date
