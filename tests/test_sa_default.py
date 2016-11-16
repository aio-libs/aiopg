import asyncio
import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable

meta = sa.MetaData()
tbl = sa.Table('sa_tbl4', meta,
               sa.Column('id', sa.Integer, nullable=False, primary_key=True),
               sa.Column('id_sequence', sa.Integer, nullable=False,
                         default=sa.Sequence('id_sequence_seq')),
               sa.Column('name', sa.String(255), nullable=False,
                         default='default test'),
               sa.Column('count', sa.Integer, default=100, nullable=None),
               sa.Column('date', sa.DateTime, default=datetime.datetime.now),
               sa.Column('count_str', sa.Integer,
                         default=sa.func.length('abcdef')),
               sa.Column('is_active', sa.Boolean, default=True))


@pytest.fixture
def engine(make_engine, loop):
    @asyncio.coroutine
    def start():
        engine = yield from make_engine()
        with (yield from engine) as conn:
            yield from conn.execute('DROP TABLE IF EXISTS sa_tbl4')
            yield from conn.execute('DROP SEQUENCE IF EXISTS id_sequence_seq')
            yield from conn.execute(CreateTable(tbl))
            yield from conn.execute('CREATE SEQUENCE id_sequence_seq START 1')

        return engine

    return loop.run_until_complete(start())


@asyncio.coroutine
def test_default_fields(engine):
    with (yield from engine) as conn:
        yield from conn.execute(sa.insert(tbl).values())

        res = yield from conn.execute(sa.select([tbl]))
        row = yield from res.fetchone()
        assert row.count == 100
        assert row.id_sequence == 1
        assert row.count_str == 6
        assert row.name == 'default test'
        assert row.is_active is True
        assert type(row.date) == datetime.datetime


@asyncio.coroutine
def test_default_fields_isnull(engine):
    with (yield from engine) as conn:
        yield from conn.execute(sa.insert(tbl).values(
            is_active=False,
            date=None,
        ))

        res = yield from conn.execute(sa.select([tbl]))
        row = yield from res.fetchone()
        assert row.count == 100
        assert row.id_sequence == 1
        assert row.count_str == 6
        assert row.name == 'default test'
        assert row.is_active is False
        assert row.date is None


@asyncio.coroutine
def test_default_fields_edit(engine):
    with (yield from engine) as conn:
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
        assert row.id_sequence == 1
        assert row.count_str == 6
        assert row.name == 'edit name'
        assert row.is_active is False
        assert row.date == date
