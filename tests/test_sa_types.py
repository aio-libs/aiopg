import asyncio

import psycopg2

import pytest
sa = pytest.importorskip("aiopg.sa")  # noqa

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.dialects.postgresql import ARRAY, JSON, HSTORE, ENUM


meta = MetaData()

tbl = Table('sa_tbl_types', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('json_val', JSON),
            Column('array_val', ARRAY(Integer)),
            Column('hstore_val', HSTORE),
            Column('enum_val', ENUM('first', 'second', name='simple_enum')))

tbl2 = Table('sa_tbl_types2', meta,
             Column('id', Integer, nullable=False,
                    primary_key=True),
             Column('json_val', JSON),
             Column('array_val', ARRAY(Integer)),
             Column('enum_val', ENUM('first', 'second', name='simple_enum')))


@pytest.yield_fixture
def connect(make_engine):
    @asyncio.coroutine
    def go(**kwargs):
        engine = yield from make_engine(**kwargs)
        with (yield from engine) as conn:
            try:
                yield from conn.execute(DropTable(tbl))
            except psycopg2.ProgrammingError:
                pass
            try:
                yield from conn.execute(DropTable(tbl2))
            except psycopg2.ProgrammingError:
                pass
            yield from conn.execute("DROP TYPE IF EXISTS simple_enum CASCADE;")
            yield from conn.execute("""CREATE TYPE simple_enum AS ENUM
                                       ('first', 'second');""")
            try:
                yield from conn.execute(CreateTable(tbl))
                ret_tbl = tbl
                has_hstore = True
            except psycopg2.ProgrammingError:
                yield from conn.execute(CreateTable(tbl2))
                ret_tbl = tbl2
                has_hstore = False
        return engine, ret_tbl, has_hstore

    yield go


@asyncio.coroutine
def test_json(connect):
    engine, tbl, has_hstore = yield from connect()
    data = {'a': 1, 'b': 'name'}
    with (yield from engine) as conn:
        yield from conn.execute(
            tbl.insert().values(json_val=data))

        ret = yield from conn.execute(tbl.select())
        item = yield from ret.fetchone()
        assert data == item['json_val']


@asyncio.coroutine
def test_array(connect):
    engine, tbl, has_hstore = yield from connect()
    data = [1, 2, 3]
    with (yield from engine) as conn:
        yield from conn.execute(
            tbl.insert().values(array_val=data))

        ret = yield from conn.execute(tbl.select())
        item = yield from ret.fetchone()
        assert data == item['array_val']


@asyncio.coroutine
def test_hstore(connect):
    engine, tbl, has_hstore = yield from connect()
    if not has_hstore:
        raise pytest.skip("hstore is not supported")
    data = {'a': 'str', 'b': 'name'}
    with (yield from engine) as conn:
        yield from conn.execute(
            tbl.insert().values(hstore_val=data))

        ret = yield from conn.execute(tbl.select())
        item = yield from ret.fetchone()
        assert data == item['hstore_val']


@asyncio.coroutine
def test_enum(connect):
    engine, tbl, has_hstore = yield from connect()
    with (yield from engine) as conn:
        yield from conn.execute(
            tbl.insert().values(enum_val='second'))

        ret = yield from conn.execute(tbl.select())
        item = yield from ret.fetchone()
        assert 'second' == item['enum_val']
