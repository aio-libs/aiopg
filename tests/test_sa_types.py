import asyncio
import unittest

import psycopg2
from aiopg import sa

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


class TestSATypes(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        engine = yield from sa.create_engine(database='aiopg',
                                             user='aiopg',
                                             password='passwd',
                                             host='127.0.0.1',
                                             loop=self.loop,
                                             **kwargs)
        with (yield from engine) as conn:
            try:
                yield from conn.execute(DropTable(tbl))
            except psycopg2.ProgrammingError:
                pass
            yield from conn.execute("DROP TYPE IF EXISTS simple_enum;")
            yield from conn.execute("""CREATE TYPE simple_enum AS ENUM
                                       ('first', 'second');""")
            yield from conn.execute(CreateTable(tbl))
        return engine

    def test_json(self):
        @asyncio.coroutine
        def go():
            engine = yield from self.connect()
            data = {'a': 1, 'b': 'name'}
            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values(json_val=data))

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(data, item['json_val'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())

    def test_array(self):
        @asyncio.coroutine
        def go():
            engine = yield from self.connect()
            data = [1, 2, 3]
            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values(array_val=data))

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(data, item['array_val'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())

    def test_hstore(self):
        @asyncio.coroutine
        def go():
            engine = yield from self.connect()
            data = {'a': 'str', 'b': 'name'}
            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values(hstore_val=data))

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(data, item['hstore_val'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())

    def test_enum(self):
        @asyncio.coroutine
        def go():
            engine = yield from self.connect()
            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values(enum_val='second'))

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual('second', item['enum_val'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())
