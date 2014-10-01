import asyncio
import unittest

import psycopg2
from aiopg import sa

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.dialects.postgresql import ARRAY, JSON


meta = MetaData()
tbl = Table('sa_tbl_types', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('json_val', JSON),
            Column('array_val', ARRAY(Integer)))


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
        self.loop.run_until_complete(go())
