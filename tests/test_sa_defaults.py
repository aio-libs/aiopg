import asyncio
import unittest

import psycopg2
from aiopg import sa

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable, DropTable


meta = MetaData()
tbl = Table('sa_tbl_defaults', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('default_value', Integer, nullable=False, default=1)
            )


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

            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values())

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(1, item['default_value'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())
