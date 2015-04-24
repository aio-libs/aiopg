import asyncio
import unittest

import psycopg2
from aiopg import sa
from aiopg.sa import utils
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable, DropTable


def get_default():
    return 2


def onupdate_default():
    return 3

meta = MetaData()
tbl = Table('sa_tbl_defaults', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('default_value', Integer, nullable=False, default=1),
            Column('default_callable', Integer, nullable=False,
                   default=get_default, onupdate=onupdate_default)
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

    def test_insert_defaults_simple(self):
        @asyncio.coroutine
        def go():
            engine = yield from self.connect()

            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values())

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(1, item['default_value'])
                self.assertEqual(2, item['default_callable'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())

    def test_defaults_factory(self):

        @asyncio.coroutine
        def go():
            engine = yield from self.connect()

            with (yield from engine) as conn:
                insert = yield from utils.insert(conn, engine.dialect, tbl)
                yield from conn.execute(insert)

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                item_id = item['id']
                self.assertEqual(1, item['default_value'])
                self.assertEqual(2, item['default_callable'])

                update = yield from utils.update(conn, engine.dialect, tbl)
                stmt = update.where(tbl.c.id == item_id). \
                    values(default_value=4)
                yield from conn.execute(stmt)

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(1, item['id'] == item_id)
                self.assertEqual(4, item['default_value'])
                self.assertEqual(3, item['default_callable'])

            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())
