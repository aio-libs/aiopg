import asyncio
import unittest

from aiopg import sa
from sqlalchemy import MetaData, Table, Column, Integer, String


meta = MetaData()
tbl = Table('tbl', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(255)))


class TestSA(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from sa.connect(database='aiopg',
                                     user='aiopg',
                                     password='passwd',
                                     host='127.0.0.1',
                                     loop=self.loop,
                                     **kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("CREATE TABLE tbl "
                               "(id serial, name varchar(255))")
        cur.close()
        return conn

    @asyncio.coroutine
    def create_pool(self, **kwargs):
        pool = yield from sa.create_pool(database='aiopg',
                                         user='aiopg',
                                         password='passwd',
                                         host='127.0.0.1',
                                         loop=self.loop,
                                         **kwargs)
        with (yield from pool.cursor()) as cur:
            yield from cur.execute("DROP TABLE IF EXISTS tbl")
            yield from cur.execute("CREATE TABLE tbl "
                                   "(id serial, name varchar(255))")
        return pool

    def test_simple_connection(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute(tbl.insert().values(id=1, name='a'))
            yield from cur.execute(tbl.select().where(tbl.c.name == 'a'))
            row = yield from cur.fetchone()
            self.assertEqual((1, 'a'), row)

        self.loop.run_until_complete(go())

    def test_simple_pool(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with (yield from pool.cursor()) as cur:
                yield from cur.execute(tbl.insert().values(id=1, name='a'))
                yield from cur.execute(tbl.select().where(tbl.c.name == 'a'))
                row = yield from cur.fetchone()
                self.assertEqual((1, 'a'), row)

        self.loop.run_until_complete(go())

    def test_dialect(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertIs(sa.dialect, conn.dialect)
            self.assertIs(sa.dialect, cur.dialect)

        self.loop.run_until_complete(go())

    def test_sclar(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with (yield from pool.cursor()) as cur:
                ret = yield from cur.scalar(tbl.count())
                self.assertEqual(0, ret)

                yield from cur.execute(tbl.insert().values(id=1, name='a'))
                ret = yield from cur.scalar(tbl.count())
                self.assertEqual(1, ret)

        self.loop.run_until_complete(go())
