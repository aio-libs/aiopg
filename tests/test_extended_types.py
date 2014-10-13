import asyncio

import unittest

from aiopg import connect
from psycopg2.extras import Json


class TestComplexPGTypesConnection(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from connect(database='aiopg',
                                  user='aiopg',
                                  password='passwd',
                                  host='127.0.0.1',
                                  loop=self.loop,
                                  **kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("""CREATE TABLE tbl (
                                      id SERIAL,
                                      val JSON)""")
        cur.close()
        self.addCleanup(conn.close)
        return conn

    def test_simple(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            data = {'a': 1, 'b': 'str'}
            cur = yield from conn.cursor()
            try:
                yield from cur.execute("INSERT INTO tbl (val) VALUES (%s)",
                                       [Json(data)])
                yield from cur.execute("SELECT * FROM tbl")
                item = yield from cur.fetchone()
                self.assertEqual((1, {'b': 'str', 'a': 1}), item)
            finally:
                cur.close()
                yield from conn.close()

        self.loop.run_until_complete(go())
