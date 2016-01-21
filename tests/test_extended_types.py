import asyncio
import uuid

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
        self.addCleanup(conn.close)
        return conn

    def test_uuid(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            _id = uuid.uuid1()
            cur = yield from conn.cursor()
            try:
                yield from cur.execute("DROP TABLE IF EXISTS tbl")
                yield from cur.execute("""CREATE TABLE tbl (id UUID)""")
                yield from cur.execute(
                    "INSERT INTO tbl (id) VALUES (%s)", [_id])
                yield from cur.execute("SELECT * FROM tbl")
                item = yield from cur.fetchone()
                self.assertEqual((_id,), item)
            finally:
                cur.close()

        self.loop.run_until_complete(go())

    def test_json(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            data = {'a': 1, 'b': 'str'}
            cur = yield from conn.cursor()
            try:
                yield from cur.execute("DROP TABLE IF EXISTS tbl")
                yield from cur.execute("""CREATE TABLE tbl (
                                      id SERIAL,
                                      val JSON)""")
                yield from cur.execute(
                    "INSERT INTO tbl (val) VALUES (%s)", [Json(data)])
                yield from cur.execute("SELECT * FROM tbl")
                item = yield from cur.fetchone()
                self.assertEqual((1, {'b': 'str', 'a': 1}), item)
            finally:
                cur.close()

        self.loop.run_until_complete(go())
