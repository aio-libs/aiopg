import asyncio
import aiopg
import unittest

from aiopg.connection import Connection


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        self.loop = None

    def connect(self):

        @asyncio.coroutine
        def go():
            conn = yield from aiopg.connect(database='aiopg',
                                            user='aiopg',
                                            password='passwd',
                                            host='127.0.0.1',
                                            loop=self.loop)
            return conn

        return self.loop.run_until_complete(go())

    def test_connect(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertIsInstance(conn, Connection)

        self.loop.run_until_complete(go())

    def test_simple_select(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            curs = yield from conn.cursor('SELECT 1')
            self.assertIsInstance(conn, Connection)

        self.loop.run_until_complete(go())
