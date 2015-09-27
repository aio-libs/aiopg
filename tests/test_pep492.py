"""
A test suite for Python 3.5's new async and await syntax.

"""

import unittest
import asyncio

import aiopg


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        self.loop = None

    async def connect(self, no_loop=False, **kwargs):
        loop = None if no_loop else self.loop
        conn = await aiopg.connect(database='aiopg',
                                   user='aiopg',
                                   password='passwd',
                                   host='127.0.0.1',
                                   loop=loop,
                                   **kwargs)
        self.addCleanup(conn.close)
        return conn

    def test_cursor_async_context_manager(self):

        async def go():
            conn = await self.connect()

            async with conn.cursor() as cur:
                await cur.execute('SELECT 1')
            self.assertTrue(cur.closed)

        self.loop.run_until_complete(go())

    def test_cursor_await(self):

        async def go():
            conn = await self.connect()

            cur = await conn.cursor()
            await cur.execute('SELECT 1')
            cur.close()

        self.loop.run_until_complete(go())
