import asyncio
import unittest
from unittest import mock

from psycopg2.extensions import TRANSACTION_STATUS_INTRANS

import aiopg
from aiopg.connection import Connection
from aiopg.pool import Pool


class TestPool(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def create_pool(self, no_loop=False, **kwargs):
        loop = None if no_loop else self.loop
        pool = yield from aiopg.create_pool(database='aiopg',
                                            user='aiopg',
                                            password='passwd',
                                            host='127.0.0.1',
                                            loop=loop,
                                            **kwargs)
        return pool

    def test_create_pool(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            self.assertIsInstance(pool, Pool)
            self.assertEqual(10, pool.minsize)
            self.assertEqual(10, pool.maxsize)
            self.assertEqual(10, pool.size)
            self.assertEqual(10, pool.freesize)

        self.loop.run_until_complete(go())

    def test_acquire(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertIsInstance(conn, Connection)
            self.assertFalse(conn.closed)
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            val = yield from cur.fetchone()
            self.assertEqual((1,), val)

        self.loop.run_until_complete(go())

    def test_release(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual({conn}, pool._used)
            pool.release(conn)
            self.assertEqual(10, pool.freesize)
            self.assertFalse(pool._used)

        self.loop.run_until_complete(go())

    def test_release_closed(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            yield from conn.close()
            pool.release(conn)
            self.assertEqual(9, pool.freesize)
            self.assertFalse(pool._used)
            self.assertEqual(9, pool.size)

            yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual(10, pool.size)

        self.loop.run_until_complete(go())

    def test_bad_context_manager_usage(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with self.assertRaises(RuntimeError):
                with pool:
                    pass

        self.loop.run_until_complete(go())

    def test_context_manager(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with (yield from pool) as conn:
                self.assertIsInstance(conn, Connection)
                self.assertEqual(9, pool.freesize)
                self.assertEqual({conn}, pool._used)
            self.assertEqual(10, pool.freesize)

        self.loop.run_until_complete(go())

    def test_clear(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            yield from pool.clear()
            self.assertEqual(0, pool.freesize)

        self.loop.run_until_complete(go())

    def test_initial_empty(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0)
            self.assertEqual(10, pool.maxsize)
            self.assertEqual(0, pool.minsize)
            self.assertEqual(0, pool.size)
            self.assertEqual(0, pool.freesize)

            with (yield from pool):
                self.assertEqual(1, pool.size)
                self.assertEqual(0, pool.freesize)
            self.assertEqual(1, pool.size)
            self.assertEqual(1, pool.freesize)

            conn1 = yield from pool.acquire()
            self.assertEqual(1, pool.size)
            self.assertEqual(0, pool.freesize)

            conn2 = yield from pool.acquire()
            self.assertEqual(2, pool.size)
            self.assertEqual(0, pool.freesize)

            pool.release(conn1)
            self.assertEqual(2, pool.size)
            self.assertEqual(1, pool.freesize)

            pool.release(conn2)
            self.assertEqual(2, pool.size)
            self.assertEqual(2, pool.freesize)

        self.loop.run_until_complete(go())

    def test_parallel_tasks(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=1)
            self.assertEqual(1, pool.maxsize)
            self.assertEqual(0, pool.minsize)
            self.assertEqual(0, pool.size)
            self.assertEqual(0, pool.freesize)

            fut1 = pool.acquire()
            fut2 = pool.acquire()

            conn1, conn2 = yield from asyncio.gather(fut1, fut2,
                                                     loop=self.loop)
            self.assertEqual(2, pool.size)
            self.assertEqual(0, pool.freesize)
            self.assertEqual({conn1, conn2}, pool._used)

            pool.release(conn1)
            self.assertEqual(2, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertEqual({conn2}, pool._used)

            pool.release(conn2)
            self.assertEqual(1, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertTrue(conn1.closed)
            self.assertFalse(conn2.closed)

            conn3 = yield from pool.acquire()
            self.assertIs(conn3, conn2)

        self.loop.run_until_complete(go())

    def test_parallel_tasks_more(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=1)

            fut1 = pool.acquire()
            fut2 = pool.acquire()
            fut3 = pool.acquire()

            conn1, conn2, conn3 = yield from asyncio.gather(fut1, fut2, fut3,
                                                            loop=self.loop)
            self.assertEqual(3, pool.size)
            self.assertEqual(0, pool.freesize)
            self.assertEqual({conn1, conn2, conn3}, pool._used)

            pool.release(conn1)
            self.assertEqual(3, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertEqual({conn2, conn3}, pool._used)

            pool.release(conn2)
            self.assertEqual(2, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertEqual({conn3}, pool._used)
            self.assertTrue(conn1.closed)
            self.assertFalse(conn2.closed)

            pool.release(conn3)
            self.assertEqual(1, pool.size)
            self.assertEqual(1, pool.freesize)
            self.assertFalse(pool._used)
            self.assertTrue(conn1.closed)
            self.assertTrue(conn2.closed)
            self.assertFalse(conn3.closed)

            conn4 = yield from pool.acquire()
            self.assertIs(conn4, conn3)

        self.loop.run_until_complete(go())

    def test_default_event_loop(self):
        asyncio.set_event_loop(self.loop)

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(no_loop=True)
            self.assertIs(pool._loop, self.loop)

        self.loop.run_until_complete(go())

    def test_cursor(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            with (yield from pool.cursor()) as cur:
                yield from cur.execute('SELECT 1')
                ret = yield from cur.fetchone()
                self.assertEqual((1,), ret)
            self.assertTrue(cur.closed)

        self.loop.run_until_complete(go())

    @mock.patch("aiopg.pool.logger")
    def test_release_with_invalid_status(self, m_log):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            self.assertEqual(9, pool.freesize)
            self.assertEqual({conn}, pool._used)
            cur = yield from conn.cursor()
            yield from cur.execute('BEGIN')
            cur.close()

            pool.release(conn)
            self.assertEqual(9, pool.freesize)
            self.assertFalse(pool._used)
            self.assertTrue(conn.closed)
            m_log.warning.assert_called_with(
                "Invalid transaction status on released connection: %d",
                TRANSACTION_STATUS_INTRANS)

        self.loop.run_until_complete(go())

    def test__fill_free(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=1)
            with (yield from pool):
                self.assertEqual(0, pool.freesize)
                self.assertEqual(1, pool.size)

                conn = yield from asyncio.wait_for(pool.acquire(),
                                                   timeout=0.5,
                                                   loop=self.loop)
                self.assertEqual(0, pool.freesize)
                self.assertEqual(2, pool.size)
                pool.release(conn)
                self.assertEqual(1, pool.freesize)
                self.assertEqual(2, pool.size)
            self.assertEqual(2, pool.freesize)
            self.assertEqual(2, pool.size)

        self.loop.run_until_complete(go())

    def test_connect_from_acquire(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0)
            self.assertEqual(0, pool.freesize)
            self.assertEqual(0, pool.size)
            with (yield from pool):
                self.assertEqual(1, pool.size)
                self.assertEqual(0, pool.freesize)
            self.assertEqual(1, pool.size)
            self.assertEqual(1, pool.freesize)
        self.loop.run_until_complete(go())
