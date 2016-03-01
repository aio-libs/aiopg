import asyncio
import unittest
from unittest import mock
import pytest
import sys

from psycopg2.extensions import TRANSACTION_STATUS_INTRANS

import aiopg
from aiopg.connection import Connection, TIMEOUT
from aiopg.pool import Pool


@pytest.mark.run_loop
def test_create_pool(create_pool):
    pool = yield from create_pool()
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 10 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize
    assert TIMEOUT == pool.timeout
    assert not pool.echo


@pytest.mark.run_loop
def test_create_pool2(create_pool):
    pool = yield from create_pool(minsize=5, maxsize=20)
    assert isinstance(pool, Pool)
    assert 5 == pool.minsize
    assert 20 == pool.maxsize
    assert 5 == pool.size
    assert 5 == pool.freesize
    assert TIMEOUT == pool.timeout


@pytest.mark.run_loop
def test_acquire(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    assert isinstance(conn, Connection)
    assert not conn.closed
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1')
    val = yield from cur.fetchone()
    assert (1,) == val
    pool.release(conn)


@pytest.mark.run_loop
def test_release(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    pool.release(conn)
    assert 10 == pool.freesize
    assert not pool._used


@pytest.mark.run_loop
def test_release_closed(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    yield from conn.close()
    pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert 9 == pool.size

    conn2 = yield from pool.acquire()
    assert 9 == pool.freesize
    assert 10 == pool.size
    pool.release(conn2)


@pytest.mark.run_loop
def test_bad_context_manager_usage(create_pool):
    pool = yield from create_pool()
    with pytest.raises(RuntimeError):
        with pool:
            pass


@pytest.mark.run_loop
def test_context_manager(create_pool):
    pool = yield from create_pool()
    with (yield from pool) as conn:
        assert isinstance(conn, Connection)
        assert 9 == pool.freesize
        assert {conn} == pool._used
    assert 10 == pool.freesize


@pytest.mark.run_loop
def test_clear(create_pool):
    pool = yield from create_pool()
    yield from pool.clear()
    assert 0 == pool.freesize


@pytest.mark.run_loop
def test_initial_empty(create_pool):
    pool = yield from create_pool(minsize=0)
    assert 10 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    with (yield from pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize

    conn1 = yield from pool.acquire()
    assert 1 == pool.size
    assert 0 == pool.freesize

    conn2 = yield from pool.acquire()
    assert 2 == pool.size
    assert 0 == pool.freesize

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize


@pytest.mark.run_loop
def test_parallel_tasks(create_pool, loop):
    pool = yield from create_pool(minsize=0, maxsize=2)
    assert 2 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    fut1 = pool.acquire()
    fut2 = pool.acquire()

    conn1, conn2 = yield from asyncio.gather(fut1, fut2,
                                             loop=loop)
    assert 2 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2} == pool._used

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize
    assert {conn2} == pool._used

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize
    assert not conn1.closed
    assert not conn2.closed

    conn3 = yield from pool.acquire()
    assert conn3 is conn1
    pool.release(conn3)


@pytest.mark.run_loop
def test_parallel_tasks_more(create_pool, loop):
    pool = yield from create_pool(minsize=0, maxsize=3)

    fut1 = pool.acquire()
    fut2 = pool.acquire()
    fut3 = pool.acquire()

    conn1, conn2, conn3 = yield from asyncio.gather(fut1, fut2, fut3,
                                                    loop=loop)
    assert 3 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2, conn3} == pool._used

    pool.release(conn1)
    assert 3 == pool.size
    assert 1 == pool.freesize
    assert {conn2, conn3} == pool._used

    pool.release(conn2)
    assert 3 == pool.size
    assert 2 == pool.freesize
    assert {conn3} == pool._used
    assert not conn1.closed
    assert not conn2.closed

    pool.release(conn3)
    assert 3 == pool.size
    assert 3 == pool.freesize
    assert not pool._used
    assert not conn1.closed
    assert not conn2.closed
    assert not conn3.closed

    conn4 = yield from pool.acquire()
    assert conn4 is conn1
    pool.release(conn4)


@pytest.mark.run_loop
def test_release_with_invalid_status(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = yield from conn.cursor()
    yield from cur.execute('BEGIN')
    cur.close()

    with mock.patch("aiopg.pool.logger") as m_log:
        pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed
    m_log.warning.assert_called_with(
        "Invalid transaction status on released connection: %d",
        TRANSACTION_STATUS_INTRANS)


@pytest.mark.run_loop
def test_default_event_loop(create_pool, loop):
    asyncio.set_event_loop(loop)

    pool = yield from create_pool(no_loop=True)
    assert pool._loop is loop


@pytest.mark.run_loop
def test_cursor(create_pool):
    pool = yield from create_pool()
    with (yield from pool.cursor()) as cur:
        yield from cur.execute('SELECT 1')
        ret = yield from cur.fetchone()
        assert (1,) == ret
    assert cur.closed


@pytest.mark.run_loop
def test_release_with_invalid_status_wait_release(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = yield from conn.cursor()
    yield from cur.execute('BEGIN')
    cur.close()

    with mock.patch("aiopg.pool.logger") as m_log:
        yield from pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed
    m_log.warning.assert_called_with(
        "Invalid transaction status on released connection: %d",
        TRANSACTION_STATUS_INTRANS)


@pytest.mark.run_loop
def test__fill_free(create_pool, loop):
    pool = yield from create_pool(minsize=1)
    with (yield from pool):
        assert 0 == pool.freesize
        assert 1 == pool.size

        conn = yield from asyncio.wait_for(pool.acquire(),
                                           timeout=0.5,
                                           loop=loop)
        assert 0 == pool.freesize
        assert 2 == pool.size
        pool.release(conn)
        assert 1 == pool.freesize
        assert 2 == pool.size
    assert 2 == pool.freesize
    assert 2 == pool.size


@pytest.mark.run_loop
def test_connect_from_acquire(create_pool):
    pool = yield from create_pool(minsize=0)
    assert 0 == pool.freesize
    assert 0 == pool.size
    with (yield from pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize


class TestPool(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.pool = None

    def tearDown(self):
        if self.pool is not None:
            self.pool.terminate()
            self.loop.run_until_complete(self.pool.wait_closed())
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
        self.pool = pool
        return pool

    def test_create_pool_with_timeout(self):

        @asyncio.coroutine
        def go():
            timeout = 0.1
            pool = yield from self.create_pool(timeout=timeout)
            self.assertEqual(timeout, pool.timeout)
            conn = yield from pool.acquire()
            self.assertEqual(timeout, conn.timeout)
            pool.release(conn)

        self.loop.run_until_complete(go())

    def test_cursor_with_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            pool = yield from self.create_pool()
            with (yield from pool.cursor(timeout=timeout)) as cur:
                self.assertEqual(timeout, cur.timeout)

        self.loop.run_until_complete(go())

    def test_concurrency(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=2, maxsize=4)
            c1 = yield from pool.acquire()
            c2 = yield from pool.acquire()
            self.assertEqual(0, pool.freesize)
            self.assertEqual(2, pool.size)
            pool.release(c1)
            pool.release(c2)

        self.loop.run_until_complete(go())

    def test_invalid_minsize_and_maxsize(self):

        @asyncio.coroutine
        def go():
            with self.assertRaises(ValueError):
                yield from self.create_pool(minsize=-1)

            with self.assertRaises(ValueError):
                yield from self.create_pool(minsize=5, maxsize=2)

        self.loop.run_until_complete(go())

    def test_true_parallel_tasks(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=0, maxsize=1)
            self.assertEqual(1, pool.maxsize)
            self.assertEqual(0, pool.minsize)
            self.assertEqual(0, pool.size)
            self.assertEqual(0, pool.freesize)

            maxsize = 0
            minfreesize = 100

            def inner():
                nonlocal maxsize, minfreesize
                maxsize = max(maxsize, pool.size)
                minfreesize = min(minfreesize, pool.freesize)
                conn = yield from pool.acquire()
                maxsize = max(maxsize, pool.size)
                minfreesize = min(minfreesize, pool.freesize)
                yield from asyncio.sleep(0.01, loop=self.loop)
                pool.release(conn)
                maxsize = max(maxsize, pool.size)
                minfreesize = min(minfreesize, pool.freesize)

            yield from asyncio.gather(inner(), inner(),
                                      loop=self.loop)

            self.assertEqual(1, maxsize)
            self.assertEqual(0, minfreesize)

        self.loop.run_until_complete(go())

    def test_cannot_acquire_after_closing(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            pool.close()

            with self.assertRaises(RuntimeError):
                yield from pool.acquire()

        self.loop.run_until_complete(go())

    def test_wait_closed(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()

            c1 = yield from pool.acquire()
            c2 = yield from pool.acquire()
            self.assertEqual(10, pool.size)
            self.assertEqual(8, pool.freesize)

            ops = []

            @asyncio.coroutine
            def do_release(conn):
                yield from asyncio.sleep(0, loop=self.loop)
                pool.release(conn)
                ops.append('release')

            @asyncio.coroutine
            def wait_closed():
                yield from pool.wait_closed()
                ops.append('wait_closed')

            pool.close()
            yield from asyncio.gather(wait_closed(),
                                      do_release(c1),
                                      do_release(c2),
                                      loop=self.loop)
            self.assertEqual(['release', 'release', 'wait_closed'], ops)
            self.assertEqual(0, pool.freesize)

        self.loop.run_until_complete(go())

    def test_echo(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(echo=True)
            self.assertTrue(pool.echo)

            with (yield from pool) as conn:
                self.assertTrue(conn.echo)

        self.loop.run_until_complete(go())

    def test_terminate_with_acquired_connections(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            pool.terminate()
            yield from pool.wait_closed()

            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_release_closed_connection(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            conn.close()

            pool.release(conn)

        self.loop.run_until_complete(go())

    def test_wait_closing_on_not_closed(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()

            with self.assertRaises(RuntimeError):
                yield from pool.wait_closed()

        self.loop.run_until_complete(go())

    def test_release_terminated_pool(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            pool.terminate()
            yield from pool.wait_closed()

            pool.release(conn)

        self.loop.run_until_complete(go())

    def test_release_terminated_pool_with_wait_release(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            conn = yield from pool.acquire()
            pool.terminate()
            yield from pool.wait_closed()

            yield from pool.release(conn)

        self.loop.run_until_complete(go())

    def test_close_with_acquired_connections(self):

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            yield from pool.acquire()
            pool.close()

            with self.assertRaises(asyncio.TimeoutError):
                yield from asyncio.wait_for(pool.wait_closed(),
                                            0.1, loop=self.loop)

        self.loop.run_until_complete(go())

    @unittest.skipIf(sys.version_info < (3, 4),
                     "Python 3.3 doesnt support __del__ calls from GC")
    def test___del__(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool()
            self.pool = None  # drop reference
            with self.assertWarns(ResourceWarning):
                del pool

        self.loop.run_until_complete(go())

    def test_unlimited_size(self):
        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(maxsize=0)
            self.assertEqual(10, pool.minsize)
            self.assertIsNone(pool._free.maxlen)

        self.loop.run_until_complete(go())

    def test_connection_in_good_state_after_timeout(self):
        @asyncio.coroutine
        def sleep(conn):
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT pg_sleep(10);')

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=1,
                                               maxsize=1,
                                               timeout=0.1)
            with (yield from pool) as conn:
                with self.assertRaises(asyncio.TimeoutError):
                    yield from sleep(conn)

            self.assertEqual(1, pool.freesize)
            with (yield from pool) as conn:
                cur = yield from conn.cursor()
                yield from cur.execute('SELECT 1;')
                val = yield from cur.fetchone()
                self.assertEqual((1,), val)

        self.loop.run_until_complete(go())

    def test_connection_in_good_state_after_timeout_inside_transaction(self):
        @asyncio.coroutine
        def sleep(conn):
            cur = yield from conn.cursor()
            yield from cur.execute('BEGIN;')
            yield from cur.execute('SELECT pg_sleep(10);')

        @asyncio.coroutine
        def go():
            pool = yield from self.create_pool(minsize=1,
                                               maxsize=1,
                                               timeout=0.1)
            with (yield from pool) as conn:
                with self.assertRaises(asyncio.TimeoutError):
                    yield from sleep(conn)

            self.assertEqual(0, pool.freesize)
            self.assertEqual(0, pool.size)
            with (yield from pool) as conn:
                cur = yield from conn.cursor()
                yield from cur.execute('SELECT 1;')
                val = yield from cur.fetchone()
                self.assertEqual((1,), val)

        self.loop.run_until_complete(go())
