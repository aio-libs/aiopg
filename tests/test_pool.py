import asyncio
from unittest import mock
import pytest
import sys

from psycopg2.extensions import TRANSACTION_STATUS_INTRANS

import aiopg
from aiopg.connection import Connection, TIMEOUT
from aiopg.pool import Pool


@asyncio.coroutine
def test_create_pool(create_pool):
    pool = yield from create_pool()
    assert isinstance(pool, Pool)
    assert 1 == pool.minsize
    assert 10 == pool.maxsize
    assert 1 == pool.size
    assert 1 == pool.freesize
    assert TIMEOUT == pool.timeout
    assert not pool.echo


@asyncio.coroutine
def test_create_pool2(create_pool):
    pool = yield from create_pool(minsize=5, maxsize=20)
    assert isinstance(pool, Pool)
    assert 5 == pool.minsize
    assert 20 == pool.maxsize
    assert 5 == pool.size
    assert 5 == pool.freesize
    assert TIMEOUT == pool.timeout


@asyncio.coroutine
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


@asyncio.coroutine
def test_release(create_pool):
    pool = yield from create_pool(minsize=10)
    conn = yield from pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    pool.release(conn)
    assert 10 == pool.freesize
    assert not pool._used


@asyncio.coroutine
def test_release_closed(create_pool):
    pool = yield from create_pool(minsize=10)
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


@asyncio.coroutine
def test_bad_context_manager_usage(create_pool):
    pool = yield from create_pool()
    with pytest.raises(RuntimeError):
        with pool:
            pass


@asyncio.coroutine
def test_context_manager(create_pool):
    pool = yield from create_pool(minsize=10)
    with (yield from pool) as conn:
        assert isinstance(conn, Connection)
        assert 9 == pool.freesize
        assert {conn} == pool._used
    assert 10 == pool.freesize


@asyncio.coroutine
def test_clear(create_pool):
    pool = yield from create_pool()
    yield from pool.clear()
    assert 0 == pool.freesize


@asyncio.coroutine
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


@asyncio.coroutine
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


@asyncio.coroutine
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


@asyncio.coroutine
def test_release_with_invalid_status(create_pool):
    pool = yield from create_pool(minsize=10)
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


@asyncio.coroutine
def test_default_event_loop(create_pool, loop):
    asyncio.set_event_loop(loop)

    pool = yield from create_pool(no_loop=True)
    assert pool._loop is loop


@asyncio.coroutine
def test_cursor(create_pool):
    pool = yield from create_pool()
    with (yield from pool.cursor()) as cur:
        yield from cur.execute('SELECT 1')
        ret = yield from cur.fetchone()
        assert (1,) == ret
    assert cur.closed


@asyncio.coroutine
def test_release_with_invalid_status_wait_release(create_pool):
    pool = yield from create_pool(minsize=10)
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


@asyncio.coroutine
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


@asyncio.coroutine
def test_connect_from_acquire(create_pool):
    pool = yield from create_pool(minsize=0)
    assert 0 == pool.freesize
    assert 0 == pool.size
    with (yield from pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize


@asyncio.coroutine
def test_create_pool_with_timeout(create_pool):
    timeout = 0.1
    pool = yield from create_pool(timeout=timeout)
    assert timeout == pool.timeout
    conn = yield from pool.acquire()
    assert timeout == conn.timeout
    pool.release(conn)


@asyncio.coroutine
def test_cursor_with_timeout(create_pool):
    timeout = 0.1
    pool = yield from create_pool()
    with (yield from pool.cursor(timeout=timeout)) as cur:
        assert timeout == cur.timeout


@asyncio.coroutine
def test_concurrency(create_pool):
    pool = yield from create_pool(minsize=2, maxsize=4)
    c1 = yield from pool.acquire()
    c2 = yield from pool.acquire()
    assert 0 == pool.freesize
    assert 2 == pool.size
    pool.release(c1)
    pool.release(c2)


@asyncio.coroutine
def test_invalid_minsize(create_pool):
    with pytest.raises(ValueError):
        yield from create_pool(minsize=-1)


@asyncio.coroutine
def test_invalid__maxsize(create_pool):
    with pytest.raises(ValueError):
        yield from create_pool(minsize=5, maxsize=2)


@asyncio.coroutine
def test_true_parallel_tasks(create_pool, loop):
    pool = yield from create_pool(minsize=0, maxsize=1)
    assert 1 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    maxsize = 0
    minfreesize = 100

    def inner():
        nonlocal maxsize, minfreesize
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        conn = yield from pool.acquire()
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        yield from asyncio.sleep(0.01, loop=loop)
        pool.release(conn)
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)

    yield from asyncio.gather(inner(), inner(), loop=loop)

    assert 1 == maxsize
    assert 0 == minfreesize


@asyncio.coroutine
def test_wait_closed(create_pool, loop):
    pool = yield from create_pool(minsize=10)

    c1 = yield from pool.acquire()
    c2 = yield from pool.acquire()
    assert 10 == pool.size
    assert 8 == pool.freesize

    ops = []

    @asyncio.coroutine
    def do_release(conn):
        yield from asyncio.sleep(0, loop=loop)
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
                              loop=loop)
    assert ['release', 'release', 'wait_closed'] == ops
    assert 0 == pool.freesize


@asyncio.coroutine
def test_echo(create_pool):
    pool = yield from create_pool(echo=True)
    assert pool.echo

    with (yield from pool) as conn:
        assert conn.echo


@asyncio.coroutine
def test_cannot_acquire_after_closing(create_pool):
    pool = yield from create_pool()
    pool.close()

    with pytest.raises(RuntimeError):
        yield from pool.acquire()


@asyncio.coroutine
def test_terminate_with_acquired_connections(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    pool.terminate()
    yield from pool.wait_closed()

    assert conn.closed


@asyncio.coroutine
def test_release_closed_connection(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    conn.close()

    pool.release(conn)


@asyncio.coroutine
def test_wait_closing_on_not_closed(create_pool):
    pool = yield from create_pool()

    with pytest.raises(RuntimeError):
        yield from pool.wait_closed()


@asyncio.coroutine
def test_release_terminated_pool(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    pool.terminate()
    yield from pool.wait_closed()

    pool.release(conn)


@asyncio.coroutine
def test_release_terminated_pool_with_wait_release(create_pool):
    pool = yield from create_pool()
    conn = yield from pool.acquire()
    pool.terminate()
    yield from pool.wait_closed()

    yield from pool.release(conn)


@asyncio.coroutine
def test_close_with_acquired_connections(create_pool, loop):
    pool = yield from create_pool()
    yield from pool.acquire()
    pool.close()

    with pytest.raises(asyncio.TimeoutError):
        yield from asyncio.wait_for(pool.wait_closed(), 0.1, loop=loop)


@pytest.mark.skipif(sys.version_info < (3, 4),
                    reason="Python 3.3 doesnt support __del__ calls from GC")
@asyncio.coroutine
def test___del__(loop, pg_params, warning):
    pool = yield from aiopg.create_pool(loop=loop, **pg_params)
    with warning(ResourceWarning):
        del pool


@asyncio.coroutine
def test_unlimited_size(create_pool):
    pool = yield from create_pool(maxsize=0)
    assert 1 == pool.minsize
    assert pool._free.maxlen is None


@asyncio.coroutine
def test_connection_in_good_state_after_timeout(create_pool):
    @asyncio.coroutine
    def sleep(conn):
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT pg_sleep(10);')

    pool = yield from create_pool(minsize=1,
                                  maxsize=1,
                                  timeout=0.1)
    with (yield from pool) as conn:
        with pytest.raises(asyncio.TimeoutError):
            yield from sleep(conn)

    assert 1 == pool.freesize

    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1;')
        val = yield from cur.fetchone()
        assert (1,) == val


@asyncio.coroutine
def test_pool_with_connection_recycling(create_pool, loop):
    pool = yield from create_pool(minsize=1,
                                  maxsize=1,
                                  pool_recycle=3)
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1;')
        val = yield from cur.fetchone()
        assert (1,) == val

    yield from asyncio.sleep(5, loop=loop)

    assert 1 == pool.freesize
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1;')
        val = yield from cur.fetchone()
        assert (1,) == val


@asyncio.coroutine
def test_connection_in_good_state_after_timeout_in_transaction(create_pool):
    @asyncio.coroutine
    def sleep(conn):
        cur = yield from conn.cursor()
        yield from cur.execute('BEGIN;')
        yield from cur.execute('SELECT pg_sleep(10);')

    pool = yield from create_pool(minsize=1,
                                  maxsize=1,
                                  timeout=0.1)
    with (yield from pool) as conn:
        with pytest.raises(asyncio.TimeoutError):
            yield from sleep(conn)

    assert 0 == pool.freesize
    assert 0 == pool.size
    with (yield from pool) as conn:
        cur = yield from conn.cursor()
        yield from cur.execute('SELECT 1;')
        val = yield from cur.fetchone()
        assert (1,) == val


@asyncio.coroutine
def test_drop_connection_if_timedout(make_connection,
                                     create_pool, loop):

    @asyncio.coroutine
    def _kill_connections():
        # Drop all connections on server
        conn = yield from make_connection()
        cur = yield from conn.cursor()
        yield from cur.execute("""WITH inactive_connections_list AS (
        SELECT pid FROM  pg_stat_activity WHERE pid <> pg_backend_pid())
        SELECT pg_terminate_backend(pid) FROM inactive_connections_list""")
        cur.close()
        conn.close()

    pool = yield from create_pool(minsize=3)
    yield from _kill_connections()
    yield from asyncio.sleep(0.5, loop=loop)

    assert len(pool._free) == 3
    assert all([c.closed for c in pool._free])

    conn = yield from pool.acquire()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1;')
    pool.release(conn)
    conn.close()
    pool.close()
    yield from pool.wait_closed()


@asyncio.coroutine
def test_close_running_cursor(create_pool):
    pool = yield from create_pool(minsize=3)

    with pytest.raises(asyncio.TimeoutError):
        with (yield from pool.cursor(timeout=0.1)) as cur:
            yield from cur.execute('SELECT pg_sleep(10)')


@asyncio.coroutine
def test_pool_on_connect(create_pool):
    called = False

    @asyncio.coroutine
    def cb(connection):
        nonlocal called
        cur = yield from connection.cursor()
        yield from cur.execute('SELECT 1')
        data = yield from cur.fetchall()
        assert [(1,)] == data
        called = True

    pool = yield from create_pool(on_connect=cb)

    with (yield from pool.cursor()) as cur:
        yield from cur.execute('SELECT 1')

    assert called
