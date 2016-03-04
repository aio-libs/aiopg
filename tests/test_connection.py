import asyncio
import aiopg
import gc
import psycopg2
import psycopg2.extras
import pytest
import socket
import time
import sys

from aiopg.connection import Connection, TIMEOUT
from aiopg.cursor import Cursor
from unittest import mock


PY_341 = sys.version_info >= (3, 4, 1)


@pytest.fixture
def connect(make_connection):

    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_connection(**kwargs)
        conn2 = yield from make_connection(**kwargs)
        cur = yield from conn2.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS foo")
        yield from conn2.close()
        return conn

    return go


@pytest.mark.run_loop
def test_connect(connect):
    conn = yield from connect()
    assert isinstance(conn, Connection)
    assert not conn._writing
    assert conn._conn is conn.raw
    assert not conn.echo


@pytest.mark.run_loop
def test_simple_select(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert isinstance(cur, Cursor)
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret


@pytest.mark.run_loop
def test_default_event_loop(connect, loop):
    asyncio.set_event_loop(loop)

    conn = yield from connect(no_loop=True)
    cur = yield from conn.cursor()
    assert isinstance(cur, Cursor)
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret
    assert conn._loop is loop


@pytest.mark.run_loop
def test_close(connect):
    conn = yield from connect()
    yield from conn.close()
    assert conn.closed


@pytest.mark.run_loop
def test_close_twice(connect):
    conn = yield from connect()
    yield from conn.close()
    yield from conn.close()
    assert conn.closed


@pytest.mark.run_loop
def test_with_cursor_factory(connect):
    conn = yield from connect()
    cur = yield from conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor)
    yield from cur.execute('SELECT 1 AS a')
    ret = yield from cur.fetchone()
    assert 1 == ret['a']


@pytest.mark.run_loop
def test_closed(connect):
    conn = yield from connect()
    assert not conn.closed
    yield from conn.close()
    assert conn.closed


@pytest.mark.run_loop
def test_tpc(connect):
    conn = yield from connect()
    xid = yield from conn.xid(1, 'a', 'b')
    assert (1, 'a', 'b') == tuple(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.tpc_begin(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.tpc_prepare()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.tpc_commit(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.tpc_rollback(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.tpc_recover()


@pytest.mark.run_loop
def test_reset(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.reset()


@pytest.mark.run_loop
def test_lobject(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.lobject()


@pytest.mark.run_loop
def test_set_session(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.set_session()


@pytest.mark.run_loop
def test_dsn(connect):
    conn = yield from connect()
    assert 'dbname=aiopg user=aiopg password=xxxxxx host=127.0.0.1' == conn.dsn


@pytest.mark.run_loop
def test_get_backend_pid(connect):
    conn = yield from connect()

    ret = yield from conn.get_backend_pid()
    assert 0 != ret


@pytest.mark.run_loop
def test_get_parameter_status(connect):
    conn = yield from connect()

    ret = yield from conn.get_parameter_status('is_superuser')
    assert 'off' == ret


@pytest.mark.run_loop
def test_cursor_factory(connect):
    conn = yield from connect(cursor_factory=psycopg2.extras.DictCursor)

    assert psycopg2.extras.DictCursor is conn.cursor_factory


@pytest.mark.run_loop
def test_notices(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute("CREATE TABLE foo (id serial PRIMARY KEY);")

    if not conn.notices:
        raise pytest.skip("Notices are disabled")

    assert ['NOTICE:  CREATE TABLE will create implicit sequence '
            '"foo_id_seq" for serial column "foo.id"\n',
            'NOTICE:  CREATE TABLE / PRIMARY KEY will create '
            'implicit index "foo_pkey" for table "foo"\n'] == conn.notices


@pytest.mark.run_loop
def test_autocommit(connect):
    conn = yield from connect()

    assert conn.autocommit

    with pytest.raises(psycopg2.ProgrammingError):
        conn.autocommit = False

    assert conn.autocommit


@pytest.mark.run_loop
def test_isolation_level(connect):
    conn = yield from connect()

    assert 0 == conn.isolation_level
    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.set_isolation_level(1)

    assert 0 == conn.isolation_level


@pytest.mark.run_loop
def test_encoding(connect):
    conn = yield from connect()

    assert 'UTF8' == conn.encoding
    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.set_client_encoding('ascii')

    assert 'UTF8' == conn.encoding


@pytest.mark.run_loop
def test_get_transaction_status(connect):
    conn = yield from connect()

    ret = yield from conn.get_transaction_status()
    assert 0 == ret


@pytest.mark.run_loop
def test_transaction(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.commit()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.rollback()


@pytest.mark.run_loop
def test_status(connect):
    conn = yield from connect()
    assert 1 == conn.status


@pytest.mark.run_loop
def test_protocol_version(connect):
    conn = yield from connect()
    assert 0 < conn.protocol_version


@pytest.mark.run_loop
def test_server_version(connect):
    conn = yield from connect()
    assert 0 < conn.server_version


@pytest.mark.run_loop
def test_cancel_noop(connect):
    conn = yield from connect()
    yield from conn.cancel()


@pytest.mark.run_loop
def test_cancel_with_timeout(connect, warning):
    conn = yield from connect()
    with warning(DeprecationWarning):
        yield from conn.cancel(10)


@pytest.mark.run_loop
def test_cancel_pending_op(connect, loop):
    conn = yield from connect()
    cur = yield from conn.cursor()
    task = asyncio.async(cur.execute("SELECT pg_sleep(10)"), loop=loop)
    yield from asyncio.sleep(0.01, loop=loop)
    yield from conn.cancel()

    with pytest.raises(asyncio.CancelledError):
        yield from task


@pytest.mark.run_loop
def test_close2(connect, loop):
    conn = yield from connect()
    conn._writing = True
    loop.add_writer(conn._fileno, conn._ready, conn._weakref)
    conn.close()
    assert not conn._writing
    assert conn.closed


@pytest.mark.run_loop
def test_psyco_exception(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    with pytest.raises(psycopg2.ProgrammingError):
        yield from cur.execute('SELECT * FROM unknown_table')


def test_ready_set_exception(connect, loop):
    @asyncio.coroutine
    def go():
        conn = yield from connect()
        impl = mock.Mock()
        impl.notifies = []
        exc = psycopg2.ProgrammingError("something bad")
        impl.poll.side_effect = exc
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter('test')

        conn._ready(conn._weakref)
        assert not conn._writing
        return waiter

    waiter = loop.run_until_complete(go())

    with pytest.raises(psycopg2.ProgrammingError):
        loop.run_until_complete(waiter)


def test_ready_OK_with_waiter(connect, loop):
    @asyncio.coroutine
    def go():
        conn = yield from connect()
        impl = mock.Mock()
        impl.notifies = []
        impl.poll.return_value = psycopg2.extensions.POLL_OK
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter('test')

        conn._ready(conn._weakref)
        assert not conn._writing
        assert not impl.close.called
        return waiter

    waiter = loop.run_until_complete(go())

    assert loop.run_until_complete(waiter) is None


def test_ready_POLL_ERROR(connect, loop):
    @asyncio.coroutine
    def go():
        conn = yield from connect()
        impl = mock.Mock()
        impl.notifies = []
        impl.poll.return_value = psycopg2.extensions.POLL_ERROR
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter('test')
        handler = mock.Mock()
        loop.set_exception_handler(handler)

        conn._ready(conn._weakref)
        handler.assert_called_with(
            loop,
            {'connection': conn,
             'message': 'Fatal error on aiopg connection: '
                        'POLL_ERROR from underlying .poll() call'})
        assert not conn._writing
        assert impl.close.called
        return waiter

    waiter = loop.run_until_complete(go())
    with pytest.raises(psycopg2.OperationalError):
        loop.run_until_complete(waiter)


def test_ready_unknown_answer(connect, loop):
    @asyncio.coroutine
    def go():
        conn = yield from connect()
        impl = mock.Mock()
        impl.notifies = []
        impl.poll.return_value = 9999
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter('test')
        handler = mock.Mock()
        loop.set_exception_handler(handler)

        conn._ready(conn._weakref)
        handler.assert_called_with(
            loop,
            {'connection': conn,
             'message': 'Fatal error on aiopg connection: '
                        'unknown answer 9999 from underlying .poll() call'}
            )
        assert not conn._writing
        assert impl.close.called
        return waiter

    waiter = loop.run_until_complete(go())
    with pytest.raises(psycopg2.OperationalError):
        loop.run_until_complete(waiter)


@pytest.mark.run_loop
def test_execute_twice(connect):
    conn = yield from connect()
    cur1 = yield from conn.cursor()
    cur2 = yield from conn.cursor()
    coro1 = cur1.execute('SELECT 1')
    fut1 = next(coro1)
    assert isinstance(fut1, asyncio.Future)
    coro2 = cur2.execute('SELECT 2')

    with pytest.raises(RuntimeError):
        next(coro2)


@pytest.mark.run_loop
def test_connect_to_unsupported_port(unused_port, loop, pg_params):
    port = unused_port()
    pg_params['port'] = port

    with pytest.raises(psycopg2.OperationalError):
        yield from aiopg.connect(loop=loop, **pg_params)


@pytest.mark.run_loop
def test_binary_protocol_error(connect):
    conn = yield from connect()
    s = socket.fromfd(conn._fileno, socket.AF_INET, socket.SOCK_STREAM)
    s.send(b'garbage')
    s.detach()
    cur = yield from conn.cursor()
    with pytest.raises(psycopg2.DatabaseError):
        yield from cur.execute('SELECT 1')


@pytest.mark.run_loop
def test_closing_in_separate_task(connect, loop):
    event = asyncio.Future(loop=loop)

    @asyncio.coroutine
    def waiter(conn):
        cur = yield from conn.cursor()
        fut = cur.execute("SELECT pg_sleep(1000)")
        event.set_result(None)
        with pytest.raises(psycopg2.OperationalError):
            yield from fut

    @asyncio.coroutine
    def closer(conn):
        yield from event
        yield from conn.close()

    conn = yield from connect()
    yield from asyncio.gather(waiter(conn), closer(conn),
                              loop=loop)


@pytest.mark.run_loop
def test_connection_timeout(connect):
    timeout = 0.1
    conn = yield from connect(timeout=timeout)
    assert timeout == conn.timeout
    cur = yield from conn.cursor()
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


@pytest.mark.run_loop
def test_override_cursor_timeout(connect):
    timeout = 0.1
    conn = yield from connect()
    assert TIMEOUT == conn.timeout
    cur = yield from conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


@pytest.mark.run_loop
def test_echo(connect):
    conn = yield from connect(echo=True)
    assert conn.echo


@pytest.mark.skipif(not PY_341,
                    reason="Python 3.3 doesnt support __del__ calls from GC")
@pytest.mark.run_loop
def test___del__(loop, pg_params, warning):
    exc_handler = mock.Mock()
    loop.set_exception_handler(exc_handler)
    conn = yield from aiopg.connect(loop=loop, **pg_params)
    with warning(ResourceWarning):
        del conn
        gc.collect()

    msg = {'connection': mock.ANY,  # conn was deleted
           'message': 'Unclosed connection'}
    if loop.get_debug():
        msg['source_traceback'] = mock.ANY
        exc_handler.assert_called_with(loop, msg)


@pytest.mark.run_loop
def test_notifies(connect):
    conn1 = yield from connect()
    cur1 = yield from conn1.cursor()
    conn2 = yield from connect()
    cur2 = yield from conn2.cursor()
    yield from cur1.execute('LISTEN test')
    assert conn2.notifies.empty()
    yield from cur2.execute("NOTIFY test, 'hello'")
    val = yield from conn1.notifies.get()
    assert 'test' == val.channel
    assert 'hello' == val.payload

    cur2.close()
    cur1.close()


@pytest.mark.run_loop
def test_close_cursor_on_timeout_error(connect):
    conn = yield from connect()
    cur = yield from conn.cursor(timeout=0.01)
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.execute("SELECT pg_sleep(10)")

    assert cur.closed
    assert not conn.closed

    yield from conn.close()
