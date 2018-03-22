import asyncio
import aiopg
import gc
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import pytest
import socket
import time
import sys

from aiopg.connection import Connection, TIMEOUT
from aiopg.cursor import Cursor
from aiopg.utils import ensure_future
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


@asyncio.coroutine
def test_connect(connect):
    conn = yield from connect()
    assert isinstance(conn, Connection)
    assert not conn._writing
    assert conn._conn is conn.raw
    assert not conn.echo


@asyncio.coroutine
def test_simple_select(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert isinstance(cur, Cursor)
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret


@asyncio.coroutine
def test_default_event_loop(connect, loop):
    asyncio.set_event_loop(loop)

    conn = yield from connect(no_loop=True)
    cur = yield from conn.cursor()
    assert isinstance(cur, Cursor)
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret
    assert conn._loop is loop


@asyncio.coroutine
def test_close(connect):
    conn = yield from connect()
    yield from conn.close()
    assert conn.closed


@asyncio.coroutine
def test_close_twice(connect):
    conn = yield from connect()
    yield from conn.close()
    yield from conn.close()
    assert conn.closed


@asyncio.coroutine
def test_with_cursor_factory(connect):
    conn = yield from connect()
    cur = yield from conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor)
    yield from cur.execute('SELECT 1 AS a')
    ret = yield from cur.fetchone()
    assert 1 == ret['a']


@asyncio.coroutine
def test_closed(connect):
    conn = yield from connect()
    assert not conn.closed
    yield from conn.close()
    assert conn.closed


@asyncio.coroutine
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


@asyncio.coroutine
def test_reset(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.reset()


@asyncio.coroutine
def test_lobject(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.lobject()


@asyncio.coroutine
def test_set_session(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.set_session()


@asyncio.coroutine
def test_dsn(connect, pg_params):
    conn = yield from connect()
    pg_params['password'] = 'x' * len(pg_params['password'])
    assert 'dbname' in conn.dsn
    assert 'user' in conn.dsn
    assert 'password' in conn.dsn
    assert 'host' in conn.dsn
    assert 'port' in conn.dsn


@asyncio.coroutine
def test_get_backend_pid(connect):
    conn = yield from connect()

    ret = yield from conn.get_backend_pid()
    assert 0 != ret


@asyncio.coroutine
def test_get_parameter_status(connect):
    conn = yield from connect()

    ret = yield from conn.get_parameter_status('integer_datetimes')
    assert 'on' == ret


@asyncio.coroutine
def test_cursor_factory(connect):
    conn = yield from connect(cursor_factory=psycopg2.extras.DictCursor)

    assert psycopg2.extras.DictCursor is conn.cursor_factory


@asyncio.coroutine
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


@asyncio.coroutine
def test_autocommit(connect):
    conn = yield from connect()

    assert conn.autocommit

    with pytest.raises(psycopg2.ProgrammingError):
        conn.autocommit = False

    assert conn.autocommit


@asyncio.coroutine
def test_isolation_level(connect):
    conn = yield from connect()

    assert psycopg2.extensions.ISOLATION_LEVEL_DEFAULT == conn.isolation_level
    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.set_isolation_level(1)

    assert psycopg2.extensions.ISOLATION_LEVEL_DEFAULT == conn.isolation_level


@asyncio.coroutine
def test_encoding(connect):
    conn = yield from connect()

    assert 'UTF8' == conn.encoding
    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.set_client_encoding('ascii')

    assert 'UTF8' == conn.encoding


@asyncio.coroutine
def test_get_transaction_status(connect):
    conn = yield from connect()

    ret = yield from conn.get_transaction_status()
    assert 0 == ret


@asyncio.coroutine
def test_transaction(connect):
    conn = yield from connect()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.commit()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.rollback()


@asyncio.coroutine
def test_status(connect):
    conn = yield from connect()
    assert 1 == conn.status


@asyncio.coroutine
def test_protocol_version(connect):
    conn = yield from connect()
    assert 0 < conn.protocol_version


@asyncio.coroutine
def test_server_version(connect):
    conn = yield from connect()
    assert 0 < conn.server_version


@asyncio.coroutine
def test_cancel_noop(connect):
    conn = yield from connect()
    yield from conn.cancel()


@asyncio.coroutine
def test_cancel_pending_op(connect, loop):
    fut = asyncio.Future(loop=loop)

    @asyncio.coroutine
    def inner():
        fut.set_result(None)
        yield from cur.execute("SELECT pg_sleep(10)")

    conn = yield from connect()
    cur = yield from conn.cursor()
    task = ensure_future(inner(), loop=loop)
    yield from fut
    yield from asyncio.sleep(0.1, loop=loop)
    yield from conn.cancel()

    with pytest.raises(asyncio.CancelledError):
        yield from task


@asyncio.coroutine
def test_cancelled_connection_is_usable_asap(connect, loop):
    @asyncio.coroutine
    def inner(future, cursor):
        future.set_result(None)
        yield from cursor.execute("SELECT pg_sleep(10)")

    fut = asyncio.Future(loop=loop)
    conn = yield from connect()
    cur = yield from conn.cursor()
    task = ensure_future(inner(fut, cur), loop=loop)
    yield from fut
    yield from asyncio.sleep(0.1, loop=loop)

    task.cancel()

    delay = 0.001

    for tick in range(100):
        yield from asyncio.sleep(delay, loop=loop)
        status = conn._conn.get_transaction_status()
        if status == psycopg2.extensions.TRANSACTION_STATUS_IDLE:
            cur = yield from conn.cursor()
            yield from cur.execute("SELECT 1")
            ret = yield from cur.fetchone()
            assert (1,) == ret
            break
        delay *= 2
    else:
        assert False, "Cancelled connection transaction status never got idle"


@asyncio.coroutine
def test_cancelled_connection_is_not_usable_until_cancellation(connect, loop):
    @asyncio.coroutine
    def inner(future, cursor):
        future.set_result(None)
        yield from cursor.execute("SELECT pg_sleep(10)")

    fut = asyncio.Future(loop=loop)
    conn = yield from connect()
    cur = yield from conn.cursor()

    task = ensure_future(inner(fut, cur), loop=loop)
    yield from fut
    yield from asyncio.sleep(0.1, loop=loop)

    task.cancel()

    for i in range(100):
        yield from asyncio.sleep(0)
        if conn._cancelling:
            break
    else:
        assert False, "Connection did not start cancelling"

    # cur = yield from conn.cursor()
    with pytest.raises(RuntimeError) as e:
        yield from cur.execute('SELECT 1')
    assert str(e.value) == ('cursor.execute() called while connection '
                            'is being cancelled')


@asyncio.coroutine
def test_close2(connect, loop):
    conn = yield from connect()
    conn._writing = True
    loop.add_writer(conn._fileno, conn._ready, conn._weakref)
    conn.close()
    assert not conn._writing
    assert conn.closed


@asyncio.coroutine
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

        # keep a reference to underlying psycopg connection, and the fd alive,
        # otherwise the event loop will fail under windows
        old_conn = conn._conn
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter('test')

        conn._ready(conn._weakref)
        assert not conn._writing
        assert not impl.close.called

        conn._conn = old_conn
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


@asyncio.coroutine
def test_execute_twice(connect):
    conn = yield from connect()
    cur1 = yield from conn.cursor()
    # cur2 = yield from conn.cursor()
    coro1 = cur1.execute('SELECT 1')
    fut1 = next(coro1)
    assert isinstance(fut1, asyncio.Future)
    coro2 = cur1.execute('SELECT 2')

    with pytest.raises(RuntimeError):
        next(coro2)

    yield from conn.cancel()


@asyncio.coroutine
def test_connect_to_unsupported_port(unused_port, loop, pg_params):
    port = unused_port()
    pg_params['port'] = port

    with pytest.raises(psycopg2.OperationalError):
        yield from aiopg.connect(loop=loop, **pg_params)


@asyncio.coroutine
def test_binary_protocol_error(connect):
    conn = yield from connect()
    s = socket.fromfd(conn._fileno, socket.AF_INET, socket.SOCK_STREAM)
    s.send(b'garbage')
    s.detach()
    cur = yield from conn.cursor()
    with pytest.raises(psycopg2.DatabaseError):
        yield from cur.execute('SELECT 1')


@asyncio.coroutine
def test_closing_in_separate_task(connect, loop):
    closed_event = asyncio.Event(loop=loop)
    exec_created = asyncio.Event(loop=loop)

    @asyncio.coroutine
    def waiter(conn):
        cur = yield from conn.cursor()
        fut = cur.execute("SELECT pg_sleep(1000)")
        exec_created.set()
        yield from closed_event.wait()
        with pytest.raises(psycopg2.InterfaceError):
            yield from fut

    @asyncio.coroutine
    def closer(conn):
        yield from exec_created.wait()
        yield from conn.close()
        closed_event.set()

    conn = yield from connect()
    yield from asyncio.gather(waiter(conn), closer(conn),
                              loop=loop)


@asyncio.coroutine
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


@asyncio.coroutine
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


@asyncio.coroutine
def test_echo(connect):
    conn = yield from connect(echo=True)
    assert conn.echo


@pytest.mark.skipif(not PY_341,
                    reason="Python 3.3 doesnt support __del__ calls from GC")
@asyncio.coroutine
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


@asyncio.coroutine
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


@asyncio.coroutine
def test_close_cursor_on_timeout_error(connect):
    conn = yield from connect()
    cur = yield from conn.cursor(timeout=0.01)
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.execute("SELECT pg_sleep(10)")

    assert cur.closed
    assert not conn.closed

    conn.close()


@asyncio.coroutine
def test_issue_111_crash_on_connect_error(loop):
    import aiopg.connection
    with pytest.raises(psycopg2.ProgrammingError):
        yield from aiopg.connection.connect('baddsn:1', loop=loop)


@asyncio.coroutine
def test_remove_reader_from_alive_fd(connect):
    conn = yield from connect()
    # keep a reference to underlying psycopg connection, and the fd alive
    _conn = conn._conn  # noqa
    fileno = conn._fileno

    impl = mock.Mock()
    exc = psycopg2.OperationalError('Test')
    impl.poll.side_effect = exc
    conn._conn = impl
    conn._fileno = fileno

    m_remove_reader = mock.Mock()
    conn._loop.remove_reader = m_remove_reader

    conn._ready(conn._weakref)
    assert not m_remove_reader.called

    conn.close()
    assert m_remove_reader.called_with(fileno)


@asyncio.coroutine
def test_remove_reader_from_dead_fd(connect):
    conn = yield from connect()
    fileno = conn._conn.fileno()
    _conn = conn._conn

    impl = mock.Mock()
    exc = psycopg2.OperationalError('Test')
    impl.poll.side_effect = exc
    conn._conn = impl
    conn._fileno = fileno

    _conn.close()

    m_remove_reader = mock.Mock()
    old_remove_reader = conn._loop.remove_reader
    conn._loop.remove_reader = m_remove_reader

    conn._ready(conn._weakref)
    assert m_remove_reader.called_with(fileno)

    m_remove_reader.reset_mock()
    conn.close()
    assert not m_remove_reader.called
    old_remove_reader(fileno)


@asyncio.coroutine
def test_connection_on_server_restart(connect, pg_server, docker):
    # Operation on closed connection should raise OperationalError
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret
    docker.restart(container=pg_server['Id'])

    with pytest.raises(psycopg2.OperationalError):
        yield from cur.execute('SELECT 1')
    conn.close()

    # Wait for postgres to be up and running again before moving on
    # so as the restart won't affect other tests
    delay = 0.001
    for i in range(100):
        try:
            conn = yield from connect()
            conn.close()
            break
        except psycopg2.Error:
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail("Cannot connect to the restarted server")
