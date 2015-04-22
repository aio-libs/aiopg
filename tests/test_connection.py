import asyncio
import aiopg
import psycopg2
import psycopg2.extras
import socket
import random
import unittest
import time
import sys

from aiopg.connection import Connection, TIMEOUT
from aiopg.cursor import Cursor
from unittest import mock


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def connect(self, no_loop=False, **kwargs):
        loop = None if no_loop else self.loop
        conn = yield from aiopg.connect(database='aiopg',
                                        user='aiopg',
                                        password='passwd',
                                        host='127.0.0.1',
                                        loop=loop,
                                        **kwargs)
        conn2 = yield from aiopg.connect(database='aiopg',
                                         user='aiopg',
                                         password='passwd',
                                         host='127.0.0.1',
                                         loop=loop)
        cur = yield from conn2.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS foo")
        yield from conn2.close()
        self.addCleanup(conn.close)
        return conn

    def test_connect(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertIsInstance(conn, Connection)
            self.assertFalse(conn._reading)
            self.assertFalse(conn._writing)
            self.assertIs(conn._conn, conn.raw)
            self.assertFalse(conn.echo)

        self.loop.run_until_complete(go())

    def test_simple_select(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertIsInstance(cur, Cursor)
            yield from cur.execute('SELECT 1')
            ret = yield from cur.fetchone()
            self.assertEqual((1,), ret)

        self.loop.run_until_complete(go())

    def test_default_event_loop(self):
        asyncio.set_event_loop(self.loop)

        @asyncio.coroutine
        def go():
            conn = yield from self.connect(no_loop=True)
            cur = yield from conn.cursor()
            self.assertIsInstance(cur, Cursor)
            yield from cur.execute('SELECT 1')
            ret = yield from cur.fetchone()
            self.assertEqual((1,), ret)
            self.assertIs(conn._loop, self.loop)

        self.loop.run_until_complete(go())

    def test_close(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.close()
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_close_twice(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.close()
            yield from conn.close()
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_with_cursor_factory(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor)
            yield from cur.execute('SELECT 1 AS a')
            ret = yield from cur.fetchone()
            self.assertEqual(1, ret['a'])

        self.loop.run_until_complete(go())

    def test_closed(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertFalse(conn.closed)
            yield from conn.close()
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_tpc(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            xid = yield from conn.xid(1, 'a', 'b')
            self.assertEqual((1, 'a', 'b'), tuple(xid))

            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.tpc_begin(xid)
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.tpc_prepare()
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.tpc_commit(xid)
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.tpc_rollback(xid)
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.tpc_recover()

        self.loop.run_until_complete(go())

    def test_reset(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.reset()

        self.loop.run_until_complete(go())

    def test_lobject(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.lobject()

        self.loop.run_until_complete(go())

    def test_set_session(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.set_session()

        self.loop.run_until_complete(go())

    def test_dsn(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertEqual(
                'dbname=aiopg user=aiopg password=xxxxxx host=127.0.0.1',
                conn.dsn)

        self.loop.run_until_complete(go())

    def test_get_backend_pid(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            ret = yield from conn.get_backend_pid()
            self.assertNotEqual(0, ret)

        self.loop.run_until_complete(go())

    def test_get_parameter_status(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            ret = yield from conn.get_parameter_status('is_superuser')
            self.assertEqual('off', ret)

        self.loop.run_until_complete(go())

    def test_cursor_factory(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect(
                cursor_factory=psycopg2.extras.DictCursor)

            self.assertIs(psycopg2.extras.DictCursor, conn.cursor_factory)

        self.loop.run_until_complete(go())

    def test_notices(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute("CREATE TABLE foo (id serial PRIMARY KEY);")

            if not conn.notices:
                raise unittest.SkipTest("Notices are disabled")

            self.assertEqual(
                ['NOTICE:  CREATE TABLE will create implicit sequence '
                 '"foo_id_seq" for serial column "foo.id"\n',
                 'NOTICE:  CREATE TABLE / PRIMARY KEY will create '
                 'implicit index "foo_pkey" for table "foo"\n'],
                conn.notices)

        self.loop.run_until_complete(go())

    def test_autocommit(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            self.assertTrue(conn.autocommit)
            with self.assertRaises(psycopg2.ProgrammingError):
                conn.autocommit = False
            self.assertTrue(conn.autocommit)

        self.loop.run_until_complete(go())

    def test_isolation_level(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            self.assertEqual(0, conn.isolation_level)
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.set_isolation_level(1)
            self.assertEqual(0, conn.isolation_level)

        self.loop.run_until_complete(go())

    def test_encoding(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            self.assertEqual('UTF8', conn.encoding)
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.set_client_encoding('ascii')
            self.assertEqual('UTF8', conn.encoding)

        self.loop.run_until_complete(go())

    def test_get_transaction_status(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            ret = yield from conn.get_transaction_status()
            self.assertEqual(0, ret)

        self.loop.run_until_complete(go())

    def test_transaction(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.commit()
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.rollback()

        self.loop.run_until_complete(go())

    def test_status(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertEqual(1, conn.status)

        self.loop.run_until_complete(go())

    def test_protocol_version(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertLess(0, conn.protocol_version)

        self.loop.run_until_complete(go())

    def test_server_version(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            self.assertLess(0, conn.server_version)

        self.loop.run_until_complete(go())

    def test_cancel(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.cancel()

        self.loop.run_until_complete(go())

    def test_cancel_with_timeout(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            yield from conn.cancel(10)

        self.loop.run_until_complete(go())

    def test_ready_without_waiter(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            conn._waiter = None
            handler = mock.Mock()
            self.loop.set_exception_handler(handler)
            conn._ready()
            handler.assert_called_with(
                self.loop,
                {'connection': conn,
                 'message': 'Fatal error on aiopg connection: '
                            'bad state in _ready callback'})
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_close2(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            conn._reading = conn._writing = True
            self.loop.add_reader(conn._fileno, conn._ready)
            self.loop.add_writer(conn._fileno, conn._ready)
            conn.close()
            self.assertFalse(conn._reading)
            self.assertFalse(conn._writing)
            self.assertTrue(conn.closed)

        self.loop.run_until_complete(go())

    def test_psyco_exception(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from cur.execute('SELECT * FROM unknown_table')

        self.loop.run_until_complete(go())

    def test_ready_set_exception(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            impl = mock.Mock()
            exc = psycopg2.ProgrammingError("something bad")
            impl.poll.side_effect = exc
            conn._conn = impl
            conn._writing = True
            waiter = conn._create_waiter('test')

            conn._ready()
            self.assertFalse(conn._writing)
            return waiter

        waiter = self.loop.run_until_complete(go())

        with self.assertRaises(psycopg2.ProgrammingError):
            self.loop.run_until_complete(waiter)

    def test_ready_OK_with_waiter(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            impl = mock.Mock()
            impl.poll.return_value = psycopg2.extensions.POLL_OK
            conn._conn = impl
            conn._writing = True
            waiter = conn._create_waiter('test')

            conn._ready()
            self.assertFalse(conn._writing)
            self.assertFalse(impl.close.called)
            return waiter

        waiter = self.loop.run_until_complete(go())

        self.assertIsNone(self.loop.run_until_complete(waiter))

    def test_ready_POLL_ERROR(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            impl = mock.Mock()
            impl.poll.return_value = psycopg2.extensions.POLL_ERROR
            conn._conn = impl
            conn._writing = True
            waiter = conn._create_waiter('test')
            handler = mock.Mock()
            self.loop.set_exception_handler(handler)

            conn._ready()
            handler.assert_called_with(
                self.loop,
                {'connection': conn,
                 'message': 'Fatal error on aiopg connection: '
                            'POLL_ERROR from underlying .poll() call'})
            self.assertFalse(conn._writing)
            self.assertTrue(impl.close.called)
            return waiter

        waiter = self.loop.run_until_complete(go())
        with self.assertRaises(psycopg2.OperationalError):
            self.loop.run_until_complete(waiter)

    def test_ready_unknown_answer(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            impl = mock.Mock()
            impl.poll.return_value = 9999
            conn._conn = impl
            conn._writing = True
            waiter = conn._create_waiter('test')
            handler = mock.Mock()
            self.loop.set_exception_handler(handler)

            conn._ready()
            handler.assert_called_with(
                self.loop,
                {'connection': conn,
                 'message': 'Fatal error on aiopg connection: '
                            'unknown answer 9999 from underlying .poll() call'}
                )
            self.assertFalse(conn._writing)
            self.assertTrue(impl.close.called)
            return waiter

        waiter = self.loop.run_until_complete(go())
        with self.assertRaises(psycopg2.OperationalError):
            self.loop.run_until_complete(waiter)

    def test_execute_twice(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur1 = yield from conn.cursor()
            cur2 = yield from conn.cursor()
            coro1 = cur1.execute('SELECT 1')
            fut1 = next(coro1)
            self.assertIsInstance(fut1, asyncio.Future)
            coro2 = cur2.execute('SELECT 2')

            with self.assertRaises(RuntimeError):
                next(coro2)

        self.loop.run_until_complete(go())

    def test_connect_to_unsupported_port(self):
        while True:
            s = socket.socket(socket.AF_INET)
            port = random.randint(1024, 65535)
            try:
                s.bind(('127.0.0.1', port))
                s.close()
                break
            except ConnectionError:
                pass

        @asyncio.coroutine
        def go():
            with self.assertRaises(psycopg2.OperationalError):
                yield from aiopg.connect(database='aiopg',
                                         user='aiopg',
                                         password='passwd',
                                         host='127.0.0.1',
                                         port=port,
                                         loop=self.loop)
        self.loop.run_until_complete(go())

    def test_binary_protocol_error(self):

        @asyncio.coroutine
        def go():
            conn = yield from aiopg.connect(database='aiopg',
                                            user='aiopg',
                                            password='passwd',
                                            host='127.0.0.1',
                                            loop=self.loop)
            s = socket.fromfd(conn._fileno, socket.AF_INET, socket.SOCK_STREAM)
            s.send(b'garbage')
            s.detach()
            cur = yield from conn.cursor()
            with self.assertRaises(psycopg2.OperationalError):
                yield from cur.execute('SELECT 1')

        self.loop.run_until_complete(go())

    def test_closing_in_separate_task(self):
        event = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def waiter(conn):
            cur = yield from conn.cursor()
            fut = cur.execute("SELECT pg_sleep(1000)")
            event.set_result(None)
            with self.assertRaises(psycopg2.OperationalError):
                yield from fut

        @asyncio.coroutine
        def closer(conn):
            yield from event
            yield from conn.close()

        @asyncio.coroutine
        def go():
            conn = yield from aiopg.connect(database='aiopg',
                                            user='aiopg',
                                            password='passwd',
                                            host='127.0.0.1',
                                            loop=self.loop)
            yield from asyncio.gather(waiter(conn), closer(conn),
                                      loop=self.loop)

        self.loop.run_until_complete(go())

    def test_connection_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            conn = yield from self.connect(timeout=timeout)
            self.assertEqual(timeout, conn.timeout)
            cur = yield from conn.cursor()
            self.assertEqual(timeout, cur.timeout)

            t1 = time.time()
            with self.assertRaises(asyncio.TimeoutError):
                yield from cur.execute("SELECT pg_sleep(1)")
            t2 = time.time()
            dt = t2 - t1
            self.assertTrue(0.09 <= dt <= 0.11, dt)

        self.loop.run_until_complete(go())

    def test_override_cursor_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            conn = yield from self.connect()
            self.assertEqual(TIMEOUT, conn.timeout)
            cur = yield from conn.cursor(timeout=timeout)
            self.assertEqual(timeout, cur.timeout)

            t1 = time.time()
            with self.assertRaises(asyncio.TimeoutError):
                yield from cur.execute("SELECT pg_sleep(1)")
            t2 = time.time()
            dt = t2 - t1
            self.assertTrue(0.09 <= dt <= 0.11, dt)

        self.loop.run_until_complete(go())

    def test_echo(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect(echo=True)
            self.assertTrue(conn.echo)

        self.loop.run_until_complete(go())

    @unittest.skipIf(sys.version_info < (3, 4),
                     "Python 3.3 doesnt support __del__ calls from GC")
    def test___del__(self):
        @asyncio.coroutine
        def go():
            conn = yield from aiopg.connect(database='aiopg',
                                            user='aiopg',
                                            password='passwd',
                                            host='127.0.0.1',
                                            loop=self.loop)
            with self.assertWarns(ResourceWarning):
                del conn

        self.loop.run_until_complete(go())
