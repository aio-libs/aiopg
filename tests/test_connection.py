import asyncio
import aiopg
import psycopg2
import psycopg2.extras
import unittest

from aiopg.connection import Connection
from aiopg.cursor import Cursor


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
        return conn

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

    def xtest_execute_twice(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur1 = yield from conn.cursor()
            cur2 = yield from conn.cursor()
            #import ipdb;ipdb.set_trace()
            coro1 = cur1.execute('SELECT 1')
            fut1 = next(coro1)
            self.assertIsInstance(fut1, asyncio.Future)
            coro2 = cur2.execute('SELECT 2')
            with self.assertRaises(asyncio.TimeoutError):
                print('aaaaaaaaaaaaaaaaaaaaaaaaaaaa')
                fut2 = asyncio.wait_for(coro2, timeout=0.001,
                                        loop=self.loop)
                fut2
                yield from asyncio.sleep(0.002)

            print(1111111111111111111111)
            ret1 = yield from coro1
            self.assertEqual((1,), ret1)

            ret2 = yield from coro1
            self.assertEqual((2,), ret2)

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
