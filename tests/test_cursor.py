import asyncio
import aiopg
import psycopg2
import psycopg2.tz
import time
import unittest

from aiopg.connection import TIMEOUT


class TestCursor(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = (yield from aiopg.connect(database='aiopg',
                                         user='aiopg',
                                         password='passwd',
                                         host='127.0.0.1',
                                         loop=self.loop,
                                         **kwargs))
        self.addCleanup(conn.close)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("CREATE TABLE tbl (id int, name varchar(255))")
        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            yield from cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
        yield from cur.execute("DROP TABLE IF EXISTS tbl2")
        yield from cur.execute("""CREATE TABLE tbl2
                                  (id int, name varchar(255))
                                  WITH OIDS""")
        yield from cur.execute("DROP FUNCTION IF EXISTS inc(val integer)")
        yield from cur.execute("""CREATE FUNCTION inc(val integer)
                                  RETURNS integer AS $$
                                  BEGIN
                                  RETURN val + 1;
                                  END; $$
                                  LANGUAGE PLPGSQL;""")
        return conn

    def test_description(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(None, cur.description)
            yield from cur.execute('SELECT * from tbl;')

            self.assertEqual(len(cur.description), 2,
                             'cursor.description describes too many columns')

            self.assertEqual(len(cur.description[0]), 7,
                             'cursor.description[x] tuples must have '
                             '7 elements')

            self.assertEqual(cur.description[0][0].lower(), 'id',
                             'cursor.description[x][0] must return column '
                             'name')

            self.assertEqual(cur.description[1][0].lower(), 'name',
                             'cursor.description[x][0] must return column '
                             'name')

            # Make sure self.description gets reset, cursor should be
            # set to None in case of none resulting queries like DDL
            yield from cur.execute('DROP TABLE IF EXISTS foobar;')
            self.assertEqual(None, cur.description)

        self.loop.run_until_complete(go())

    def test_raw(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(cur._impl, cur.raw)

        self.loop.run_until_complete(go())

    def test_close(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            cur.close()
            self.assertTrue(cur.closed)
            with self.assertRaises(psycopg2.InterfaceError):
                yield from cur.execute('SELECT 1')

        self.loop.run_until_complete(go())

    def test_close_twice(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            cur.close()
            cur.close()
            self.assertTrue(cur.closed)
            with self.assertRaises(psycopg2.InterfaceError):
                yield from cur.execute('SELECT 1')
            self.assertIsNone(conn._waiter)

        self.loop.run_until_complete(go())

    def test_connection(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertIs(cur.connection, conn)

        self.loop.run_until_complete(go())

    def test_name(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(None, cur.name)

        self.loop.run_until_complete(go())

    def test_scrollable(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(None, cur.scrollable)
            with self.assertRaises(psycopg2.ProgrammingError):
                cur.scrollable = True

        self.loop.run_until_complete(go())

    def test_withhold(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(False, cur.withhold)
            with self.assertRaises(psycopg2.ProgrammingError):
                cur.withhold = True

        self.loop.run_until_complete(go())

    def test_execute(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            ret = yield from cur.fetchone()
            self.assertEqual((1,), ret)

        self.loop.run_until_complete(go())

    def test_executemany(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from cur.executemany('SELECT %s', ['1', '2'])

        self.loop.run_until_complete(go())

    def test_mogrify(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            ret = yield from cur.mogrify('SELECT %s', ['1'])
            self.assertEqual(b"SELECT '1'", ret)

        self.loop.run_until_complete(go())

    def test_setinputsizes(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.setinputsizes(10)

        self.loop.run_until_complete(go())

    def test_fetchmany(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT * from tbl;')
            ret = yield from cur.fetchmany()
            self.assertEqual([(1, 'a')], ret)

            yield from cur.execute('SELECT * from tbl;')
            ret = yield from cur.fetchmany(2)
            self.assertEqual([(1, 'a'), (2, 'b')], ret)

        self.loop.run_until_complete(go())

    def test_fetchall(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT * from tbl;')
            ret = yield from cur.fetchall()
            self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], ret)

        self.loop.run_until_complete(go())

    def test_scroll(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT * from tbl;')
            yield from cur.scroll(1)
            ret = yield from cur.fetchone()
            self.assertEqual((2, 'b'), ret)

        self.loop.run_until_complete(go())

    def test_arraysize(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(1, cur.arraysize)

            cur.arraysize = 10
            self.assertEqual(10, cur.arraysize)

        self.loop.run_until_complete(go())

    def test_itersize(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(2000, cur.itersize)

            cur.itersize = 10
            self.assertEqual(10, cur.itersize)

        self.loop.run_until_complete(go())

    def test_rows(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT * from tbl')
            self.assertEqual(3, cur.rowcount)
            self.assertEqual(0, cur.rownumber)
            yield from cur.fetchone()
            self.assertEqual(1, cur.rownumber)

            self.assertEqual(0, cur.lastrowid)
            yield from cur.execute('INSERT INTO tbl2 VALUES (%s, %s)',
                                   (4, 'd'))
            self.assertNotEqual(0, cur.lastrowid)

        self.loop.run_until_complete(go())

    def test_query(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            self.assertEqual(b'SELECT 1', cur.query)

        self.loop.run_until_complete(go())

    def test_statusmessage(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            self.assertEqual('SELECT 1', cur.statusmessage)

        self.loop.run_until_complete(go())

    @unittest.expectedFailure
    def test_cast(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.cast(1, 2)
            self.assertEqual('SELECT 1', cur.statusmessage)

        self.loop.run_until_complete(go())

    def test_tzinfo_factory(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertIs(psycopg2.tz.FixedOffsetTimezone, cur.tzinfo_factory)

            cur.tzinfo_factory = psycopg2.tz.LocalTimezone
            self.assertIs(psycopg2.tz.LocalTimezone, cur.tzinfo_factory)

        self.loop.run_until_complete(go())

    def test_nextset(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            with self.assertRaises(psycopg2.NotSupportedError):
                yield from cur.nextset()

        self.loop.run_until_complete(go())

    def test_setoutputsize(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.setoutputsize(4, 1)

        self.loop.run_until_complete(go())

    def test_copy_family(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from cur.copy_from('file', 'table')
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from cur.copy_to('file', 'table')
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from cur.copy_expert('sql', 'table')

        self.loop.run_until_complete(go())

    def test_callproc(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.callproc('inc', [1])
            ret = yield from cur.fetchone()
            self.assertEqual((2,), ret)

            cur.close()
            with self.assertRaises(psycopg2.InterfaceError):
                yield from cur.callproc('inc', [1])
            self.assertIsNone(conn._waiter)

        self.loop.run_until_complete(go())

    def test_execute_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            conn = yield from self.connect()
            cur = yield from conn.cursor(timeout=timeout)
            self.assertEqual(timeout, cur.timeout)

            t1 = time.time()
            with self.assertRaises(asyncio.TimeoutError):
                yield from cur.execute("SELECT pg_sleep(1)")
            t2 = time.time()
            dt = t2 - t1
            self.assertTrue(0.09 <= dt <= 0.11, dt)

        self.loop.run_until_complete(go())

    def test_execute_override_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(TIMEOUT, cur.timeout)

            t1 = time.time()
            with self.assertRaises(asyncio.TimeoutError):
                yield from cur.execute("SELECT pg_sleep(1)", timeout=timeout)
            t2 = time.time()
            dt = t2 - t1
            self.assertTrue(0.09 <= dt <= 0.11, dt)

        self.loop.run_until_complete(go())

    def test_callproc_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            conn = yield from self.connect()
            cur = yield from conn.cursor(timeout=timeout)
            self.assertEqual(timeout, cur.timeout)

            t1 = time.time()
            with self.assertRaises(asyncio.TimeoutError):
                yield from cur.callproc("pg_sleep", [1])
            t2 = time.time()
            dt = t2 - t1
            self.assertTrue(0.09 <= dt <= 0.11, dt)

        self.loop.run_until_complete(go())

    def test_callproc_override_timeout(self):
        @asyncio.coroutine
        def go():
            timeout = 0.1
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(TIMEOUT, cur.timeout)

            t1 = time.time()
            with self.assertRaises(asyncio.TimeoutError):
                yield from cur.callproc("pg_sleep", [1], timeout=timeout)
            t2 = time.time()
            dt = t2 - t1
            self.assertTrue(0.09 <= dt <= 0.11, dt)

        self.loop.run_until_complete(go())

    def test_echo(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect(echo=True)
            cur = yield from conn.cursor()
            self.assertTrue(cur.echo)

        self.loop.run_until_complete(go())

    def test_echo_false(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertFalse(cur.echo)

        self.loop.run_until_complete(go())

    def test_iter(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            yield from cur.execute("SELECT * FROM tbl")
            data = [(1, 'a'), (2, 'b'), (3, 'c')]
            for item, tst in zip(cur, data):
                self.assertEqual(item, tst)

        self.loop.run_until_complete(go())

    def test_echo_callproc(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect(echo=True)
            cur = yield from conn.cursor()

            # TODO: check log records
            yield from cur.callproc('inc', [1])
            ret = yield from cur.fetchone()
            self.assertEqual((2,), ret)
            cur.close()

        self.loop.run_until_complete(go())
