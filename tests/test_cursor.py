import asyncio
import aiopg
import psycopg2
import unittest


class TestCursor(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        self.loop = None

    @asyncio.coroutine
    def connect(self):
        return (yield from aiopg.connect(database='aiopg',
                                         user='aiopg',
                                         password='passwd',
                                         host='127.0.0.1',
                                         loop=self.loop))

    def test_description(self):

        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            cur = yield from conn.cursor()
            self.assertEqual(None, cur.description)

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
            #with self.assertRaises(psycopg2.ProgrammingError):
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
            yield from cur.execute('SELECT * from tbl;')
            self.assertEqual(3, cur.rowcount)
            self.assertEqual(0, cur.rownumber)
            yield from cur.fetchone()
            self.assertEqual(1, cur.rownumber)
            self.assertEqual(1, cur.lastrowid)

            self.assertEqual(0, cur.lastrowid)

        self.loop.run_until_complete(go())
