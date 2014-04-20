import asyncio
from aiopg import connect, sa, Cursor

import unittest
import sqlalchemy

from sqlalchemy import MetaData, Table, Column, Integer, String


meta = MetaData()
tbl = Table('tbl', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(255)))


class TestSACnnection(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from connect(database='aiopg',
                                     user='aiopg',
                                     password='passwd',
                                     host='127.0.0.1',
                                     loop=self.loop,
                                     **kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("CREATE TABLE tbl "
                               "(id serial, name varchar(255))")
        yield from cur.execute("INSERT INTO tbl (name)"
                               "VALUES ('first')")
        cur.close()
        return conn


    def test_execute_text_select(self):
        @asyncio.coroutine
        def go():
            conn_impl = yield from self.connect()
            conn = sa.SAConnection(conn_impl, sa.dialect)
            res = yield from conn.execute("SELECT * FROM tbl;")
            self.assertIsInstance(res.cursor, Cursor)
            rows = [r for r in res]
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)
            self.assertEqual(1, len(rows))
            row = rows[0]
            self.assertEqual(1, row[0])
            self.assertEqual(1, row['id'])
            self.assertEqual(1, row.id)
            self.assertEqual('first', row[1])
            self.assertEqual('first', row['name'])
            self.assertEqual('first', row.name)

        self.loop.run_until_complete(go())

    def test_execute_sa_select(self):
        @asyncio.coroutine
        def go():
            conn_impl = yield from self.connect()
            conn = sa.SAConnection(conn_impl, sa.dialect)
            res = yield from conn.execute(tbl.select())
            self.assertIsInstance(res.cursor, Cursor)
            rows = [r for r in res]
            self.assertTrue(res.closed)
            self.assertIsNone(res.cursor)

            self.assertEqual(1, len(rows))
            row = rows[0]
            self.assertEqual(1, row[0])
            self.assertEqual(1, row['id'])
            self.assertEqual(1, row.id)
            self.assertEqual('first', row[1])
            self.assertEqual('first', row['name'])
            self.assertEqual('first', row.name)

        self.loop.run_until_complete(go())

    def test_scalar(self):
        @asyncio.coroutine
        def go():
            conn_impl = yield from self.connect()
            conn = sa.SAConnection(conn_impl, sa.dialect)
            res = yield from conn.scalar(tbl.count())
            self.assertEqual(1, res)

        self.loop.run_until_complete(go())
