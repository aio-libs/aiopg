import asyncio
from aiopg import connect, sa

import unittest

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl2', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestTransaction(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def start(self, **kwargs):
        conn = yield from self.connect(**kwargs)
        yield from conn.execute("DROP TABLE IF EXISTS sa_tbl2")
        yield from conn.execute("CREATE TABLE sa_tbl2 "
                                "(id serial, name varchar(255))")
        yield from conn.execute("INSERT INTO sa_tbl2 (name)"
                                "VALUES ('first')")
        return (yield from self.connect(**kwargs))

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from connect(database='aiopg',
                                  user='aiopg',
                                  password='passwd',
                                  host='127.0.0.1',
                                  loop=self.loop,
                                  **kwargs)
        return sa.SAConnection(conn, sa.dialect)

    def test_without_transactions(self):
        @asyncio.coroutine
        def go():
            yield from self.start()

            conn1 = yield from self.connect()
            conn2 = yield from self.connect()
            res1 = yield from conn1.scalar(tbl.count())
            self.assertEqual(1, res1)

            yield from conn2.execute(tbl.delete())

            res2 = yield from conn1.scalar(tbl.count())
            self.assertEqual(0, res2)

        self.loop.run_until_complete(go())

    def test_connection_attr(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            tr = yield from conn.begin()
            self.assertIs(tr.connection, conn)

        self.loop.run_until_complete(go())

    def test_root_transaction(self):
        @asyncio.coroutine
        def go():
            yield from self.start()

            conn1 = yield from self.connect()
            conn2 = yield from self.connect()

            tr = yield from conn1.begin()
            self.assertTrue(tr.is_active)
            yield from conn1.execute(tbl.delete())

            res1 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            yield from tr.commit()

            self.assertFalse(tr.is_active)
            res2 = yield from conn2.scalar(tbl.count())
            self.assertEqual(0, res2)

        self.loop.run_until_complete(go())

    def test_root_transaction_rollback(self):
        @asyncio.coroutine
        def go():
            yield from self.start()

            conn1 = yield from self.connect()
            conn2 = yield from self.connect()

            tr = yield from conn1.begin()
            self.assertTrue(tr.is_active)
            yield from conn1.execute(tbl.delete())

            res1 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            yield from tr.rollback()

            self.assertFalse(tr.is_active)
            res2 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res2)

        self.loop.run_until_complete(go())

    def test_root_transaction_close(self):
        @asyncio.coroutine
        def go():
            yield from self.start()

            conn1 = yield from self.connect()
            conn2 = yield from self.connect()

            tr = yield from conn1.begin()
            self.assertTrue(tr.is_active)
            yield from conn1.execute(tbl.delete())

            res1 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            yield from tr.close()

            self.assertFalse(tr.is_active)
            res2 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res2)

        self.loop.run_until_complete(go())

    def test_rollback_on_connection_close(self):
        @asyncio.coroutine
        def go():
            yield from self.start()

            conn1 = yield from self.connect()
            conn2 = yield from self.connect()

            tr = yield from conn1.begin()
            yield from conn1.execute(tbl.delete())

            res1 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res1)

            yield from conn1.close()

            res2 = yield from conn2.scalar(tbl.count())
            self.assertEqual(1, res2)

        self.loop.run_until_complete(go())

    def test_root_transaction_commit_inactive(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            tr = yield from conn.begin()
            self.assertTrue(tr.is_active)
            yield from tr.commit()
            self.assertFalse(tr.is_active)
            with self.assertRaises(sa.InvalidRequestError):
                yield from tr.commit()

        self.loop.run_until_complete(go())

    def test_root_transaction_rollback_inactive(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            tr = yield from conn.begin()
            self.assertTrue(tr.is_active)
            yield from tr.rollback()
            self.assertFalse(tr.is_active)
            yield from tr.rollback()
            self.assertFalse(tr.is_active)

        self.loop.run_until_complete(go())

    def test_root_transaction_double_close(self):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()
            tr = yield from conn.begin()
            self.assertTrue(tr.is_active)
            yield from tr.close()
            self.assertFalse(tr.is_active)
            yield from tr.close()
            self.assertFalse(tr.is_active)

        self.loop.run_until_complete(go())
