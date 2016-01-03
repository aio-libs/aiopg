import asyncio
from aiopg import connect

import unittest
from unittest import mock

import pytest
sa = pytest.importorskip("aiopg.sa")  # noqa


from sqlalchemy import Table, Column, Integer, String

import psycopg2


meta = sa.AsyncMetaData()
tbl = Table('sa_tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestSAConnection(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.raw_connection = None

    def tearDown(self):
        self.raw_connection.close()
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from connect(database='aiopg',
                                  user='aiopg',
                                  password='passwd',
                                  host='127.0.0.1',
                                  loop=self.loop,
                                  **kwargs)
        self.raw_connection = conn
        engine = mock.Mock(from_spec=sa.engine.Engine)
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    def test_metadata_create_drop(self, **kwargs):
        @asyncio.coroutine
        def go():
            conn = yield from self.connect()

            yield from meta.drop_all(conn)
            with self.assertRaises(psycopg2.ProgrammingError):
                yield from conn.execute("SELECT * FROM sa_tbl")

            yield from meta.create_all(conn)
            res = yield from conn.execute("SELECT * FROM sa_tbl")
            self.assertEqual(0, len(list(res)))

        self.loop.run_until_complete(go())
