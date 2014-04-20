import asyncio
from aiopg import connect, sa

import unittest

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl', meta,
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
    def connect(self, **kwargs):
        conn = yield from connect(database='aiopg',
                                  user='aiopg',
                                  password='passwd',
                                  host='127.0.0.1',
                                  loop=self.loop,
                                  **kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS sa_tbl")
        yield from cur.execute("CREATE TABLE sa_tbl "
                               "(id serial, name varchar(255))")
        yield from cur.execute("INSERT INTO sa_tbl (name)"
                               "VALUES ('first')")
        cur.close()
        return sa.SAConnection(conn, sa.dialect)
