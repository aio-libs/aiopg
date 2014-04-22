import asyncio
from aiopg import sa

import unittest

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl3', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.engine = self.loop.run_until_complete(
            sa.create_engine(database='aiopg',
                             user='aiopg',
                             password='passwd',
                             host='127.0.0.1',
                             loop=self.loop))
        self.loop.run_until_complete(self.start())

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def start(self, **kwargs):
        with (yield from self.engine) as conn:
            yield from conn.execute("DROP TABLE IF EXISTS sa_tbl3")
            yield from conn.execute("CREATE TABLE sa_tbl3 "
                                    "(id serial, name varchar(255))")

    def test_dialect(self):
        self.assertEqual(sa.dialect, self.engine.dialect)

    def test_name(self):
        self.assertEqual('postgresql', self.engine.name)

    def test_driver(self):
        self.assertEqual('psycopg2', self.engine.driver)

    def test_dsn(self):
        self.assertEqual(
            'dbname=aiopg user=aiopg password=xxxxxx host=127.0.0.1',
            self.engine.dsn)
