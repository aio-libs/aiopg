import asyncio
import unittest

import psycopg2
from aiopg import sa

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable, DropTable


meta = MetaData()


def get_default():
    return 2

tbl = Table('sa_tbl_defaults', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('default_value', Integer, nullable=False, default=1),
            Column('default_callable', Integer, nullable=False,
                   default=get_default)
            )


class TestSATypes(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @asyncio.coroutine
    def connect(self, **kwargs):
        engine = yield from sa.create_engine(database='aiopg',
                                             user='aiopg',
                                             password='passwd',
                                             host='127.0.0.1',
                                             loop=self.loop,
                                             **kwargs)
        with (yield from engine) as conn:
            try:
                yield from conn.execute(DropTable(tbl))
            except psycopg2.ProgrammingError:
                pass
            yield from conn.execute(CreateTable(tbl))
        return engine

    def test_defaults(self):
        @asyncio.coroutine
        def go():
            engine = yield from self.connect()

            with (yield from engine) as conn:
                yield from conn.execute(tbl.insert().values())

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(1, item['default_value'])
                self.assertEqual(2, item['default_callable'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())

    def test_defaults_override(self):
        @asyncio.coroutine
        def get_column_default(conn, dialect, column):
            default = column.default
            if default.is_sequence:
                sql = "select nextval('%s')" % \
                    dialect.identifier_preparer.format_sequence(default)
                return (yield from conn.scalar(sql))
            elif default.is_clause_element:
                return (yield from conn.scalar(default.arg))
            elif default.is_callable:
                return default.arg(dialect)
            else:
                return default.arg

        @asyncio.coroutine
        def get_table_defaults(conn, dialect, table):
            result = {}
            for column in table.c:
                if column.default is not None:
                    val = yield from get_column_default(conn, dialect, column)
                    result[column.name] = val
            return result

        @asyncio.coroutine
        def insert(conn, dialect, table, returning_id=False, **kwargs):
            values = yield from get_table_defaults(conn, dialect, table)
            values.update(kwargs)
            if returning_id:
                insertion = table.insert(returning=[table.id])
            else:
                insertion = table.insert()
            return insertion.values(**values)

        @asyncio.coroutine
        def go():
            engine = yield from self.connect()

            with (yield from engine) as conn:
                ins = yield from insert(conn, engine.dialect, tbl)
                yield from conn.execute(ins.values())

                ret = yield from conn.execute(tbl.select())
                item = yield from ret.fetchone()
                self.assertEqual(1, item['default_value'])
                self.assertEqual(2, item['default_callable'])
            engine.close()
            yield from engine.wait_closed()

        self.loop.run_until_complete(go())
