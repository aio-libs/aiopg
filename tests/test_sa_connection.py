import asyncio
from aiopg import Cursor

from unittest import mock

import pytest
sa = pytest.importorskip("aiopg.sa")  # noqa


from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.schema import DropTable, CreateTable

import psycopg2


meta = MetaData()
tbl = Table('sa_tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


@pytest.yield_fixture
def connect(make_connection):
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_connection(**kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS sa_tbl")
        yield from cur.execute("CREATE TABLE sa_tbl "
                               "(id serial, name varchar(255))")
        yield from cur.execute("INSERT INTO sa_tbl (name)"
                               "VALUES ('first')")
        cur.close()

        engine = mock.Mock(from_spec=sa.engine.Engine)
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    yield go


@asyncio.coroutine
def test_execute_text_select(connect):
    conn = yield from connect()
    res = yield from conn.execute("SELECT * FROM sa_tbl;")
    assert isinstance(res.cursor, Cursor)
    assert ('id', 'name') == res.keys()
    rows = [r for r in res]
    assert res.closed
    assert res.cursor is None
    assert 1 == len(rows)
    row = rows[0]
    assert 1 == row[0]
    assert 1 == row['id']
    assert 1 == row.id
    assert 'first' == row[1]
    assert 'first' == row['name']
    assert 'first' == row.name


@asyncio.coroutine
def test_execute_sa_select(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.select())
    assert isinstance(res.cursor, Cursor)
    assert ('id', 'name') == res.keys()
    rows = [r for r in res]
    assert res.closed
    assert res.cursor is None
    assert res.returns_rows

    assert 1 == len(rows)
    row = rows[0]
    assert 1 == row[0]
    assert 1 == row['id']
    assert 1 == row.id
    assert 'first' == row[1]
    assert 'first' == row['name']
    assert 'first' == row.name


@asyncio.coroutine
def test_execute_sa_insert_with_dict(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), {"id": 2, "name": "second"})

    res = yield from conn.execute(tbl.select())
    rows = list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@asyncio.coroutine
def test_execute_sa_insert_with_tuple(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), (2, "second"))

    res = yield from conn.execute(tbl.select())
    rows = list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@asyncio.coroutine
def test_execute_sa_insert_named_params(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), id=2, name="second")

    res = yield from conn.execute(tbl.select())
    rows = list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@asyncio.coroutine
def test_execute_sa_insert_positional_params(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert(), 2, "second")

    res = yield from conn.execute(tbl.select())
    rows = list(res)
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@asyncio.coroutine
def test_scalar(connect):
    conn = yield from connect()
    res = yield from conn.scalar(tbl.count())
    assert 1, res


@asyncio.coroutine
def test_scalar_None(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.delete())
    res = yield from conn.scalar(tbl.select())
    assert res is None


@asyncio.coroutine
def test_row_proxy(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.select())
    rows = [r for r in res]
    row = rows[0]
    row2 = yield from (yield from conn.execute(tbl.select())).first()
    assert 2 == len(row)
    assert ['id', 'name'] == list(row)
    assert 'id' in row
    assert 'unknown' not in row
    assert 'first' == row.name
    assert 'first' == row[tbl.c.name]
    with pytest.raises(AttributeError):
        row.unknown
    assert "(1, 'first')" == repr(row)
    assert (1, 'first') == row.as_tuple()
    assert (555, 'other') != row.as_tuple()
    assert row2 == row
    assert not (row2 != row)
    assert 5 != row


@asyncio.coroutine
def test_insert(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.insert().values(name='second'))
    assert ('id',) == res.keys()
    assert 1 == res.rowcount
    assert res.returns_rows

    rows = [r for r in res]
    assert 1 == len(rows)
    assert 2 == rows[0].id


@asyncio.coroutine
def test_raw_insert(connect):
    conn = yield from connect()
    yield from conn.execute(
        "INSERT INTO sa_tbl (name) VALUES ('third')")
    res = yield from conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = [r for r in res]
    assert 2 == len(rows)
    assert 2 == rows[1].id


@asyncio.coroutine
def test_raw_insert_with_params(connect):
    conn = yield from connect()
    res = yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%s, %s)",
        2, 'third')
    res = yield from conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = [r for r in res]
    assert 2 == len(rows)
    assert 2 == rows[1].id


@asyncio.coroutine
def test_raw_insert_with_params_dict(connect):
    conn = yield from connect()
    res = yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        {'id': 2, 'name': 'third'})
    res = yield from conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = [r for r in res]
    assert 2 == len(rows)
    assert 2 == rows[1].id


@asyncio.coroutine
def test_raw_insert_with_named_params(connect):
    conn = yield from connect()
    res = yield from conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        id=2, name='third')
    res = yield from conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = [r for r in res]
    assert 2 == len(rows)
    assert 2 == rows[1].id


@asyncio.coroutine
def test_raw_insert_with_executemany(connect):
    conn = yield from connect()
    with pytest.raises(sa.ArgumentError):
        yield from conn.execute(
            "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
            [(2, 'third'), (3, 'forth')])


@asyncio.coroutine
def test_delete(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete().where(tbl.c.id == 1))
    assert () == res.keys()
    assert 1 == res.rowcount
    assert not res.returns_rows
    assert res.closed
    assert res.cursor is None


@asyncio.coroutine
def test_double_close(connect):
    conn = yield from connect()
    res = yield from conn.execute("SELECT 1")
    res.close()
    assert res.closed
    assert res.cursor is None
    res.close()
    assert res.closed
    assert res.cursor is None


@asyncio.coroutine
def test_fetchall(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchall()
    assert 2 == len(rows)
    assert res.closed
    assert res.returns_rows
    assert [(1, 'first') == (2, 'second')], rows


@asyncio.coroutine
def test_fetchall_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchall()


@asyncio.coroutine
def test_fetchall_not_returns_rows(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchall()


@asyncio.coroutine
def test_fetchone_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchone()


@asyncio.coroutine
def test_first_not_returns_rows(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        yield from res.first()


@asyncio.coroutine
def test_fetchmany(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first')] == rows


@asyncio.coroutine
def test_fetchmany_with_size(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchmany(100)
    assert 2 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first') == (2, 'second')], rows


@asyncio.coroutine
def test_fetchmany_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()


@asyncio.coroutine
def test_fetchmany_with_size_closed(connect):
    conn = yield from connect()
    yield from conn.execute(tbl.insert().values(name='second'))

    res = yield from conn.execute(tbl.select())
    res.close()
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany(5555)


@asyncio.coroutine
def test_fetchmany_not_returns_rows(connect):
    conn = yield from connect()
    res = yield from conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()


@asyncio.coroutine
def test_fetchmany_close_after_last_read(connect):
    conn = yield from connect()

    res = yield from conn.execute(tbl.select())
    rows = yield from res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first')] == rows
    rows2 = yield from res.fetchmany()
    assert 0 == len(rows2)
    assert res.closed


@asyncio.coroutine
def test_create_table(connect):
    conn = yield from connect()
    res = yield from conn.execute(DropTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from conn.execute("SELECT * FROM sa_tbl")

    res = yield from conn.execute(CreateTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        yield from res.fetchmany()

    res = yield from conn.execute("SELECT * FROM sa_tbl")
    assert 0 == len(list(res))
