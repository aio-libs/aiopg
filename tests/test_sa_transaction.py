import asyncio
from unittest import mock

import pytest
sa = pytest.importorskip("aiopg.sa")  # noqa

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl2', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


@pytest.fixture
def connect(make_connection):
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_connection(**kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS sa_tbl2")
        yield from cur.execute("CREATE TABLE sa_tbl2 "
                               "(id serial, name varchar(255))")
        yield from cur.execute("INSERT INTO sa_tbl2 (name)"
                               "VALUES ('first')")
        cur.close()

        engine = mock.Mock(from_spec=sa.engine.Engine)
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)

    yield go


@pytest.fixture
def xa_connect(connect):
    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from connect(**kwargs)
        val = yield from conn.scalar('show max_prepared_transactions')
        if not int(val):
            raise pytest.skip('Twophase transacions are not supported. '
                              'Set max_prepared_transactions to '
                              'a nonzero value')
        return conn

    yield go


@asyncio.coroutine
def test_without_transactions(connect):
    conn1 = yield from connect()
    conn2 = yield from connect()
    res1 = yield from conn1.scalar(tbl.count())
    assert 1 == res1

    yield from conn2.execute(tbl.delete())

    res2 = yield from conn1.scalar(tbl.count())
    assert 0 == res2


@asyncio.coroutine
def test_connection_attr(connect):
    conn = yield from connect()
    tr = yield from conn.begin()
    assert tr.connection is conn


@asyncio.coroutine
def test_root_transaction(connect):
    conn1 = yield from connect()
    conn2 = yield from connect()

    tr = yield from conn1.begin()
    assert tr.is_active
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from tr.commit()

    assert not tr.is_active
    assert not conn1.in_transaction
    res2 = yield from conn2.scalar(tbl.count())
    assert 0 == res2


@asyncio.coroutine
def test_root_transaction_rollback(connect):
    conn1 = yield from connect()
    conn2 = yield from connect()

    tr = yield from conn1.begin()
    assert tr.is_active
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from tr.rollback()

    assert not tr.is_active
    res2 = yield from conn2.scalar(tbl.count())
    assert 1 == res2


@asyncio.coroutine
def test_root_transaction_close(connect):
    conn1 = yield from connect()
    conn2 = yield from connect()

    tr = yield from conn1.begin()
    assert tr.is_active
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from tr.close()

    assert not tr.is_active
    res2 = yield from conn2.scalar(tbl.count())
    assert 1 == res2


@asyncio.coroutine
def test_root_transaction_commit_inactive(connect):
    conn = yield from connect()
    tr = yield from conn.begin()
    assert tr.is_active
    yield from tr.commit()
    assert not tr.is_active
    with pytest.raises(sa.InvalidRequestError):
        yield from tr.commit()


@asyncio.coroutine
def test_root_transaction_rollback_inactive(connect):
    conn = yield from connect()
    tr = yield from conn.begin()
    assert tr.is_active
    yield from tr.rollback()
    assert not tr.is_active
    yield from tr.rollback()
    assert not tr.is_active


@asyncio.coroutine
def test_root_transaction_double_close(connect):
    conn = yield from connect()
    tr = yield from conn.begin()
    assert tr.is_active
    yield from tr.close()
    assert not tr.is_active
    yield from tr.close()
    assert not tr.is_active


@asyncio.coroutine
def test_inner_transaction_commit(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin()
    tr2 = yield from conn.begin()
    assert tr2.is_active

    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active


@asyncio.coroutine
def test_rollback_on_connection_close(connect):
    conn1 = yield from connect()
    conn2 = yield from connect()

    tr = yield from conn1.begin()
    yield from conn1.execute(tbl.delete())

    res1 = yield from conn2.scalar(tbl.count())
    assert 1 == res1

    yield from conn1.close()

    res2 = yield from conn2.scalar(tbl.count())
    assert 1 == res2
    del tr


@asyncio.coroutine
def test_inner_transaction_rollback(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin()
    tr2 = yield from conn.begin()
    assert tr2.is_active
    yield from conn.execute(tbl.insert().values(name='aaaa'))

    yield from tr2.rollback()
    assert not tr2.is_active
    assert not tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 1 == res


@asyncio.coroutine
def test_inner_transaction_close(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin()
    tr2 = yield from conn.begin()
    assert tr2.is_active
    yield from conn.execute(tbl.insert().values(name='aaaa'))

    yield from tr2.close()
    assert not tr2.is_active
    assert tr1.is_active
    yield from tr1.commit()

    res = yield from conn.scalar(tbl.count())
    assert 2 == res


@asyncio.coroutine
def test_nested_transaction_commit(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()
    assert tr1.is_active
    assert tr2.is_active

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res

    yield from tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res


@asyncio.coroutine
def test_nested_transaction_commit_twice(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr2.commit()
    assert not tr2.is_active
    assert tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res

    yield from tr1.close()


@asyncio.coroutine
def test_nested_transaction_rollback(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()
    assert tr1.is_active
    assert tr2.is_active

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 1 == res

    yield from tr1.commit()
    assert not tr2.is_active
    assert not tr1.is_active

    res = yield from conn.scalar(tbl.count())
    assert 1 == res


@asyncio.coroutine
def test_nested_transaction_rollback_twice(connect):
    conn = yield from connect()
    tr1 = yield from conn.begin_nested()
    tr2 = yield from conn.begin_nested()

    yield from conn.execute(tbl.insert().values(name='aaaa'))
    yield from tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr2.rollback()
    assert not tr2.is_active
    assert tr1.is_active

    yield from tr1.commit()
    res = yield from conn.scalar(tbl.count())
    assert 1 == res


@asyncio.coroutine
def test_twophase_transaction_commit(xa_connect):
    conn = yield from xa_connect()
    tr = yield from conn.begin_twophase()
    yield from conn.execute(tbl.insert().values(name='aaaa'))

    yield from tr.prepare()
    assert tr.is_active

    yield from tr.commit()
    assert not tr.is_active

    res = yield from conn.scalar(tbl.count())
    assert 2 == res


@asyncio.coroutine
def test_twophase_transaction_twice(xa_connect):
    conn = yield from xa_connect()
    tr = yield from conn.begin_twophase()
    with pytest.raises(sa.InvalidRequestError):
        yield from conn.begin_twophase()

    assert tr.is_active
    yield from tr.prepare()
    yield from tr.commit()


@asyncio.coroutine
def test_transactions_sequence(xa_connect):
    conn = yield from xa_connect()

    yield from conn.execute(tbl.delete())

    assert conn._transaction is None

    tr1 = yield from conn.begin()
    assert tr1 is conn._transaction
    yield from conn.execute(tbl.insert().values(name='a'))
    res1 = yield from conn.scalar(tbl.count())
    assert 1 == res1

    yield from tr1.commit()
    assert conn._transaction is None

    tr2 = yield from conn.begin()
    assert tr2 is conn._transaction
    yield from conn.execute(tbl.insert().values(name='b'))
    res2 = yield from conn.scalar(tbl.count())
    assert 2 == res2

    yield from tr2.rollback()
    assert conn._transaction is None

    tr3 = yield from conn.begin()
    assert tr3 is conn._transaction
    yield from conn.execute(tbl.insert().values(name='b'))
    res3 = yield from conn.scalar(tbl.count())
    assert 2 == res3

    yield from tr3.commit()
    assert conn._transaction is None
