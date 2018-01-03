import asyncio
from aiopg.connection import TIMEOUT
from psycopg2.extensions import parse_dsn

import pytest
sa = pytest.importorskip("aiopg.sa")  # noqa

from sqlalchemy import MetaData, Table, Column, Integer, String

meta = MetaData()
tbl = Table('sa_tbl3', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


@pytest.fixture
def engine(make_engine, loop):
    @asyncio.coroutine
    def start():
        engine = yield from make_engine()
        with (yield from engine) as conn:
            yield from conn.execute("DROP TABLE IF EXISTS sa_tbl3")
            yield from conn.execute("CREATE TABLE sa_tbl3 "
                                    "(id serial, name varchar(255))")
        return engine
    return loop.run_until_complete(start())


def test_dialect(engine):
    assert sa.engine._dialect is engine.dialect


def test_name(engine):
    assert 'postgresql' == engine.name


def test_driver(engine):
    assert 'psycopg2' == engine.driver


def test_dsn(engine, pg_params):
    params = pg_params.copy()
    params['password'] = 'xxx'
    params['dbname'] = params.pop('database')
    params['port'] = str(params['port'])
    assert parse_dsn(engine.dsn) == params


def test_minsize(engine):
    assert 1 == engine.minsize


def test_maxsize(engine):
    assert 10 == engine.maxsize


def test_size(engine):
    assert 1 == engine.size


def test_freesize(engine):
    assert 1 == engine.freesize


@asyncio.coroutine
def test_make_engine_with_default_loop(make_engine, loop):
    asyncio.set_event_loop(loop)
    engine = yield from make_engine(use_loop=False)
    engine.close()
    yield from engine.wait_closed()


def test_not_context_manager(engine):
    with pytest.raises(RuntimeError):
        with engine:
            pass


@asyncio.coroutine
def test_release_transacted(engine):
    conn = yield from engine.acquire()
    tr = yield from conn.begin()
    with pytest.raises(sa.InvalidRequestError):
        engine.release(conn)
    del tr
    yield from conn.close()


def test_timeout(engine):
    assert TIMEOUT == engine.timeout


@asyncio.coroutine
def test_timeout_override(make_engine):
    timeout = 1
    engine = yield from make_engine(timeout=timeout)
    assert timeout == engine.timeout
    conn = yield from engine.acquire()
    with pytest.raises(asyncio.TimeoutError):
        yield from conn.execute("SELECT pg_sleep(10)")

    engine.terminate()
    yield from engine.wait_closed()


@asyncio.coroutine
def test_cannot_acquire_after_closing(make_engine):
    engine = yield from make_engine()
    engine.close()

    with pytest.raises(RuntimeError):
        yield from engine.acquire()

    yield from engine.wait_closed()


@asyncio.coroutine
def test_wait_closed(make_engine, loop):
    engine = yield from make_engine(minsize=10)

    c1 = yield from engine.acquire()
    c2 = yield from engine.acquire()
    assert 10 == engine.size
    assert 8 == engine.freesize

    ops = []

    @asyncio.coroutine
    def do_release(conn):
        yield from asyncio.sleep(0, loop=loop)
        engine.release(conn)
        ops.append('release')

    @asyncio.coroutine
    def wait_closed():
        yield from engine.wait_closed()
        ops.append('wait_closed')

    engine.close()
    yield from asyncio.gather(wait_closed(),
                              do_release(c1),
                              do_release(c2),
                              loop=loop)
    assert ['release', 'release', 'wait_closed'] == ops
    assert 0 == engine.freesize


@asyncio.coroutine
def test_terminate_with_acquired_connections(make_engine):
    engine = yield from make_engine()
    conn = yield from engine.acquire()
    engine.terminate()
    yield from engine.wait_closed()

    assert conn.closed
