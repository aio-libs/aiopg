import asyncio

import psycopg2
import pytest
from aiopg import IsolationLevel, Transaction
from aiopg.transaction import IsolationCompiler


@pytest.fixture
def engine(make_engine, loop):
    @asyncio.coroutine
    def start():
        engine = yield from make_engine()
        with (yield from engine) as cur:
            yield from cur.execute("DROP TABLE IF EXISTS tbl")
            yield from cur.execute("CREATE TABLE tbl (id int, "
                                   "name varchar(255))")

            yield from cur.execute("insert into tbl values(22, 'read only')")
        return engine

    return loop.run_until_complete(start())


@pytest.mark.parametrize('isolation_level,readonly,deferrable', [
    (IsolationLevel.read_committed, False, False),
    (IsolationLevel.repeatable_read, False, False),
    (IsolationLevel.serializable, False, False),
    (IsolationLevel.serializable, False, True),
])
@asyncio.coroutine
def test_transaction_oldstyle(engine, isolation_level, readonly, deferrable):
    with (yield from engine) as cur:
        tr = Transaction(cur, isolation_level,
                         readonly=readonly, deferrable=deferrable)
        yield from tr.begin()
        yield from cur.execute("insert into tbl values(1, 'data')")
        resp = yield from cur.execute('select * from tbl where id = 1')
        row = yield from resp.fetchone()

        assert row.id == 1
        assert row.name == 'data'

        yield from tr.commit()


@asyncio.coroutine
def two_begin(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.begin()
    yield from tr.begin()


@asyncio.coroutine
def two_commit(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.begin()
    yield from tr.commit()
    yield from tr.commit()


@asyncio.coroutine
def two_rollback(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.begin()
    yield from tr.rollback()
    yield from tr.rollback()


@asyncio.coroutine
def e_rollback_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.rollback_savepoint()


@asyncio.coroutine
def e_release_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.release_savepoint()


@asyncio.coroutine
def two_rollback_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.begin()
    yield from tr.release_savepoint()
    yield from tr.commit()


@asyncio.coroutine
def e_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.savepoint()


@asyncio.coroutine
def e_commit_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    yield from tr.begin()
    yield from tr.savepoint()
    yield from tr.savepoint()
    yield from tr.commit()


@pytest.mark.parametrize('fn', [
    two_begin, two_commit, two_rollback,
    e_rollback_savepoint, e_release_savepoint, e_savepoint,
    e_commit_savepoint, two_rollback_savepoint
])
@asyncio.coroutine
def test_transaction_fail_oldstyle(engine, fn):
    with pytest.raises(psycopg2.ProgrammingError):
        with (yield from engine) as cur:
            yield from fn(cur)


def test_transaction_value_error():
    with pytest.raises(ValueError):
        Transaction(None, IsolationLevel.read_committed, readonly=True)


def test_transaction_isolation_implemented():
    class IsolationCompilerTest(IsolationCompiler):
        def begin(self):
            return super().begin()

    tr = IsolationCompilerTest(False, False)

    with pytest.raises(NotImplementedError):
        tr.begin()


@asyncio.coroutine
def test_transaction_finalization_warning(engine, monkeypatch):
    with (yield from engine) as cur:
        tr = Transaction(cur, IsolationLevel.read_committed)

        def valid(x, _):
            assert x in [
                'You have not closed transaction {!r}'.format(tr),
                'You have not closed savepoint {!r}'.format(tr)
            ]

        monkeypatch.setattr('aiopg.transaction.warnings.warn', valid)
        yield from tr.begin()
        yield from tr.savepoint()


@asyncio.coroutine
def test_transaction_readonly_insert_oldstyle(engine):
    with (yield from engine) as cur:
        tr = Transaction(cur, IsolationLevel.serializable,
                         readonly=True)

        yield from tr.begin()
        with pytest.raises(psycopg2.InternalError):
            yield from cur.execute("insert into tbl values(1, 'data')")
            yield from tr.rollback()


@asyncio.coroutine
def test_transaction_readonly_oldstyle(engine):
    with (yield from engine) as cur:
        tr = Transaction(cur, IsolationLevel.serializable, readonly=True)

        yield from tr.begin()
        resp = yield from cur.execute('select * from tbl where id = 22')
        row = yield from resp.fetchone()

        assert row.id == 22
        assert row.name == 'read only'
        yield from tr.commit()


@asyncio.coroutine
def test_transaction_point_oldstyle(engine):
    with (yield from engine) as cur:
        tr = Transaction(cur, IsolationLevel.read_committed)
        yield from tr.begin()

        yield from cur.execute("insert into tbl values(1, 'data')")

        try:
            yield from tr.savepoint()
            yield from cur.execute("insert into tbl values(1/0, 'no data')")
        except psycopg2.DataError:
            yield from tr.rollback_savepoint()

        yield from tr.savepoint()
        yield from cur.execute("insert into tbl values(2, 'data')")
        yield from tr.release_savepoint()

        yield from cur.execute("insert into tbl values(3, 'data')")

        resp = yield from cur.execute('select * from tbl')
        row = yield from resp.fetchall()
        assert row == [(22, 'read only'), (1, 'data'), (2, 'data'),
                       (3, 'data')]

        yield from tr.commit()
