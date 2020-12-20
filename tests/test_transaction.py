import psycopg2
import pytest

from aiopg import IsolationLevel, Transaction


@pytest.fixture
def engine(make_engine, loop):
    async def start():
        engine = await make_engine()
        async with engine.acquire() as cur:
            await cur.execute("DROP TABLE IF EXISTS tbl")
            await cur.execute("CREATE TABLE tbl (id int, "
                              "name varchar(255))")

            await cur.execute("insert into tbl values(22, 'read only')")
        return engine

    return loop.run_until_complete(start())


@pytest.mark.parametrize('isolation_level,readonly,deferrable', [
    (IsolationLevel.read_committed, False, False),
    (IsolationLevel.repeatable_read, False, False),
    (IsolationLevel.serializable, False, False),
    (IsolationLevel.serializable, False, True),
])
async def test_transaction_oldstyle(engine, isolation_level, readonly,
                                    deferrable):
    async with engine.acquire() as cur:
        tr = Transaction(cur, isolation_level,
                         readonly=readonly, deferrable=deferrable)
        await tr.begin()
        await cur.execute("insert into tbl values(1, 'data')")
        resp = await cur.execute('select * from tbl where id = 1')
        row = await resp.fetchone()

        assert row.id == 1
        assert row.name == 'data'

        await tr.commit()


async def two_begin(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    try:
        await tr.begin()
    except psycopg2.ProgrammingError as e:
        await tr.rollback()
        raise e


async def two_commit(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    await tr.commit()
    await tr.commit()


async def two_rollback(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    await tr.rollback()
    await tr.rollback()


async def e_rollback_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.rollback_savepoint()


async def e_release_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.release_savepoint()


async def two_rollback_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    try:
        await tr.release_savepoint()
    except psycopg2.ProgrammingError as e:
        await tr.commit()
        raise e


async def e_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.savepoint()


async def e_commit_savepoint(cur):
    tr = Transaction(cur, IsolationLevel.read_committed)
    await tr.begin()
    await tr.savepoint()
    try:
        await tr.savepoint()
    except psycopg2.ProgrammingError as e:
        await tr.rollback_savepoint()
        await tr.commit()
        raise e


@pytest.mark.parametrize('fn', [
    two_begin, two_commit, two_rollback,
    e_rollback_savepoint, e_release_savepoint, e_savepoint,
    e_commit_savepoint, two_rollback_savepoint
])
async def test_transaction_fail_oldstyle(engine, fn):
    with pytest.raises(psycopg2.ProgrammingError):
        async with engine.acquire() as cur:
            await fn(cur)


def test_transaction_value_error():
    with pytest.raises(ValueError):
        Transaction(None, IsolationLevel.read_committed, readonly=True)


async def test_transaction_finalization_warning(engine, monkeypatch):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.read_committed)

        def valid(x, _):
            assert x in [
                'Invalid transaction status on released connection: 2',
                'You have not closed transaction {!r}'.format(tr),
                'You have not closed savepoint {!r}'.format(tr)
            ]

        monkeypatch.setattr('aiopg.transaction.warnings.warn', valid)
        await tr.begin()
        await tr.savepoint()


async def test_transaction_readonly_insert_oldstyle(engine):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.serializable,
                         readonly=True)

        await tr.begin()
        with pytest.raises(psycopg2.InternalError):
            await cur.execute("insert into tbl values(1, 'data')")
        await tr.rollback()


async def test_transaction_readonly_oldstyle(engine):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.serializable, readonly=True)

        await tr.begin()
        resp = await cur.execute('select * from tbl where id = 22')
        row = await resp.fetchone()

        assert row.id == 22
        assert row.name == 'read only'
        await tr.commit()


async def test_transaction_point_oldstyle(engine):
    async with engine.acquire() as cur:
        tr = Transaction(cur, IsolationLevel.read_committed)
        await tr.begin()

        await cur.execute("insert into tbl values(1, 'data')")

        try:
            await tr.savepoint()
            await cur.execute("insert into tbl values(1/0, 'no data')")
        except psycopg2.DataError:
            await tr.rollback_savepoint()

        await tr.savepoint()
        await cur.execute("insert into tbl values(2, 'data')")
        await tr.release_savepoint()

        await cur.execute("insert into tbl values(3, 'data')")

        resp = await cur.execute('select * from tbl')
        row = await resp.fetchall()
        assert row == [(22, 'read only'), (1, 'data'), (2, 'data'),
                       (3, 'data')]

        await tr.commit()
