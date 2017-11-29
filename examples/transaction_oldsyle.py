import asyncio

import aiopg
import psycopg2
from aiopg.transaction import Transaction, IsolationLevel

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


@asyncio.coroutine
def transaction(cur, isolation_level,
                readonly=False, deferrable=False):
    transaction = Transaction(cur, isolation_level, readonly, deferrable)

    yield from transaction.begin()
    try:
        yield from cur.execute('insert into tbl values (1)')

        yield from transaction.savepoint()
        try:
            yield from cur.execute('insert into tbl values (3)')
            yield from transaction.release_savepoint()
        except psycopg2.Error:
            yield from transaction.rollback_savepoint()

        yield from cur.execute('insert into tbl values (4)')
        yield from transaction.commit()

    except psycopg2.Error:
        yield from transaction.rollback()


@asyncio.coroutine
def main():
    pool = yield from aiopg.create_pool(dsn)
    with (yield from pool.cursor()) as cur:
        yield from transaction(cur, IsolationLevel.repeatable_read)
        yield from transaction(cur, IsolationLevel.read_committed)
        yield from transaction(cur, IsolationLevel.serializable)

        cur.execute('select * from tbl')


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
