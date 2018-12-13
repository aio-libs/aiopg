import asyncio

import aiopg
import psycopg2
from aiopg.transaction import Transaction, IsolationLevel

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


async def transaction(cur, isolation_level,
                      readonly=False, deferrable=False):
    transaction = Transaction(cur, isolation_level, readonly, deferrable)

    await transaction.begin()
    try:
        await cur.execute('insert into tbl values (1)')

        await transaction.savepoint()
        try:
            await cur.execute('insert into tbl values (3)')
            await transaction.release_savepoint()
        except psycopg2.Error:
            await transaction.rollback_savepoint()

        await cur.execute('insert into tbl values (4)')
        await transaction.commit()

    except psycopg2.Error:
        await transaction.rollback()


async def main():
    pool = await aiopg.create_pool(dsn)
    async with pool.cursor() as cur:
        await transaction(cur, IsolationLevel.repeatable_read)
        await transaction(cur, IsolationLevel.read_committed)
        await transaction(cur, IsolationLevel.serializable)

        cur.execute('select * from tbl')


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
