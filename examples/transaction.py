import asyncio

import aiopg
from aiopg.transaction import IsolationLevel, Transaction

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


async def transaction(cur, isolation_level,
                      readonly=False, deferrable=False):
    async with Transaction(cur, isolation_level,
                           readonly, deferrable) as transaction:
        await cur.execute('insert into tbl values (1)')

        async with transaction.point():
            await cur.execute('insert into tbl values (3)')

        await cur.execute('insert into tbl values (4)')


async def main():
    async with aiopg.create_pool(dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('CREATE TABLE tbl (id int)')
                await transaction(cur, IsolationLevel.repeatable_read)
                await transaction(cur, IsolationLevel.read_committed)
                await transaction(cur, IsolationLevel.serializable)

                await cur.execute('select * from tbl')


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
