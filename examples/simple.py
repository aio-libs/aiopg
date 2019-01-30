import asyncio

import aiopg

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


async def test_select():
    async with aiopg.create_pool(dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                ret = []
                async for row in cur:
                    ret.append(row)
                assert ret == [(1,)]
    print("ALL DONE")


loop = asyncio.get_event_loop()
loop.run_until_complete(test_select())
