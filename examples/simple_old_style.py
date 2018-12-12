import asyncio
import aiopg

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


async def test_select():
    pool = await aiopg.create_pool(dsn)
    with (await pool.cursor()) as cur:
        await cur.execute("SELECT 1")
        ret = await cur.fetchone()
        assert ret == (1,)
    print("ALL DONE")


loop = asyncio.get_event_loop()
loop.run_until_complete(test_select())
