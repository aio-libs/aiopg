import asyncio
import aiopg

dsn = 'dbname=aiopg user=aiopg password=passwd host=localhost'


@asyncio.coroutine
def test_select():
    pool = yield from aiopg.create_pool(dsn)
    with (yield from pool.cursor()) as cur:
        ret = yield from cur.execute("SELECT 1")
        assert ret == (1,)


loop = asyncio.get_event_loop()
loop.run_until_complete(test_select())
