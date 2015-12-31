import asyncio
import aiopg

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


@asyncio.coroutine
def test_select():
    pool = yield from aiopg.create_pool(dsn)
    with (yield from pool.cursor()) as cur:
        yield from cur.execute("SELECT 1")
        ret = yield from cur.fetchone()
        assert ret == (1,)
    print("ALL DONE")


loop = asyncio.get_event_loop()
loop.run_until_complete(test_select())
