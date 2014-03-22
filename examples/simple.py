import asyncio
from pgtulip.pool import Pool

dsn = 'dbname=jetty user=nick password=1234 host=localhost port=5432'


@asyncio.coroutine
def test_select():
    pool = Pool(dsn)
    # init pool connections
    yield from pool.setup()
    morgify_result = yield from pool.morgify("select 1;")
    # simple query in transaction
    transaction_result = yield from pool.run_transaction(
        [
            ("select %s;", (2)),
            ("select 3;", (3)),
        ]
    )
    # simple query
    query_result = yield from pool.run_query("select %s;", (5,))

    print("morgify_result=", morgify_result)
    print("transaction_result=", transaction_result)
    print("query_result=", query_result)


loop = asyncio.get_event_loop()
loop.run_until_complete(test_select())