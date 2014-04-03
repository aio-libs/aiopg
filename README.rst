aiopg
=======

**Aiopg** is a library for accessing a PostgreSQL_ database
from the asyncio_ (PEP-3156/tulip) framework. It wraps
asynchronous features of the Psycopg database driver.

Library is **not stable** and there are **no tests** (yet),
consider psycotulip_ instead.

Example
=======

.. code-block:: python

    import asyncio
    from aiopg.pool import Pool

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

.. _PostgreSQL: http://www.postgresql.org/
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _psycotulip: https://github.com/fafhrd91/psycotulip


Other Projects
==============
I have learned alot from this projects, authors have done great work,
give them a try first.

https://github.com/FSX/momoko

https://github.com/wulczer/txpostgres

https://github.com/fafhrd91/psycotulip
