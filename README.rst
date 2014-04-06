aiopg
=======

**aiopg** is a library for accessing a PostgreSQL_ database
from the asyncio_ (PEP-3156/tulip) framework. It wraps
asynchronous features of the Psycopg database driver.

Example
=======

.. code-block:: python

    import asyncio
    from aiopg.pool import create_pool

    dsn = 'dbname=jetty user=nick password=1234 host=localhost port=5432'


    @asyncio.coroutine
    def test_select():
        pool = yield from create_pool(dsn)

        with (yield from pool) as conn:
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            ret = yield from cur.fetchone()
            assert ret == (1,), ret

    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_select())

.. _PostgreSQL: http://www.postgresql.org/
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _aiopg: https://github.com/aio-libs/aiopg
