aiopg
=====
.. image:: https://travis-ci.org/aio-libs/aiopg.svg?branch=master
    :target: https://travis-ci.org/aio-libs/aiopg
.. image:: https://coveralls.io/repos/aio-libs/aiopg/badge.svg
    :target: https://coveralls.io/r/aio-libs/aiopg

**aiopg** is a library for accessing a PostgreSQL_ database
from the asyncio_ (PEP-3156/tulip) framework. It wraps
asynchronous features of the Psycopg database driver.

Example
-------

::

    import asyncio
    import aiopg

    dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'

    async def go():
        pool = await aiopg.create_pool(dsn)
        async with pool.acquire() as cur:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                ret = []
                async for row in cur:
                    ret.append(row)
                assert ret == [(1,)]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())


Example of SQLAlchemy optional integration
-------------------------------------------

::

   import asyncio
   from aiopg.sa import create_engine
   from sqlalchemy.schema import CreateTable
   import sqlalchemy as sa

   metadata = sa.MetaData()

   tbl = sa.Table('tbl', metadata,
       sa.Column('id', sa.Integer, primary_key=True),
       sa.Column('val', sa.String(255)))


   @asyncio.coroutine
   def go():
       engine = yield from create_engine(user='aiopg',
                                         database='aiopg',
                                         host='127.0.0.1',
                                         password='passwd')

       with (yield from engine) as conn:
           yield from conn.execute(CreateTable(tbl))
           yield from conn.execute(tbl.insert().values(val='abc'))

           res = yield from conn.execute(tbl.select())
           for row in res:
               print(row.id, row.val)


   asyncio.get_event_loop().run_until_complete(go())


For ``yield from`` based code see ``./examples`` folder, files with
``old_style`` part in their names.

.. _PostgreSQL: http://www.postgresql.org/
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html

Please use::

   $ python3 runtests.py

for executing the project's unittests.  See CONTRIBUTING.rst for details
on how to set up your environment to run the tests.
