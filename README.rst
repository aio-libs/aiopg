aiopg
=======
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


   asyncio.get_event_loop().run_until_complete(test_select())


Example of SQLAlchemy optional integration
-------------------------------------------

::

   import asyncio
   from aiopg.sa import create_engine
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
           yield from conn.execute(tbl.insert().values(val='abc'))

           res = yield from conn.execute(tbl.select())
           for row in res:
               print(row.id, row.val)


   asyncio.get_event_loop().run_until_complete(go())

.. _PostgreSQL: http://www.postgresql.org/
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html

Please use::

   $ python3 runtests.py

for executing project's unittests
