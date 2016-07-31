.. aiopg documentation master file, created by
   sphinx-quickstart on Sat Apr  5 00:00:44 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aiopg
=================================

.. _GitHub: https://github.com/aio-libs/aiopg
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html


**aiopg** is a library for accessing a :term:`PostgreSQL` database
from the asyncio_ (PEP-3156/tulip) framework. It wraps
asynchronous features of the Psycopg database driver.

Features
--------

- Implements *asyncio* :term:`DBAPI` *like* interface for
  :term:`PostgreSQL`.  It includes :ref:`aiopg-core-connection`,
  :ref:`aiopg-core-cursor` and :ref:`aiopg-core-pool` objects.
- Implements *optional* support for charming :term:`sqlalchemy`
  functional sql layer.


Basics
------

The library uses :mod:`psycopg2` connections in **asynchronous** mode
internally.

Literally it is an (almost) transparent wrapper for psycopg2
connection and cursor, but with only exception.

You should use ``yield from conn.f()`` instead of just call ``conn.f()`` for
every method.

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.

See example::

    import asyncio
    import aiopg

    dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'

    async def go():
        async with aiopg.create_pool(dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    ret = []
                    async for row in cur:
                        ret.append(row)
                    assert ret == [(1,)]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())

For documentation about connection and cursor methods/properties
please go to psycopg docs: http://initd.org/psycopg/docs/

.. note:: psycopg2 creates new connections with ``autocommit=True``
          option in asynchronous mode. Autocommitting cannot be disabled.

          See :ref:`aiopg-core-transactions` about transaction usage
          in *autocommit mode*.

.. note::

   Throughout this documentation, examples utilize the `async/await` syntax
   introduced by :pep:`492` that is only valid for Python 3.5+.

   If you are using Python 3.4, please replace ``await`` with
   ``yield from`` and ``async def`` with a ``@coroutine`` decorator.
   For example, this::

       async def coro(...):
           ret = await f()

   shoud be replaced by::

       @asyncio.coroutine
       def coro(...):
           ret = yield from f()

   see also :ref:`aiopg-examples-old-style` examples.

SQLAlchemy and aiopg
--------------------

:ref:`aiopg-core` provides core support for :term:`PostgreSQL` connections.

We have found it to be very annoying to write raw SQL queries manually,
so we introduce support for :term:`sqlalchemy` query builders::

    import asyncio
    from aiopg.sa import create_engine
    import sqlalchemy as sa


    metadata = sa.MetaData()

    tbl = sa.Table('tbl', metadata,
                   sa.Column('id', sa.Integer, primary_key=True),
                   sa.Column('val', sa.String(255)))

    async def go():
        async with create_engine(user='aiopg',
                                 database='aiopg',
                                 host='127.0.0.1',
                                 password='passwd') as engine:
            async with engine.acquire() as conn:
                await conn.execute(tbl.insert().values(val='abc'))

                async for row in conn.execute(tbl.select().where(tbl.c.val=='abc'))
                    print(row.id, row.val)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())

We believe constructions like ``tbl.insert().values(val='abc')`` and
``tbl.select().where(tbl.c.val=='abc')`` to be very handy and
convinient.


Installation
--------------------

.. code::

   pip3 install aiopg

.. note:: :mod:`aiopg` requires :term:`psycopg2` library.

   You can use standard one from your distro like::

      $ sudo apt-get install python3-psycopg2

   but if you like to use virtual environments
   (:term:`virtualenvwrapper`, :term:`virtualenv` or :term:`venv`) you
   probably have to install :term:`libpq` development package::

      $ sudo apt-get install libpq-dev

Also you probably want to use :mod:`aiopg.sa`.

.. _aiozmq-install-sqlalchemy:

:mod:`aiopg.sa` module is **optional** and requires
:term:`sqlalchemy`. You can install *sqlalchemy* by running::

  pip3 install sqlalchemy

Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aiopg/issues>`_ if you have found a bug
or have some suggestion for library improvement.

The library uses `Travis <https://travis-ci.org/aio-libs/aiopg>`_ for
Continious Integration.

Discussion list
---------------

*aio-libs* google group: https://groups.google.com/forum/#!forum/aio-libs

Feel free to post your questions and ideas here.


Dependencies
------------

- Python 3.3 and :mod:`asyncio` or Python 3.4+
- psycopg2
- aiopg.sa requires :term:`sqlalchemy`.

Authors and License
-------------------

The ``aiopg`` package is written by Andrew Svetlov.  It's BSD
licensed and freely available.

Feel free to improve this package and send a pull request to GitHub_.

Contents:

.. toctree::
   :maxdepth: 2

   core
   sa
   examples
   contributing
   glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
