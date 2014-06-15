.. _aiopg-core:

Core API Reference
===============================

.. module:: aiopg
   :synopsis: A library for accessing a PostgreSQL database from the asyncio
.. currentmodule:: aiopg


.. _aiopg-core-connection:

Connection
--------------

The library provides a way to connect to PostgreSQL database.

Example::

  @asyncio.coroutine
  def go():
      conn = yield from aiopg.connect(database='aiopg',
                                      user='aiopg',
                                      password='secret',
                                      host='127.0.0.1')
      cur = yield from conn.cursor()
      yield from cur.execute("SELECT * FROM tbl")
      ret = yield from cur.fetchall()


.. function:: connect(dsn=None, *, loop=None, **kwargs)

   A :ref:`coroutine <coroutine>` that connects to PostgreSQL.

   The function accepts all parameters that :func:`psycopg2.connect`
   does plus optional keyword-only parameter *loop*.

   :returns: :class:`Connection` instance.


.. class:: Connection

   A connection to a :term:`PostgreSQL` database instance. It encapsulates a
   database session.

   Its insterface is very close to :class:`psycopg2.connection`
   (http://initd.org/psycopg/docs/connection.html) except all methods
   are :ref:`coroutines <coroutine>`.

   Use :func:`connect` for creating connection.

   The most important method is

   .. method:: cursor(name=None, cursor_factory=None, \
               scrollable=None, withhold=False)

       A :ref:`coroutine <coroutine>` that returns cursor for connection.

       The only *cursor_factory* can be specified, all other
       parameters are not supported by :term:`psycopg2` in
       asynchronous mode yet.

       :returns: :class:`Cursor` instance.

   .. method:: close()

      Immediatelly close the connection.

   .. attribute:: closed

      The readonly property that returns ``True`` if connections is closed.

   .. method:: cancel()

      A :ref:`coroutine <coroutine>` that cancels current database
      operation.

      The method interrupts the processing of the current
      operation. If no query is being executed, it does nothing. You
      can call this function from a different thread than the one
      currently executing a database operation, for instance if you
      want to cancel a long running query if a button is pushed in the
      UI. Interrupting query execution will cause the cancelled method
      to raise a `~psycopg2.extensions.QueryCanceledError`. Note that
      the termination of the query is not guaranteed to succeed: see
      the documentation for |PQcancel|_.

      .. |PQcancel| replace:: `!PQcancel()`
      .. _PQcancel: http://www.postgresql.org/docs/current/static/libpq-cancel.html#LIBPQ-PQCANCEL
   .. attribute:: dsn

      The readonly property that returns *dsn* string used by the
      connection.

   .. attribute:: autocommit

      Autocommit mode status for connection (always ``True``).

      .. note::

         :term:`psycopg2` doesn't allow to change *autocommit* mode in
         asynchronous mode.

   .. attribute:: encoding

      Client encoding for SQL operations.

      .. note::

         :term:`psycopg2` doesn't allow to change encoding in
         asynchronous mode.


.. _aiopg-core-cursor:

Cursor
------

.. class:: Cursor

   A cursor for connection.

   Allows Python code to execute :term:`PostgreSQL` command in a database
   session. Cursors are created by the :meth:`Connection.cursor` coroutine:
   they are bound to the connection for the entire lifetime and all
   the commands are executed in the context of the database session
   wrapped by the connection.

   Cursors that are created from the same connection are not isolated,
   i.e., any changes done to the database by a cursor are immediately
   visible by the other cursors. Cursors created from different
   connections can or can not be isolated, depending on the
   connections’ isolation level. See also rollback() and commit()
   methods.

   Its insterface is very close to :class:`psycopg2.cursor`
   (http://initd.org/psycopg/docs/cursor.html) except all methods
   are :ref:`coroutines <coroutine>`.

   Use :meth:`Connection.cursor()` for getting cursor for connection.


.. _aiopg-core-pool:

Pool
-----

The library provides *connection pool* as well as plain
:class:`Connection` objects.


The basic usage is::

    import asyncio
    import aiopg

    dsn = 'dbname=jetty user=nick password=1234 host=localhost port=5432'


    @asyncio.coroutine
    def test_select():
        pool = yield from aiopg.create_pool(dsn)

        with (yield from pool.cursor()) as cur:
            yield from cur.execute('SELECT 1')
            ret = yield from cur.fetchone()
            assert ret == (1,), ret


.. function:: create_pool(dsn=None, *, minsize=10, maxsize=10,\
                          loop=None, **kwargs)

   A :ref:`coroutine <coroutine>` that creates a pool of connections to
   :term:`PostgreSQL` database.

   The function accepts all parameters that :func:`psycopg2.connect`
   does plus optional keyword-only parameters *loop*, *minsize*, *maxsize*.

   *loop* is an optional *event loop* instance,
    :func:`asyncio.get_event_loop` is used if *loop* is not specified.

   *minsize* and *maxsize* are minimum and maximum sizes of the *pool*.

   Returns :class:`Pool` instance.


.. class:: Pool

   A connection pool.

   After creation pool has *minsize* free connections and can grow up
   to *maxsize* ones.

   If *minsize* is ``0`` the pool doesn't creates any connection on startup.

   If *maxsize* is ``0`` than size of pool is unlimited (but it
   recycles used connections of course).

   The most important way to use it is getting connection in *with statement*::

      with (yield from pool) as conn:
          cur = yield from conn.cursor()

   and shortcut for getting *cursor* directly::

      with (yield from pool.cursor()) as cur:
          yield from cur.execute('SELECT 1')

   See also :meth:`Pool.acquire` and :meth:`Pool.release` for acquring
   *connection* without *with statement*.

   .. attribute:: minsize

      A minimal size of the pool (*read-only*), ``10`` by default.

   .. attribute:: maxsize

      A maximal size of the pool (*read-only*), ``10`` by default.

   .. attribute:: size

      A current size of the pool (*readonly*). Includes used and free
      connections.

   .. attribute:: freesize

      A count of free connections in the pool (*readonly*).

   .. method:: clear()

      A :ref:`coroutine <coroutine>` that closes all *free* connections
      in the pool. At next connection acquiring at least :attr:`minsize` of
      them will be recreated.

   .. method:: acquire()

      A :ref:`coroutine <coroutine>` that acquires a connection from
      *free pool*. Creates new connection if needed and :attr:`size`
      of pool is less than :attr:`maxsize`.

      Returns a :class:`Connection` instance.

   .. method:: release(conn)

      Reverts connection *conn* to *free pool* for future recycling.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

   .. method:: cursor()

      A :ref:`coroutine<coroutine>` that :meth:`acquires <acquire>` a
      connection and returns *context manager*.

      The usage is::

         with (yield from pool.cursor()) as cur:
             yield from cur.execute('SELECT 1')

      After exiting from *with block* cursor *cur* will be closed.

.. _aiopg-core-exceptions:

Exceptions
-----------

Any call to library function, method or property can raise an exception.

:mod:`aiopg` doesn't define any exception class itself, it reuses
:ref:`DBAPI Exceptions <dbapi-exceptions>` from :mod:`psycopg2`


.. _aiopg-core-transactions:

Transactions
------------

While :mod:`aiopg` works only in *autocommit mode* it is still
possible to use SQL transactions.

Just execute **BEGIN** and **COMMIT** statements manually.

:meth:`Connection.commit` and :meth:`Connection.rollback` methods are
disabled and always raises :exc:`psycopg2.ProgrammingError` exception.
