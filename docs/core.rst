.. _aiopg-core:

====================
 Core API Reference
====================

.. module:: aiopg
   :synopsis: A library for accessing a PostgreSQL database from the asyncio
.. currentmodule:: aiopg


.. _aiopg-core-connection:

Connection
==========

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


.. function:: connect(dsn=None, *, loop=None, timeout=60.0, \
                      enable_json=True, enable_hstore=True, echo=False, \
                      **kwargs)

   A :ref:`coroutine <coroutine>` that connects to PostgreSQL.

   The function accepts all parameters that :func:`psycopg2.connect`
   does plus optional keyword-only *loop* and *timeout* parameters.

   :param loop: asyncio event loop instance or ``None`` for default one.

   :param float timeout: default timeout (in seconds) for connection operations.

                         60 secs by default.

   :param bool enable_json: enable json column types for connection.

                         ``True`` by default.

   :param bool enable_hstore: try to enable hstore column types for connection.

                         ``True`` by default.

                         For using HSTORE columns extension should be
                         installed in database first::

                             CREATE EXTENSION HSTORE

   :param bool echo: log executed SQL statement (``False`` by default).

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
               scrollable=None, withhold=False, *, timeout=None)

      A :ref:`coroutine <coroutine>` that creates a new cursor object
      using the connection.

      The only *cursor_factory* can be specified, all other
      parameters are not supported by :term:`psycopg2` in
      asynchronous mode yet.

      The *cursor_factory* argument can be used to create
      non-standard cursors. The argument must be a subclass of
      `psycopg2.extensions.cursor`. See :ref:`subclassing-cursor` for
      details. A default factory for the connection can also be
      specified using the :attr:`Connection.cursor_factory` attribute.

      *timeout* is a timeout for returned cursor instance if
      parameter is not `None`.

      *name*, *scrollable* and *withhold* parameters are not supported
      by :term:`psycopg2` in asynchronous mode.

      :returns: :class:`Cursor` instance.

   .. method:: close()

      Immediatelly close the connection.

      Close the connection now (rather than whenever `del` is executed).
      The connection will be unusable from this point forward; an
      `psycopg2.InterfaceError` will be raised if any operation is
      attempted with the connection.  The same applies to all cursor objects
      trying to use the connection.  Note that closing a connection without
      committing the changes first will cause any pending change to be
      discarded as if a ``ROLLBACK`` was performed.

      .. versionchanged:: 0.5

         :meth:`close` is regular function now.  For sake of backward
         compatibility the method returns :class:`asyncio.Future`
         instance with result already set to ``None`` (you still can
         use ``yield from conn.close()`` construction.

   .. attribute:: closed

      The readonly property that returns ``True`` if connections is closed.

   .. attribute:: echo

      Return *echo mode* status. Log all executed queries to logger
      named ``aiopg`` if ``True``

   .. attribute:: raw

      The readonly property that underlying
      :class:`psycopg2.connection` instance.

   .. method:: cancel(timeout=None)

      A :ref:`coroutine <coroutine>` that cancels current database
      operation.

      The method interrupts the processing of the current
      operation. If no query is being executed, it does nothing. You
      can call this function from a different thread than the one
      currently executing a database operation, for instance if you
      want to cancel a long running query if a button is pushed in the
      UI. Interrupting query execution will cause the cancelled method
      to raise a :exc:`psycopg2.extensions.QueryCanceledError`. Note that
      the termination of the query is not guaranteed to succeed: see
      the documentation for |PQcancel|_.

      :param float timeout: timeout for cancelling.

      .. |PQcancel| replace:: ``PQcancel()``
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

   .. attribute:: isolation_level

      Get the transaction isolation level for the current session.

      .. note::

         The only value allowed in asynchronous mode value is
         :const:`psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED`
         (``READ COMMITTED``).

   .. attribute:: notices

       A list containing all the database messages sent to the client during
       the session::

           >>> yield from cur.execute("CREATE TABLE foo (id serial PRIMARY KEY);")
           >>> pprint(conn.notices)
           ['NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "foo_pkey" for table "foo"\n',
            'NOTICE:  CREATE TABLE will create implicit sequence "foo_id_seq" for serial column "foo.id"\n']

       To avoid a leak in case excessive notices are generated, only the last
       50 messages are kept.

       You can configure what messages to receive using `PostgreSQL logging
       configuration parameters`__ such as ``log_statement``,
       ``client_min_messages``, ``log_min_duration_statement`` etc.

       .. __: http://www.postgresql.org/docs/current/static/runtime-config-logging.html

   .. attribute:: cursor_factory

      The default cursor factory used by `Connection.cursor()` if the
      parameter is not specified.

   .. method:: get_backend_pid()

      Returns the process ID (PID) of the backend server process handling
      this connection.

      Note that the PID belongs to a process executing on the database
      server host, not the local host!

      .. seealso:: libpq docs for `PQbackendPID()`__ for details.

         .. __: http://www.postgresql.org/docs/current/static/libpq-status.html#LIBPQ-PQBACKENDPID

   .. method:: get_parameter_status(parameter)

      Look up a current parameter setting of the server.

      Potential values for ``parameter`` are: ``server_version``,
      ``server_encoding``, ``client_encoding``, ``is_superuser``,
      ``session_authorization``, ``DateStyle``, ``TimeZone``,
      ``integer_datetimes``, and ``standard_conforming_strings``.

      If server did not report requested parameter, return ``None``.

      .. seealso:: libpq docs for `PQparameterStatus()`__ for details.

         .. __: http://www.postgresql.org/docs/current/static/libpq-status.html#LIBPQ-PQPARAMETERSTATUS

   .. method:: get_transaction_status()

      Return the current session transaction status as an integer.  Symbolic
      constants for the values are defined in the module
      `psycopg2.extensions`: see :ref:`transaction-status-constants`
      for the available values.

      .. seealso:: libpq docs for `PQtransactionStatus()`__ for details.

         .. __: http://www.postgresql.org/docs/current/static/libpq-status.html#LIBPQ-PQTRANSACTIONSTATUS

   .. attribute:: protocol_version

      A read-only integer representing frontend/backend protocol being used.
      Currently Psycopg supports only protocol 3, which allows connection
      to PostgreSQL server from version 7.4. Psycopg versions previous than
      2.3 support both protocols 2 and 3.

      .. seealso:: libpq docs for `PQprotocolVersion()`__ for details.

         .. __: http://www.postgresql.org/docs/current/static/libpq-status.html#LIBPQ-PQPROTOCOLVERSION

   .. attribute:: server_version

      A read-only integer representing the backend version.

      The number is formed by converting the major, minor, and revision
      numbers into two-decimal-digit numbers and appending them together.
      For example, version 8.1.5 will be returned as ``80105``.

      .. seealso:: libpq docs for `PQserverVersion()`__ for details.

         .. __: http://www.postgresql.org/docs/current/static/libpq-status.html#LIBPQ-PQSERVERVERSION

   .. attribute:: status

      A read-only integer representing the status of the connection.
      Symbolic constants for the values are defined in the module
      `psycopg2.extensions`: see :ref:`connection-status-constants`
      for the available values.

      The status is undefined for *closed* connectons.

   .. attribute:: timeout

      A read-only float representing default timeout for connection's
      operations.

   The :class:`Connection` class also has several methods not
   described here.  Those methods are not supported in asynchronous
   mode (:exc:`psycopg2.ProgrammingError` is raised).

.. _aiopg-core-cursor:

Cursor
======

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
   connections’ isolation level.

   Its insterface is very close to :class:`psycopg2.cursor`
   (http://initd.org/psycopg/docs/cursor.html) except all methods
   are :ref:`coroutines <coroutine>`.

   Use :meth:`Connection.cursor()` for getting cursor for connection.


   .. attribute:: echo

      Return *echo mode* status. Log all executed queries to logger
      named ``aiopg`` if ``True``

   .. attribute:: description

      This read-only attribute is a sequence of 7-item sequences.

      Each of these sequences is a :func:`collections.namedtuple`
      containing information describing one result column:

      0.  *name*: the name of the column returned.
      1.  *type_code*: the PostgreSQL OID of the column. You can use the
          |pg_type|_ system table to get more informations about the type.
          This is the value used by Psycopg to decide what Python type use
          to represent the value.  See also
          :ref:`type-casting-from-sql-to-python`.
      2.  *display_size*: the actual length of the column in bytes.
          Obtaining this value is computationally intensive, so it is
          always ``None`` unless the :envvar:`PSYCOPG_DISPLAY_SIZE` parameter
          is set at compile time. See also PQgetlength_.
      3.  *internal_size*: the size in bytes of the column associated to
          this column on the server. Set to a negative value for
          variable-size types See also PQfsize_.
      4.  *precision*: total number of significant digits in columns of
          type |NUMERIC|_. ``None`` for other types.
      5.  *scale*: count of decimal digits in the fractional part in
          columns of type |NUMERIC|. ``None`` for other types.
      6.  *null_ok*: always ``None`` as not easy to retrieve from the libpq.

      This attribute will be ``None`` for operations that do not return rows
      or if the cursor has not had an operation invoked via the
      :meth:`execute` method yet.

      .. |pg_type| replace:: ``pg_type``
      .. _pg_type: http://www.postgresql.org/docs/current/static/catalog-pg-type.html
      .. _PQgetlength: http://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQGETLENGTH
      .. _PQfsize: http://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQFSIZE
      .. _NUMERIC: http://www.postgresql.org/docs/current/static/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
      .. |NUMERIC| replace:: ``NUMERIC``

   .. method:: close()

      Close the cursor now (rather than whenever ``del`` is executed).
      The cursor will be unusable from this point forward; an
      :exc:`psycopg2.InterfaceError` will be raised if any operation is
      attempted with the cursor.

      .. note:: :meth:`close` is not a :ref:`coroutine <coroutine>`,
                you don't need to wait it via ``yield from
                curs.close()``.

   .. attribute:: closed

      Read-only boolean attribute: specifies if the cursor is closed
      (``True``) or not (``False``).

   .. attribute:: raw

      The readonly property that underlying
      :class:`psycopg2.cursor` instance.

   .. attribute:: connection

      Read-only attribute returning a reference to the :class:`Connection`
      object on which the cursor was created.

   .. attribute:: timeout

      A read-only float representing default timeout for cursor's
      operations.

   .. method:: execute(operation, parameters=None, *, timeout=None)

      Prepare and execute a database operation (query or command).

      Parameters may be provided as sequence or mapping and will be bound to
      variables in the operation.  Variables are specified either with
      positional (``%s``) or named (:samp:`%({name})s`) placeholders. See
      :ref:`query-parameters`.

      :param float timeout: overrides cursor's timeout if not ``None``.

      :returns: ``None``. If a query was executed, the returned
                values can be retrieved using |fetch*|_ methods.

   .. method:: callproc(procname, parameters=None, *, timeout=None)

      Call a stored database procedure with the given name. The sequence of
      parameters must contain one entry for each argument that the procedure
      expects. The result of the call is returned as modified copy of the
      input sequence. Input parameters are left untouched, output and
      input/output parameters replaced with possibly new values.

      The procedure may also provide a result set as output. This must then
      be made available through the standard |fetch*|_ methods.

      :param float timeout: overrides cursor's timeout if not ``None``.

   .. method:: mogrify(operation, parameters=None)

      Returns a query string after arguments binding. The string
      returned is exactly the one that would be sent to the database
      running the :meth:`Cursor.execute` method or similar.

      The returned string is always a bytes string::

         >>> yield from cur.mogrify("INSERT INTO test (num, data) VALUES (%s, %s)", (42, 'bar'))
         "INSERT INTO test (num, data) VALUES (42, E'bar')"

   .. method:: setinputsizes(sizes)

      This method is exposed in compliance with the :term:`DBAPI`. It currently
      does nothing but it is safe to call it.


   .. |fetch*| replace:: ``fetch*()``

   .. _fetch*:

   .. rubric:: Results retrieval methods


   The following methods are used to read data from the database after an
   :meth:`Cursor.execute` call.

   .. _cursor-iterable:

   .. warning::

      :class:`Cursor` objects do **not** support iteration, since
      version 0.7.

      Iterable protocol in :class:`Cursor` hides ``yield from`` from user,
      witch should be explicit. Moreover iteration support is optional,
      according to PEP-249 (https://www.python.org/dev/peps/pep-0249/#iter).

   .. method:: fetchone()

      Fetch the next row of a query result set, returning a single tuple,
      or ``None`` when no more data is available::

         >>> yield from cur.execute("SELECT * FROM test WHERE id = %s", (3,))
         >>> yield from cur.fetchone()
         (3, 42, 'bar')

      A :exc:`psycopg2.ProgrammingError` is raised if the previous
      call to :meth:`execute` did not produce any result set or no
      call was issued yet.


   .. method:: fetchmany(size=cursor.arraysize)

      Fetch the next set of rows of a query result, returning a list of
      tuples. An empty list is returned when no more rows are available.

      The number of rows to fetch per call is specified by the parameter.
      If it is not given, the cursor's :attr:`Cursor.arraysize` determines
      the number of rows to be fetched. The method should try to fetch as
      many rows as indicated by the size parameter. If this is not possible
      due to the specified number of rows not being available, fewer rows
      may be returned::

         >>> yield from cur.execute("SELECT * FROM test;")
         >>> yield from cur.fetchmany(2)
         [(1, 100, "abc'def"), (2, None, 'dada')]
         >>> yield from cur.fetchmany(2)
         [(3, 42, 'bar')]
         >>> yield from cur.fetchmany(2)
         []

      A :exc:`psycopg2.ProgrammingError` is raised if the previous call to
      :meth:`execute` did not produce any result set or no call was issued yet.

      Note there are performance considerations involved with the size
      parameter.  For optimal performance, it is usually best to use the
      :attr:`Cursor.arraysize` attribute.  If the size parameter is used,
      then it is best for it to retain the same value from one
      :meth:`fetchmany` call to the next.


   .. method:: fetchall()

      Fetch all (remaining) rows of a query result, returning them as a list
      of tuples.  An empty list is returned if there is no more record to
      fetch::

         >>> yield from cur.execute("SELECT * FROM test;")
         >>> yield from cur.fetchall()
         [(1, 100, "abc'def"), (2, None, 'dada'), (3, 42, 'bar')]

      A :exc:`psycopg2.ProgrammingError` is raised if the previous
      call to :meth:`execute` did not produce any result set or no
      call was issued yet.


    .. method:: scroll(value, mode='relative')

       Scroll the cursor in the result set to a new position according
       to mode.

       If *mode* is ``relative`` (default), value is taken as offset to
       the current position in the result set, if set to ``absolute``,
       value states an absolute target position.

       If the scroll operation would leave the result set, a
       :exc:`psycopg2.ProgrammingError` is raised and the cursor position is
       not changed.

       .. note::

          According to the :term:`DBAPI`, the exception raised for a cursor out
          of bound should have been :exc:`IndexError`.  The best option is
          probably to catch both exceptions in your code::

             try:
                 yield from cur.scroll(1000 * 1000)
             except (ProgrammingError, IndexError), exc:
                 deal_with_it(exc)

   .. attribute:: arraysize

      This read/write attribute specifies the number of rows to fetch at a
      time with :meth:`Cursor.fetchmany`. It defaults to 1 meaning to fetch
      a single row at a time.


   .. attribute:: rowcount

      This read-only attribute specifies the number of rows that the
      last :meth:`execute` produced (for :abbr:`DQL (Data Query
      Language)` statements like ``SELECT``) or affected (for
      :abbr:`DML (Data Manipulation Language)` statements like
      ``UPDATE`` or ``INSERT``).

      The attribute is *-1* in case no :meth:`execute` has been
      performed on the cursor or the row count of the last operation
      if it can't be determined by the interface.

      .. note::
         The :term:`DBAPI` interface reserves to redefine the latter case to
         have the object return ``None`` instead of *-1* in future versions
         of the specification.


   .. attribute:: rownumber

      This read-only attribute provides the current 0-based index of the
      cursor in the result set or ``None`` if the index cannot be
      determined.

      The index can be seen as index of the cursor in a sequence (the result
      set). The next fetch operation will fetch the row indexed by
      ``rownumber`` in that sequence.

   .. attribute:: lastrowid

      This read-only attribute provides the OID of the last row inserted
      by the cursor. If the table wasn't created with OID support or the
      last operation is not a single record insert, the attribute is set to
      ``None``.

      .. note::

         PostgreSQL currently advices to not create OIDs on the tables and
         the default for |CREATE-TABLE|__ is to not support them. The
         |INSERT-RETURNING|__ syntax available from PostgreSQL 8.3 allows
         more flexibility.

      .. |CREATE-TABLE| replace:: ``CREATE TABLE``
      .. __: http://www.postgresql.org/docs/current/static/sql-createtable.html

      .. |INSERT-RETURNING| replace:: ``INSERT ... RETURNING``
      .. __: http://www.postgresql.org/docs/current/static/sql-insert.html


   .. attribute:: query

      Read-only attribute containing the body of the last query sent to the
      backend (including bound arguments) as bytes string. ``None`` if no
      query has been executed yet::

         >>> yield from cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (42, 'bar'))
         >>> cur.query
         "INSERT INTO test (num, data) VALUES (42, E'bar')"

   .. attribute:: statusmessage

      Read-only attribute containing the message returned by the last
      command::

         >>> yield from cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (42, 'bar'))
         >>> cur.statusmessage
         'INSERT 0 1'

   .. attribute:: tzinfo_factory

      The time zone factory used to handle data types such as
      ``TIMESTAMP WITH TIME ZONE``.  It should be a `datetime.tzinfo`
      object.  A few implementations are available in the ``psycopg2.tz``
      module.


   .. method:: setoutputsize(size, column=None)

      This method is exposed in compliance with the :term:`DBAPI`. It currently
      does nothing but it is safe to call it.




.. _aiopg-core-pool:

Pool
====

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
                          enable_json=True, enable_hstore=True, \
                          loop=None, timeout=60.0, **kwargs)

   A :ref:`coroutine <coroutine>` that creates a pool of connections to
   :term:`PostgreSQL` database.

   The function accepts all parameters that :func:`psycopg2.connect`
   does plus optional keyword-only parameters *loop*, *minsize*, *maxsize*.

   *loop* is an optional *event loop* instance,
    :func:`asyncio.get_event_loop` is used if *loop* is not specified.

   *minsize* and *maxsize* are minimum and maximum sizes of the *pool*.

   *timeout* is a default timeout (in seconds) for connection
    operations. 60 secs if not specified.

   *enable_json* --- enable json column types for connections
   created by the pool.

      ``True`` by default.

   *enable_hstore* --- try to enable hstore column types for connections
   created by the pool.

      ``True`` by default.

      For using HSTORE columns extension should be
      installed in database first::

         CREATE EXTENSION HSTORE

   *echo* --- executed log SQL queryes (``False`` by default).

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

   .. attribute:: echo

      Return *echo mode* status. Log all executed queries to logger
      named ``aiopg`` if ``True``

   .. attribute:: minsize

      A minimal size of the pool (*read-only*), ``10`` by default.

   .. attribute:: maxsize

      A maximal size of the pool (*read-only*), ``10`` by default.

   .. attribute:: size

      A current size of the pool (*readonly*). Includes used and free
      connections.

   .. attribute:: freesize

      A count of free connections in the pool (*readonly*).

   .. attribute:: timeout

      A read-only float representing default timeout for operations
      for connections from pool.

   .. method:: clear()

      A :ref:`coroutine <coroutine>` that closes all *free* connections
      in the pool. At next connection acquiring at least :attr:`minsize` of
      them will be recreated.

   .. method:: close()

      Close pool.

      Mark all pool connections to be closed on getting back to pool.
      Closed pool doesn't allow to acquire new connections.

      If you want to wait for actual closing of acquired connection please
      call :meth:`wait_closed` after :meth:`close`.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

   .. method:: terminate()

      Terminate pool.

      Close pool with instantly closing all acquired connections also.

      :meth:`wait_closed` should be called after :meth:`terminate` for
      waiting for actual finishing.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

   .. method:: wait_closed()

      A :ref:`coroutine <coroutine>` that waits for releasing and
      closing all acquired connections.

      Should be called after :meth:`close` for waiting for actual pool
      closing.

   .. method:: acquire()

      A :ref:`coroutine <coroutine>` that acquires a connection from
      *free pool*. Creates new connection if needed and :attr:`size`
      of pool is less than :attr:`maxsize`.

      Returns a :class:`Connection` instance.

   .. method:: release(conn)

      Reverts connection *conn* to *free pool* for future recycling.

      .. warning:: The method is not a :ref:`coroutine <coroutine>`.

   .. method:: cursor(name=None, cursor_factory=None, scrollable=None, \
               withhold=False, *, timeout=None)

      A :ref:`coroutine<coroutine>` that :meth:`acquires <acquire>` a
      connection and returns *context manager*.

      The only *cursor_factory* can be specified, all other
      parameters are not supported by :term:`psycopg2` in
      asynchronous mode yet.

      The *cursor_factory* argument can be used to create
      non-standard cursors. The argument must be a subclass of
      `psycopg2.extensions.cursor`. See :ref:`subclassing-cursor` for
      details. A default factory for the connection can also be
      specified using the :attr:`Connection.cursor_factory` attribute.

      *timeout* is a timeout for returned cursor instance if parameter
      is not `None`.

      *name*, *scrollable* and *withhold* parameters are not supported
      by :term:`psycopg2` in asynchronous mode.

      The usage is::

         with (yield from pool.cursor()) as cur:
             yield from cur.execute('SELECT 1')

      After exiting from *with block* cursor *cur* will be closed.


.. _aiopg-core-exceptions:

Exceptions
==========

Any call to library function, method or property can raise an exception.

:mod:`aiopg` doesn't define any exception class itself, it reuses
:ref:`DBAPI Exceptions <dbapi-exceptions>` from :mod:`psycopg2`


.. _aiopg-core-transactions:

Transactions
============

While :mod:`aiopg` works only in *autocommit mode* it is still
possible to use SQL transactions.

Just execute **BEGIN** and **COMMIT** statements manually.

:meth:`Connection.commit` and :meth:`Connection.rollback` methods are
disabled and always raises :exc:`psycopg2.ProgrammingError` exception.


.. _aiopg-core-extension-type-translations:

Extension type translations
===========================

JSON
----

:mod:`aiopg` has support for ``JSON`` data type enabled by default.

For pushing data to server please wrap json dict into
:class:`psycopg2.extras.Json`::

   from psycopg2.extras import Json

   data = {'a': 1, 'b': 'str'}
   yield from cur.execute("INSERT INTO tbl (val) VALUES (%s)", [Json(data)])

On receiving data from json column :term:`psycopg2` autoconvers result
into python :class:`dict` object::

   yield from cur.execute("SELECT val FROM tbl")
   item = yield from cur.fetchone()
   assert item == {'b': 'str', 'a': 1}
