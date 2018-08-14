import asyncio
import warnings

import psycopg2

from .log import logger
from .transaction import Transaction, IsolationLevel
from .utils import PY_35, PY_352, _TransactionBeginContextManager


class Cursor:
    def __init__(self, conn, impl, timeout, echo):
        self._conn = conn
        self._impl = impl
        self._timeout = timeout
        self._echo = echo
        self._transaction = Transaction(self, IsolationLevel.repeatable_read)

    @property
    def echo(self):
        """Return echo mode status."""
        return self._echo

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences is a collections.namedtuple containing
        information describing one result column:

        0.  name: the name of the column returned.
        1.  type_code: the PostgreSQL OID of the column.
        2.  display_size: the actual length of the column in bytes.
        3.  internal_size: the size in bytes of the column associated to
            this column on the server.
        4.  precision: total number of significant digits in columns of
            type NUMERIC. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type NUMERIC. None for other types.
        6.  null_ok: always None as not easy to retrieve from the libpq.

        This attribute will be None for operations that do not
        return rows or if the cursor has not had an operation invoked
        via the execute() method yet.

        """
        return self._impl.description

    def close(self):
        """Close the cursor now."""
        self._impl.close()

    @property
    def closed(self):
        """Read-only boolean attribute: specifies if the cursor is closed."""
        return self._impl.closed

    @property
    def connection(self):
        """Read-only attribute returning a reference to the `Connection`."""
        return self._conn

    @property
    def raw(self):
        """Underlying psycopg cursor object, readonly"""
        return self._impl

    @property
    def name(self):
        # Not supported
        return self._impl.name

    @property
    def scrollable(self):
        # Not supported
        return self._impl.scrollable

    @scrollable.setter
    def scrollable(self, val):
        # Not supported
        self._impl.scrollable = val

    @property
    def withhold(self):
        # Not supported
        return self._impl.withhold

    @withhold.setter
    def withhold(self, val):
        # Not supported
        self._impl.withhold = val

    @asyncio.coroutine
    def execute(self, operation, parameters=None, *, timeout=None):
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be
        bound to variables in the operation.  Variables are specified
        either with positional %s or named %({name})s placeholders.

        """
        if timeout is None:
            timeout = self._timeout
        waiter = self._conn._create_waiter('cursor.execute')
        if self._echo:
            logger.info(operation)
            logger.info("%r", parameters)
        try:
            self._impl.execute(operation, parameters)
        except BaseException:
            self._conn._waiter = None
            raise
        try:
            yield from self._conn._poll(waiter, timeout)
        except asyncio.TimeoutError:
            self._impl.close()
            raise

    @asyncio.coroutine
    def executemany(self, operation, seq_of_parameters):
        # Not supported
        raise psycopg2.ProgrammingError(
            "executemany cannot be used in asynchronous mode")

    @asyncio.coroutine
    def callproc(self, procname, parameters=None, *, timeout=None):
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each
        argument that the procedure expects. The result of the call is
        returned as modified copy of the input sequence. Input
        parameters are left untouched, output and input/output
        parameters replaced with possibly new values.

        """
        if timeout is None:
            timeout = self._timeout
        waiter = self._conn._create_waiter('cursor.callproc')
        if self._echo:
            logger.info("CALL %s", procname)
            logger.info("%r", parameters)
        try:
            self._impl.callproc(procname, parameters)
        except BaseException:
            self._conn._waiter = None
            raise
        else:
            yield from self._conn._poll(waiter, timeout)

    def begin(self):
        return _TransactionBeginContextManager(self._transaction.begin())

    def begin_nested(self):
        if not self._transaction.is_begin:
            return _TransactionBeginContextManager(
                self._transaction.begin())
        else:
            return self._transaction.point()

    @asyncio.coroutine
    def mogrify(self, operation, parameters=None):
        """Return a query string after arguments binding.

        The string returned is exactly the one that would be sent to
        the database running the .execute() method or similar.

        """
        ret = self._impl.mogrify(operation, parameters)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "mogrify")
        return ret

    @asyncio.coroutine
    def setinputsizes(self, sizes):
        """This method is exposed in compliance with the DBAPI.

        It currently does nothing but it is safe to call it.

        """
        self._impl.setinputsizes(sizes)

    @asyncio.coroutine
    def fetchone(self):
        """Fetch the next row of a query result set.

        Returns a single tuple, or None when no more data is
        available.

        """
        ret = self._impl.fetchone()
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @asyncio.coroutine
    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result.

        Returns a list of tuples. An empty list is returned when no
        more rows are available.

        The number of rows to fetch per call is specified by the
        parameter.  If it is not given, the cursor's .arraysize
        determines the number of rows to be fetched. The method should
        try to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number
        of rows not being available, fewer rows may be returned.

        """
        if size is None:
            size = self._impl.arraysize
        ret = self._impl.fetchmany(size)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @asyncio.coroutine
    def fetchall(self):
        """Fetch all (remaining) rows of a query result.

        Returns them as a list of tuples.  An empty list is returned
        if there is no more record to fetch.

        """
        ret = self._impl.fetchall()
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @asyncio.coroutine
    def scroll(self, value, mode="relative"):
        """Scroll to a new position according to mode.

        If mode is relative (default), value is taken as offset
        to the current position in the result set, if set to
        absolute, value states an absolute target position.

        """
        ret = self._impl.scroll(value, mode)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @property
    def arraysize(self):
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, val):
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        self._impl.arraysize = val

    @property
    def itersize(self):
        # Not supported
        return self._impl.itersize

    @itersize.setter
    def itersize(self, val):
        # Not supported
        self._impl.itersize = val

    @property
    def rowcount(self):
        """Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.

        """
        return self._impl.rowcount

    @property
    def rownumber(self):
        """Row index.

        This read-only attribute provides the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined."""

        return self._impl.rownumber

    @property
    def lastrowid(self):
        """OID of the last inserted row.

        This read-only attribute provides the OID of the last row
        inserted by the cursor. If the table wasn't created with OID
        support or the last operation is not a single record insert,
        the attribute is set to None.

        """
        return self._impl.lastrowid

    @property
    def query(self):
        """The last executed query string.

        Read-only attribute containing the body of the last query sent
        to the backend (including bound arguments) as bytes
        string. None if no query has been executed yet.

        """
        return self._impl.query

    @property
    def statusmessage(self):
        """the message returned by the last command."""

        return self._impl.statusmessage

    # @asyncio.coroutine
    # def cast(self, old, s):
    #     ...

    @property
    def tzinfo_factory(self):
        """The time zone factory used to handle data types such as
        `TIMESTAMP WITH TIME ZONE`.
        """
        return self._impl.tzinfo_factory

    @tzinfo_factory.setter
    def tzinfo_factory(self, val):
        """The time zone factory used to handle data types such as
        `TIMESTAMP WITH TIME ZONE`.
        """
        self._impl.tzinfo_factory = val

    @asyncio.coroutine
    def nextset(self):
        # Not supported
        self._impl.nextset()  # raises psycopg2.NotSupportedError

    @asyncio.coroutine
    def setoutputsize(self, size, column=None):
        # Does nothing
        self._impl.setoutputsize(size, column)

    @asyncio.coroutine
    def copy_from(self, file, table, sep='\t', null='\\N', size=8192,
                  columns=None):
        raise psycopg2.ProgrammingError(
            "copy_from cannot be used in asynchronous mode")

    @asyncio.coroutine
    def copy_to(self, file, table, sep='\t', null='\\N', columns=None):
        raise psycopg2.ProgrammingError(
            "copy_to cannot be used in asynchronous mode")

    @asyncio.coroutine
    def copy_expert(self, sql, file, size=8192):
        raise psycopg2.ProgrammingError(
            "copy_expert cannot be used in asynchronous mode")

    @property
    def timeout(self):
        """Return default timeout for cursor operations."""
        return self._timeout

    def __iter__(self):
        warnings.warn("Iteration over cursor is deprecated",
                      DeprecationWarning,
                      stacklevel=2)
        while True:
            row = yield from self.fetchone()
            if row is None:
                return
            else:
                yield row

    if PY_35:  # pragma: no branch

        def __aiter__(self):
            return self

        if not PY_352:
            __aiter__ = asyncio.coroutine(__aiter__)

        @asyncio.coroutine
        def __anext__(self):
            ret = yield from self.fetchone()
            if ret is not None:
                return ret
            else:
                raise StopAsyncIteration

        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
            return
