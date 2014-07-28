import asyncio

import psycopg2
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR)

from .cursor import Cursor


__all__ = ('connect',)


TIMEOUT = 60.


@asyncio.coroutine
def connect(dsn=None, *, timeout=TIMEOUT, loop=None, **kwargs):
    """A factory for connecting to PostgreSQL.

    The coroutine accepts all parameters that psycopg2.connect() does
    plus optional keyword-only `loop` and `timeout` parameters.

    Returns instantiated Connection object.

    """
    if loop is None:
        loop = asyncio.get_event_loop()

    waiter = asyncio.Future(loop=loop)
    conn = Connection(dsn, loop, timeout, waiter, **kwargs)
    yield from conn._poll(waiter, timeout)
    return conn


class Connection:
    """Low-level asynchronous interface for wrapped psycopg2 connection.

    The Connection instance encapsulates a database session.
    Provides support for creating asynchronous cursors.

    """

    def __init__(self, dsn, loop, timeout, waiter, **kwargs):
        self._loop = loop
        self._conn = psycopg2.connect(dsn, async=True, **kwargs)
        self._dsn = self._conn.dsn
        assert self._conn.isexecuting(), "Is conn async at all???"
        self._fileno = self._conn.fileno()
        self._timeout = timeout
        self._waiter = waiter
        self._reading = False
        self._writing = False
        self._ready()

    def _ready(self):
        if self._waiter is None:
            self._fatal_error("Fatal error on aiopg connection: "
                              "bad state in _ready callback")
            return

        try:
            state = self._conn.poll()
        except (psycopg2.Warning, psycopg2.Error) as exc:
            if self._reading:
                self._loop.remove_reader(self._fileno)
                self._reading = False
            if self._writing:
                self._loop.remove_writer(self._fileno)
                self._writing = False
            if not self._waiter.cancelled():
                self._waiter.set_exception(exc)
        else:
            if state == POLL_OK:
                if self._reading:
                    self._loop.remove_reader(self._fileno)
                    self._reading = False
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
                if not self._waiter.cancelled():
                    self._waiter.set_result(None)
            elif state == POLL_READ:
                if not self._reading:
                    self._loop.add_reader(self._fileno, self._ready)
                    self._reading = True
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
            elif state == POLL_WRITE:
                if self._reading:
                    self._loop.remove_reader(self._fileno)
                    self._reading = False
                if not self._writing:
                    self._loop.add_writer(self._fileno, self._ready)
                    self._writing = True
            elif state == POLL_ERROR:
                self._fatal_error("Fatal error on aiopg connection: "
                                  "POLL_ERROR from underlying .poll() call")
            else:
                self._fatal_error("Fatal error on aiopg connection: "
                                  "unknown answer {} from underlying "
                                  ".poll() call"
                                  .format(state))

    def _fatal_error(self, message):
        # Should be called from exception handler only.
        self._loop.call_exception_handler({
            'message': message,
            'connection': self,
            })
        self._close()
        if self._waiter and not self._waiter.done():
            self._waiter.set_exception(psycopg2.OperationalError(message))

    def _create_waiter(self, func_name):
        if self._waiter is not None:
            raise RuntimeError('%s() called while another coroutine is '
                               'already waiting for incoming data' % func_name)
        self._waiter = asyncio.Future(loop=self._loop)
        return self._waiter

    @asyncio.coroutine
    def _poll(self, waiter, timeout):
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready()
        try:
            yield from asyncio.wait_for(self._waiter, timeout, loop=self._loop)
        finally:
            self._waiter = None

    def _isexecuting(self):
        return self._conn.isexecuting()

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False, timeout=None):
        """A coroutine that returns a new cursor object using the connection.

        *cursor_factory* argument can be used to create non-standard
         cursors. The argument must be suclass of
         `psycopg2.extensions.cursor`.

        *name*, *scrollable* and *withhold* parameters are not supported by
        psycopg in asynchronous mode.

        """
        if timeout is None:
            timeout = self._timeout

        impl = yield from self._cursor(name=name,
                                       cursor_factory=cursor_factory,
                                       scrollable=scrollable,
                                       withhold=withhold)
        return Cursor(self, impl, timeout)

    @asyncio.coroutine
    def _cursor(self, name=None, cursor_factory=None,
                scrollable=None, withhold=False):
        if cursor_factory is None:
            impl = self._conn.cursor(name=name,
                                     scrollable=scrollable, withhold=withhold)
        else:
            impl = self._conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold)
        return impl

    @asyncio.coroutine
    def close(self):
        """Remove the connection from the event_loop and close it."""
        # N.B. If connection contains uncommitted transaction the
        # transaction will be discarded
        self._close()

    def _close(self):
        if self._reading:
            self._loop.remove_reader(self._fileno)
            self._reading = False
        if self._writing:
            self._loop.remove_writer(self._fileno)
            self._writing = False
        self._conn.close()
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_exception(
                psycopg2.OperationalError("Connection closed"))

    @property
    def closed(self):
        """Connection status.

        Read-only attribute reporting whether the database connection is
        open (False) or closed (True).

        """
        return self._conn.closed

    @property
    def raw(self):
        """Underlying psycopg connection object, readonly"""
        return self._conn

    @asyncio.coroutine
    def commit(self):
        raise psycopg2.ProgrammingError(
            "commit cannot be used in asynchronous mode")

    @asyncio.coroutine
    def rollback(self):
        raise psycopg2.ProgrammingError(
            "rollback cannot be used in asynchronous mode")

    # TPC

    @asyncio.coroutine
    def xid(self, format_id, gtrid, bqual):
        return self._conn.xid(format_id, gtrid, bqual)

    @asyncio.coroutine
    def tpc_begin(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_begin cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_prepare(self):
        raise psycopg2.ProgrammingError(
            "tpc_prepare cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_commit(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_commit cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_rollback(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_rollback cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_recover(self):
        raise psycopg2.ProgrammingError(
            "tpc_recover cannot be used in asynchronous mode")

    @asyncio.coroutine
    def cancel(self, timeout=None):
        """Cancel the current database operation."""
        waiter = self._create_waiter('cancel')
        self._conn.cancel()
        if timeout is None:
            timeout = self._timeout
        yield from self._poll(waiter, timeout)

    @asyncio.coroutine
    def reset(self):
        raise psycopg2.ProgrammingError(
            "reset cannot be used in asynchronous mode")

    @property
    def dsn(self):
        """DSN connection string.

        Read-only attribute representing dsn connection string used
        for connectint to PostgreSQL server.

        """
        return self._dsn

    @asyncio.coroutine
    def set_session(self, *, isolation_level=None, readonly=None,
                    deferrable=None, autocommit=None):
        raise psycopg2.ProgrammingError(
            "set_session cannot be used in asynchronous mode")

    @property
    def autocommit(self):
        """Autocommit status"""
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, val):
        """Autocommit status"""
        self._conn.autocommit = val

    @property
    def isolation_level(self):
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        return self._conn.isolation_level

    @asyncio.coroutine
    def set_isolation_level(self, val):
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        self._conn.set_isolation_level(val)

    @property
    def encoding(self):
        """Client encoding for SQL operations."""
        return self._conn.encoding

    @asyncio.coroutine
    def set_client_encoding(self, val):
        self._conn.set_client_encoding(val)

    @property
    def notices(self):
        """A list of all db messages sent to the client during the session."""
        return self._conn.notices

    @property
    def cursor_factory(self):
        """The default cursor factory used by .cursor()."""
        return self._conn.cursor_factory

    @asyncio.coroutine
    def get_backend_pid(self):
        """Returns the PID of the backend server process."""
        return self._conn.get_backend_pid()

    @asyncio.coroutine
    def get_parameter_status(self, parameter):
        """Look up a current parameter setting of the server."""
        return self._conn.get_parameter_status(parameter)

    @asyncio.coroutine
    def get_transaction_status(self):
        """Return the current session transaction status as an integer."""
        return self._conn.get_transaction_status()

    @property
    def protocol_version(self):
        """A read-only integer representing protocol being used."""
        return self._conn.protocol_version

    @property
    def server_version(self):
        """A read-only integer representing the backend version."""
        return self._conn.server_version

    @property
    def status(self):
        """A read-only integer representing the status of the connection."""
        return self._conn.status

    @asyncio.coroutine
    def lobject(self, *args, **kwargs):
        raise psycopg2.ProgrammingError(
            "lobject cannot be used in asynchronous mode")

    @property
    def timeout(self):
        """Return default timeout for connection operations."""
        return self._timeout
