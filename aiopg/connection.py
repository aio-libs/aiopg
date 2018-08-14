import asyncio
import contextlib
import errno
import select
import sys
import traceback
import warnings
import weakref
import platform

import psycopg2
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR)
from psycopg2 import extras

from .cursor import Cursor
from .utils import _ContextManager, PY_35, create_future


__all__ = ('connect',)


TIMEOUT = 60.0
PY_341 = sys.version_info >= (3, 4, 1)

# Windows specific error code, not in errno for some reason, and doesnt map
# to OSError.errno EBADF
WSAENOTSOCK = 10038


@asyncio.coroutine
def _enable_hstore(conn):
    cur = yield from conn.cursor()
    yield from cur.execute("""\
        SELECT t.oid, typarray
        FROM pg_type t JOIN pg_namespace ns
            ON typnamespace = ns.oid
        WHERE typname = 'hstore';
        """)
    rv0, rv1 = [], []
    for oids in (yield from cur.fetchall()):
        rv0.append(oids[0])
        rv1.append(oids[1])

    cur.close()
    return tuple(rv0), tuple(rv1)


def connect(dsn=None, *, timeout=TIMEOUT, loop=None, enable_json=True,
            enable_hstore=True, enable_uuid=True, echo=False, **kwargs):
    """A factory for connecting to PostgreSQL.

    The coroutine accepts all parameters that psycopg2.connect() does
    plus optional keyword-only `loop` and `timeout` parameters.

    Returns instantiated Connection object.

    """
    coro = _connect(dsn=dsn, timeout=timeout, loop=loop,
                    enable_json=enable_json, enable_hstore=enable_hstore,
                    enable_uuid=enable_uuid, echo=echo, **kwargs)
    return _ContextManager(coro)


@asyncio.coroutine
def _connect(dsn=None, *, timeout=TIMEOUT, loop=None, enable_json=True,
             enable_hstore=True, enable_uuid=True, echo=False, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    waiter = create_future(loop)
    conn = Connection(dsn, loop, timeout, waiter, bool(echo), **kwargs)
    try:
        yield from conn._poll(waiter, timeout)
    except Exception:
        conn.close()
        raise
    if enable_json:
        extras.register_default_json(conn._conn)
    if enable_uuid:
        extras.register_uuid(conn_or_curs=conn._conn)
    if enable_hstore:
        oids = yield from _enable_hstore(conn)
        if oids is not None:
            oid, array_oid = oids
            extras.register_hstore(conn._conn, oid=oid, array_oid=array_oid)
    return conn


def _is_bad_descriptor_error(os_error):
    if platform.system() == 'Windows':  # pragma: no cover
        return os_error.winerror == WSAENOTSOCK
    else:
        return os_error.errno == errno.EBADF


class Connection:
    """Low-level asynchronous interface for wrapped psycopg2 connection.

    The Connection instance encapsulates a database session.
    Provides support for creating asynchronous cursors.

    """

    _source_traceback = None

    def __init__(self, dsn, loop, timeout, waiter, echo, **kwargs):
        self._loop = loop
        self._conn = psycopg2.connect(dsn, async_=True, **kwargs)
        self._dsn = self._conn.dsn
        assert self._conn.isexecuting(), "Is conn an async at all???"
        self._fileno = self._conn.fileno()
        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._waiter = waiter
        self._writing = False
        self._cancelling = False
        self._cancellation_waiter = None
        self._echo = echo
        self._notifies = asyncio.Queue(loop=loop)
        self._weakref = weakref.ref(self)
        self._loop.add_reader(self._fileno, self._ready, self._weakref)
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    @staticmethod
    def _ready(weak_self):
        self = weak_self()
        if self is None:
            return

        waiter = self._waiter

        try:
            state = self._conn.poll()
            while self._conn.notifies:
                notify = self._conn.notifies.pop(0)
                self._notifies.put_nowait(notify)
        except (psycopg2.Warning, psycopg2.Error) as exc:
            if self._fileno is not None:
                try:
                    select.select([self._fileno], [], [], 0)
                except OSError as os_exc:
                    if _is_bad_descriptor_error(os_exc):
                        with contextlib.suppress(OSError):
                            self._loop.remove_reader(self._fileno)
                            # forget a bad file descriptor, don't try to
                            # touch it
                            self._fileno = None

            try:
                if self._writing:
                    self._writing = False
                    if self._fileno is not None:
                        self._loop.remove_writer(self._fileno)
            except OSError as exc2:
                if exc2.errno != errno.EBADF:
                    # EBADF is ok for closed file descriptor
                    # chain exception otherwise
                    exc2.__cause__ = exc
                    exc = exc2
            if waiter is not None and not waiter.done():
                waiter.set_exception(exc)
        else:
            if self._fileno is None:
                # connection closed
                if waiter is not None and not waiter.done():
                    waiter.set_exception(
                        psycopg2.OperationalError("Connection closed"))
            if state == POLL_OK:
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)
            elif state == POLL_READ:
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
            elif state == POLL_WRITE:
                if not self._writing:
                    self._loop.add_writer(self._fileno, self._ready, weak_self)
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
        self.close()
        if self._waiter and not self._waiter.done():
            self._waiter.set_exception(psycopg2.OperationalError(message))

    def _create_waiter(self, func_name):
        if self._waiter is not None:
            if self._cancelling:
                if not self._waiter.done():
                    raise RuntimeError('%s() called while connection is '
                                       'being cancelled' % func_name)
            else:
                raise RuntimeError('%s() called while another coroutine is '
                                   'already waiting for incoming '
                                   'data' % func_name)
        self._waiter = create_future(self._loop)
        return self._waiter

    @asyncio.coroutine
    def _poll(self, waiter, timeout):
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready(self._weakref)

        @asyncio.coroutine
        def cancel():
            self._waiter = create_future(self._loop)
            self._cancelling = True
            self._cancellation_waiter = self._waiter
            self._conn.cancel()
            if not self._conn.isexecuting():
                return
            try:
                yield from asyncio.wait_for(self._waiter, timeout,
                                            loop=self._loop)
            except psycopg2.extensions.QueryCanceledError:
                pass
            except asyncio.TimeoutError:
                self._close()

        try:
            yield from asyncio.wait_for(self._waiter, timeout, loop=self._loop)
        except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
            yield from asyncio.shield(cancel(), loop=self._loop)
            raise exc
        except psycopg2.extensions.QueryCanceledError:
            raise asyncio.CancelledError
        finally:
            if self._cancelling:
                self._cancelling = False
                if self._waiter is self._cancellation_waiter:
                    self._waiter = None
                self._cancellation_waiter = None
            else:
                self._waiter = None

    def _isexecuting(self):
        return self._conn.isexecuting()

    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False, timeout=None):
        """A coroutine that returns a new cursor object using the connection.

        *cursor_factory* argument can be used to create non-standard
         cursors. The argument must be subclass of
         `psycopg2.extensions.cursor`.

        *name*, *scrollable* and *withhold* parameters are not supported by
        psycopg in asynchronous mode.

        """
        self._last_usage = self._loop.time()
        coro = self._cursor(name=name, cursor_factory=cursor_factory,
                            scrollable=scrollable, withhold=withhold,
                            timeout=timeout)
        return _ContextManager(coro)

    @asyncio.coroutine
    def _cursor(self, name=None, cursor_factory=None,
                scrollable=None, withhold=False, timeout=None):
        if timeout is None:
            timeout = self._timeout

        impl = yield from self._cursor_impl(name=name,
                                            cursor_factory=cursor_factory,
                                            scrollable=scrollable,
                                            withhold=withhold)
        return Cursor(self, impl, timeout, self._echo)

    @asyncio.coroutine
    def _cursor_impl(self, name=None, cursor_factory=None,
                     scrollable=None, withhold=False):
        if cursor_factory is None:
            impl = self._conn.cursor(name=name,
                                     scrollable=scrollable, withhold=withhold)
        else:
            impl = self._conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold)
        return impl

    def _close(self):
        """Remove the connection from the event_loop and close it."""
        # N.B. If connection contains uncommitted transaction the
        # transaction will be discarded
        if self._fileno is not None:
            self._loop.remove_reader(self._fileno)
            if self._writing:
                self._writing = False
                self._loop.remove_writer(self._fileno)
        self._conn.close()
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_exception(
                psycopg2.OperationalError("Connection closed"))

    def close(self):
        self._close()
        ret = create_future(self._loop)
        ret.set_result(None)
        return ret

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
    def cancel(self):
        """Cancel the current database operation."""
        if self._waiter is None:
            return

        @asyncio.coroutine
        def cancel():
            self._conn.cancel()
            try:
                yield from self._waiter
            except psycopg2.extensions.QueryCanceledError:
                pass

        yield from asyncio.shield(cancel(), loop=self._loop)

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

    @property
    def last_usage(self):
        """Return time() when connection was used."""
        return self._last_usage

    @property
    def echo(self):
        """Return echo mode status."""
        return self._echo

    if PY_341:  # pragma: no branch
        def __del__(self):
            try:
                _conn = self._conn
            except AttributeError:
                return
            if _conn is not None and not _conn.closed:
                self.close()
                warnings.warn("Unclosed connection {!r}".format(self),
                              ResourceWarning)

                context = {'connection': self,
                           'message': 'Unclosed connection'}
                if self._source_traceback is not None:
                    context['source_traceback'] = self._source_traceback
                self._loop.call_exception_handler(context)

    @property
    def notifies(self):
        """Return notification queue."""
        return self._notifies

    if PY_35:  # pragma: no branch
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
