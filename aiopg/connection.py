import asyncio
import contextlib
import errno
import platform
import select
import sys
import traceback
import warnings
import weakref
from collections.abc import Mapping

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import POLL_ERROR, POLL_OK, POLL_READ, POLL_WRITE

from .cursor import Cursor
from .utils import _ContextManager, create_future, get_running_loop

__all__ = ('connect',)

TIMEOUT = 60.0

# Windows specific error code, not in errno for some reason, and doesnt map
# to OSError.errno EBADF
WSAENOTSOCK = 10038


def connect(dsn=None, *, timeout=TIMEOUT, enable_json=True,
            enable_hstore=True, enable_uuid=True, echo=False, **kwargs):
    """A factory for connecting to PostgreSQL.

    The coroutine accepts all parameters that psycopg2.connect() does
    plus optional keyword-only `timeout` parameters.

    Returns instantiated Connection object.

    """
    coro = Connection(
        dsn, timeout, bool(echo),
        enable_hstore=enable_hstore,
        enable_uuid=enable_uuid,
        enable_json=enable_json,
        **kwargs
    )

    return _ContextManager(coro)


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

    def __init__(
            self, dsn, timeout, echo,
            *, enable_json=True, enable_hstore=True,
            enable_uuid=True, **kwargs
    ):
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore
        self._enable_uuid = enable_uuid
        self._loop = get_running_loop(kwargs.pop('loop', None) is not None)
        self._waiter = create_future(self._loop)

        kwargs['async_'] = kwargs.pop('async', True)
        self._conn = psycopg2.connect(dsn, **kwargs)

        self._dsn = self._conn.dsn
        assert self._conn.isexecuting(), "Is conn an async at all???"
        self._fileno = self._conn.fileno()
        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._writing = False
        self._cancelling = False
        self._cancellation_waiter = None
        self._echo = echo
        self._cursor_instance = None
        self._notifies = asyncio.Queue()
        self._weakref = weakref.ref(self)
        self._loop.add_reader(self._fileno, self._ready, self._weakref)

        if self._loop.get_debug():
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

    async def _poll(self, waiter, timeout):
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready(self._weakref)

        async def cancel():
            self._waiter = create_future(self._loop)
            self._cancelling = True
            self._cancellation_waiter = self._waiter
            self._conn.cancel()
            if not self._conn.isexecuting():
                return
            try:
                await asyncio.wait_for(self._waiter, timeout)
            except psycopg2.extensions.QueryCanceledError:
                pass
            except asyncio.TimeoutError:
                self._close()

        try:
            await asyncio.wait_for(self._waiter, timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
            await asyncio.shield(cancel())
            raise exc
        except psycopg2.extensions.QueryCanceledError as exc:
            self._loop.call_exception_handler({
                'message': exc.pgerror,
                'exception': exc,
                'future': self._waiter,
            })
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

        NOTE: as of [TODO] any previously created created cursor from this
            connection will be closed
        """
        self._last_usage = self._loop.time()
        coro = self._cursor(name=name, cursor_factory=cursor_factory,
                            scrollable=scrollable, withhold=withhold,
                            timeout=timeout)
        return _ContextManager(coro)

    async def _cursor(self, name=None, cursor_factory=None,
                      scrollable=None, withhold=False, timeout=None):

        if not self.closed_cursor:
            warnings.warn(('You can only have one cursor per connection. '
                           'The cursor for connection will be closed forcibly'
                           ' {!r}.').format(self), ResourceWarning)

        self.free_cursor()

        if timeout is None:
            timeout = self._timeout

        impl = await self._cursor_impl(name=name,
                                       cursor_factory=cursor_factory,
                                       scrollable=scrollable,
                                       withhold=withhold)
        self._cursor_instance = Cursor(self, impl, timeout, self._echo)
        return self._cursor_instance

    async def _cursor_impl(self, name=None, cursor_factory=None,
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
        self.free_cursor()

        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_exception(
                psycopg2.OperationalError("Connection closed"))

    @property
    def closed_cursor(self):
        if not self._cursor_instance:
            return True

        return bool(self._cursor_instance.closed)

    def free_cursor(self):
        if not self.closed_cursor:
            self._cursor_instance.close()
            self._cursor_instance = None

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

    async def commit(self):
        raise psycopg2.ProgrammingError(
            "commit cannot be used in asynchronous mode")

    async def rollback(self):
        raise psycopg2.ProgrammingError(
            "rollback cannot be used in asynchronous mode")

    # TPC

    async def xid(self, format_id, gtrid, bqual):
        return self._conn.xid(format_id, gtrid, bqual)

    async def tpc_begin(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_begin cannot be used in asynchronous mode")

    async def tpc_prepare(self):
        raise psycopg2.ProgrammingError(
            "tpc_prepare cannot be used in asynchronous mode")

    async def tpc_commit(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_commit cannot be used in asynchronous mode")

    async def tpc_rollback(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_rollback cannot be used in asynchronous mode")

    async def tpc_recover(self):
        raise psycopg2.ProgrammingError(
            "tpc_recover cannot be used in asynchronous mode")

    async def cancel(self):
        """Cancel the current database operation."""
        if self._waiter is None:
            return

        async def cancel():
            self._conn.cancel()
            try:
                await self._waiter
            except psycopg2.extensions.QueryCanceledError:
                pass

        await asyncio.shield(cancel())

    async def reset(self):
        raise psycopg2.ProgrammingError(
            "reset cannot be used in asynchronous mode")

    @property
    def dsn(self):
        """DSN connection string.

        Read-only attribute representing dsn connection string used
        for connectint to PostgreSQL server.

        """
        return self._dsn

    async def set_session(self, *, isolation_level=None, readonly=None,
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

    async def set_isolation_level(self, val):
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        self._conn.set_isolation_level(val)

    @property
    def encoding(self):
        """Client encoding for SQL operations."""
        return self._conn.encoding

    async def set_client_encoding(self, val):
        self._conn.set_client_encoding(val)

    @property
    def notices(self):
        """A list of all db messages sent to the client during the session."""
        return self._conn.notices

    @property
    def cursor_factory(self):
        """The default cursor factory used by .cursor()."""
        return self._conn.cursor_factory

    async def get_backend_pid(self):
        """Returns the PID of the backend server process."""
        return self._conn.get_backend_pid()

    async def get_parameter_status(self, parameter):
        """Look up a current parameter setting of the server."""
        return self._conn.get_parameter_status(parameter)

    async def get_transaction_status(self):
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

    async def lobject(self, *args, **kwargs):
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

    def __repr__(self):
        msg = (
            '<'
            '{module_name}::{class_name} '
            'isexecuting={isexecuting}, '
            'closed={closed}, '
            'echo={echo}, '
            'cursor={cursor}'
            '>'
        )
        return msg.format(
            module_name=type(self).__module__,
            class_name=type(self).__name__,
            echo=self.echo,
            isexecuting=self._isexecuting(),
            closed=bool(self.closed),
            cursor=repr(self._cursor_instance)
        )

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

    async def _get_oids(self):
        cur = await self.cursor()
        rv0, rv1 = [], []
        try:
            await cur.execute(
                "SELECT t.oid, typarray "
                "FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid "
                "WHERE typname = 'hstore';"
            )

            async for oids in cur:
                if isinstance(oids, Mapping):
                    rv0.append(oids['oid'])
                    rv1.append(oids['typarray'])
                else:
                    rv0.append(oids[0])
                    rv1.append(oids[1])
        finally:
            cur.close()

        return tuple(rv0), tuple(rv1)

    async def _connect(self):
        try:
            await self._poll(self._waiter, self._timeout)
        except Exception:
            self.close()
            raise
        if self._enable_json:
            extras.register_default_json(self._conn)
        if self._enable_uuid:
            extras.register_uuid(conn_or_curs=self._conn)
        if self._enable_hstore:
            oids = await self._get_oids()
            if oids is not None:
                oid, array_oid = oids
                extras.register_hstore(
                    self._conn,
                    oid=oid,
                    array_oid=array_oid
                )

        return self

    def __await__(self):
        return self._connect().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
