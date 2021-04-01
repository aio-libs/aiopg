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
from types import TracebackType
from typing import Any, Optional, Type

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from .cursor import Cursor
from .utils import _ContextManager, create_completed_future, get_running_loop

__all__ = ('connect',)

TIMEOUT = 60.0

# Windows specific error code, not in errno for some reason, and doesnt map
# to OSError.errno EBADF
WSAENOTSOCK = 10038


def connect(
    dsn: Optional[str] = None,
    *,
    timeout: float = TIMEOUT,
    enable_json: bool = True,
    enable_hstore: bool = True,
    enable_uuid: bool = True,
    echo: bool = False,
    **kwargs: Any
) -> _ContextManager['Connection']:
    """A factory for connecting to PostgreSQL.

    The coroutine accepts all parameters that psycopg2.connect() does
    plus optional keyword-only `timeout` parameters.

    Returns instantiated Connection object.

    """
    coro = Connection(
        dsn,
        timeout,
        bool(echo),
        enable_hstore=enable_hstore,
        enable_uuid=enable_uuid,
        enable_json=enable_json,
        **kwargs
    )

    return _ContextManager[Connection](coro, lambda x: x.close())


def _is_bad_descriptor_error(os_error: OSError) -> bool:
    if platform.system() == 'Windows':  # pragma: no cover
        winerror = int(getattr(os_error, "winerror", 0))
        return winerror == WSAENOTSOCK
    return os_error.errno == errno.EBADF


class Connection:
    """Low-level asynchronous interface for wrapped psycopg2 connection.

    The Connection instance encapsulates a database session.
    Provides support for creating asynchronous cursors.

    """

    _source_traceback = None

    def __init__(
            self,
            dsn: Optional[str],
            timeout: float,
            echo: bool = False,
            enable_json: bool = True,
            enable_hstore: bool = True,
            enable_uuid: bool = True,
            **kwargs: Any,
    ):
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore
        self._enable_uuid = enable_uuid
        self._loop = get_running_loop()
        self._waiter: Optional[asyncio.Future[None]] = (
            self._loop.create_future()
        )

        kwargs['async_'] = kwargs.pop('async', True)
        kwargs.pop('loop', None)  # backward compatibility
        self._conn = psycopg2.connect(dsn, **kwargs)

        self._dsn = self._conn.dsn
        assert self._conn.isexecuting(), "Is conn an async at all???"
        self._fileno = self._conn.fileno()
        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._writing = False
        self._echo = echo
        self._notifies = asyncio.Queue()
        self._weakref = weakref.ref(self)
        self._loop.add_reader(self._fileno, self._ready, self._weakref)

        if self._loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    @staticmethod
    def _ready(weak_self: weakref.ref['Connection']) -> None:
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
            if state == psycopg2.extensions.POLL_OK:
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)
            elif state == psycopg2.extensions.POLL_READ:
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
            elif state == psycopg2.extensions.POLL_WRITE:
                if not self._writing:
                    self._loop.add_writer(self._fileno, self._ready, weak_self)
                    self._writing = True
            elif state == psycopg2.extensions.POLL_ERROR:
                self._fatal_error("Fatal error on aiopg connection: "
                                  "POLL_ERROR from underlying .poll() call")
            else:
                self._fatal_error(f"Fatal error on aiopg connection: "
                                  f"unknown answer {state} from underlying "
                                  f".poll() call")

    def _fatal_error(self, message: str) -> None:
        # Should be called from exception handler only.
        self._loop.call_exception_handler({
            'message': message,
            'connection': self,
        })
        self.close()
        if self._waiter and not self._waiter.done():
            self._waiter.set_exception(psycopg2.OperationalError(message))

    def _create_waiter(self, func_name: str) -> asyncio.Future[None]:
        if self._waiter is not None:
            raise RuntimeError(f'{func_name}() called while another coroutine '
                               f'is already waiting for incoming data')
        self._waiter = self._loop.create_future()
        return self._waiter

    async def _poll(
        self, waiter: asyncio.Future[None], timeout: float
    ) -> None:
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready(self._weakref)

        try:
            await asyncio.wait_for(self._waiter, timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
            await asyncio.shield(self.close())
            raise exc
        except psycopg2.extensions.QueryCanceledError as exc:
            self._loop.call_exception_handler({
                'message': exc.pgerror,
                'exception': exc,
                'future': self._waiter,
            })
            raise asyncio.CancelledError
        finally:
            self._waiter = None

    def isexecuting(self) -> bool:
        return self._conn.isexecuting()  # type: ignore

    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False, timeout=None,
               isolation_level=None):
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
                            timeout=timeout, isolation_level=isolation_level)
        return _ContextManager(coro)

    async def _cursor(self, name=None, cursor_factory=None,
                      scrollable=None, withhold=False, timeout=None,
                      isolation_level=None):

        if timeout is None:
            timeout = self._timeout

        impl = await self._cursor_impl(
            name=name,
            cursor_factory=cursor_factory,
            scrollable=scrollable,
            withhold=withhold
        )  # type: ignore
        cursor = Cursor(
            self, impl, timeout, self._echo, isolation_level
        )
        return cursor

    async def _cursor_impl(self, name=None, cursor_factory=None,
                           scrollable=None, withhold=False):
        if cursor_factory is None:
            impl = self._conn.cursor(name=name,
                                     scrollable=scrollable, withhold=withhold)
        else:
            impl = self._conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold)
        return impl

    def _close(self) -> None:
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

    def close(self) -> asyncio.Future[None]:
        self._close()
        return create_completed_future(self._loop)

    @property
    def closed(self) -> bool:
        """Connection status.

        Read-only attribute reporting whether the database connection is
        open (False) or closed (True).

        """
        return self._conn.closed  # type: ignore

    @property
    def raw(self) -> Any:
        """Underlying psycopg connection object, readonly"""
        return self._conn

    async def commit(self) -> None:
        raise psycopg2.ProgrammingError(
            "commit cannot be used in asynchronous mode")

    async def rollback(self) -> None:
        raise psycopg2.ProgrammingError(
            "rollback cannot be used in asynchronous mode")

    # TPC

    async def xid(self, format_id, gtrid, bqual):
        return self._conn.xid(format_id, gtrid, bqual)

    async def tpc_begin(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_begin cannot be used in asynchronous mode")

    async def tpc_prepare(self) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_prepare cannot be used in asynchronous mode")

    async def tpc_commit(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_commit cannot be used in asynchronous mode")

    async def tpc_rollback(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_rollback cannot be used in asynchronous mode")

    async def tpc_recover(self) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_recover cannot be used in asynchronous mode")

    async def cancel(self) -> None:
        raise psycopg2.ProgrammingError(
            "cancel cannot be used in asynchronous mode")

    async def reset(self) -> None:
        raise psycopg2.ProgrammingError(
            "reset cannot be used in asynchronous mode")

    @property
    def dsn(self) -> Optional[str]:
        """DSN connection string.

        Read-only attribute representing dsn connection string used
        for connectint to PostgreSQL server.

        """
        return self._dsn  # type: ignore

    async def set_session(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "set_session cannot be used in asynchronous mode")

    @property
    def autocommit(self) -> bool:
        """Autocommit status"""
        return self._conn.autocommit  # type: ignore

    @autocommit.setter
    def autocommit(self, val: bool) -> None:
        """Autocommit status"""
        self._conn.autocommit = val

    @property
    def isolation_level(self) -> int:
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        return self._conn.isolation_level  # type: ignore

    async def set_isolation_level(self, val: int) -> None:
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        self._conn.set_isolation_level(val)

    @property
    def encoding(self) -> str:
        """Client encoding for SQL operations."""
        return self._conn.encoding  # type: ignore

    async def set_client_encoding(self, val: str) -> None:
        self._conn.set_client_encoding(val)

    @property
    def notices(self):
        """A list of all db messages sent to the client during the session."""
        return self._conn.notices

    @property
    def cursor_factory(self) -> Any:
        """The default cursor factory used by .cursor()."""
        return self._conn.cursor_factory

    async def get_backend_pid(self) -> int:
        """Returns the PID of the backend server process."""
        return self._conn.get_backend_pid()  # type: ignore

    async def get_parameter_status(self, parameter: str) -> Optional[str]:
        """Look up a current parameter setting of the server."""
        return self._conn.get_parameter_status(parameter)

    async def get_transaction_status(self) -> int:
        """Return the current session transaction status as an integer."""
        return self._conn.get_transaction_status()  # type: ignore

    @property
    def protocol_version(self) -> int:
        """A read-only integer representing protocol being used."""
        return self._conn.protocol_version  # type: ignore

    @property
    def server_version(self) -> int:
        """A read-only integer representing the backend version."""
        return self._conn.server_version  # type: ignore

    @property
    def status(self) -> int:
        """A read-only integer representing the status of the connection."""
        return self._conn.status  # type: ignore

    async def lobject(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "lobject cannot be used in asynchronous mode")

    @property
    def timeout(self) -> float:
        """Return default timeout for connection operations."""
        return self._timeout

    @property
    def last_usage(self) -> float:
        """Return time() when connection was used."""
        return self._last_usage

    @property
    def echo(self) -> bool:
        """Return echo mode status."""
        return self._echo

    def __repr__(self) -> str:
        return (
            f'<'
            f'{type(self).__module__}::{type(self).__name__} '
            f'isexecuting={self.isexecuting()}, '
            f'closed={self.closed}, '
            f'echo={self.echo}, '
            f'>'
        )

    def __del__(self) -> None:
        try:
            _conn = self._conn
        except AttributeError:
            return
        if _conn is not None and not _conn.closed:
            self.close()
            warnings.warn(f"Unclosed connection {self!r}",
                          ResourceWarning)

            context = {'connection': self,
                       'message': 'Unclosed connection'}
            if self._source_traceback is not None:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)

    @property
    def notifies(self) -> asyncio.Queue[Any]:
        """Return notification queue."""
        return self._notifies

    async def _get_oids(self):
        cursor = await self.cursor()
        rv0, rv1 = [], []
        try:
            await cursor.execute(
                "SELECT t.oid, typarray "
                "FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid "
                "WHERE typname = 'hstore';"
            )

            async for oids in cursor:
                if isinstance(oids, Mapping):
                    rv0.append(oids['oid'])
                    rv1.append(oids['typarray'])
                else:
                    rv0.append(oids[0])
                    rv1.append(oids[1])
        finally:
            cursor.close()

        return tuple(rv0), tuple(rv1)

    async def _connect(self) -> 'Connection':
        try:
            await self._poll(self._waiter, self._timeout)  # type: ignore
        except BaseException:
            await asyncio.shield(self.close())
            raise
        if self._enable_json:
            psycopg2.extras.register_default_json(self._conn)
        if self._enable_uuid:
            psycopg2.extras.register_uuid(conn_or_curs=self._conn)
        if self._enable_hstore:
            oids = await self._get_oids()
            if oids is not None:
                oid, array_oid = oids
                psycopg2.extras.register_hstore(
                    self._conn,
                    oid=oid,
                    array_oid=array_oid
                )

        return self

    def __await__(self):
        return self._connect().__await__()

    async def __aenter__(self) -> 'Connection':
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()
