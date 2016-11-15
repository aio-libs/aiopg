import asyncio
import json

import aiopg
from aiopg.sa import exc
from sqlalchemy import ColumnDefault

from .connection import SAConnection
from .exc import InvalidRequestError
from ..connection import TIMEOUT
from ..utils import PY_35, _PoolContextManager, _PoolAcquireContextManager

try:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
    from sqlalchemy.dialects.postgresql.psycopg2 import PGCompiler_psycopg2
except ImportError:  # pragma: no cover
    raise ImportError('aiopg.sa requires sqlalchemy')


class APGCompiler_psycopg2(PGCompiler_psycopg2):
    def construct_params(self, params=None, _group_number=None,
                         _check=True, table=None):
        """return a dictionary of bind parameter keys and values"""
        fields = table.c if table is not None else {}

        if params:
            pd = {}
            for bindparam in self.bind_names:
                name = self.bind_names[bindparam]
                if bindparam.key in params:
                    pd[name] = params[bindparam.key]
                elif name in params:
                    pd[name] = params[name]

                elif _check and bindparam.required:
                    if _group_number:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r, "
                            "in parameter group %d" %
                            (bindparam.key, _group_number))
                    else:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r"
                            % bindparam.key)

                elif bindparam.callable:
                    pd[name] = bindparam.effective_value
                else:
                    pd[name] = bindparam.value
            return pd
        else:
            pd = {}
            if fields:
                fields = {f.key: f.default for f in fields
                          if isinstance(f.default, ColumnDefault)}

            for bindparam in self.bind_names:
                if _check and bindparam.required:
                    if _group_number:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r, "
                            "in parameter group %d" %
                            (bindparam.key, _group_number))
                    else:
                        raise exc.InvalidRequestError(
                            "A value is required for bind parameter %r"
                            % bindparam.key)

                if bindparam.callable:
                    value = bindparam.effective_value
                else:
                    value = bindparam.value

                if value is None and bindparam.key in fields:
                    field = fields[bindparam.key]
                    if field.is_callable:
                        value = field.arg(table)
                    else:
                        value = field.arg

                pd[self.bind_names[bindparam]] = value

            return pd


_dialect = PGDialect_psycopg2(json_serializer=json.dumps,
                              json_deserializer=lambda x: x)

_dialect.statement_compiler = APGCompiler_psycopg2
_dialect.implicit_returning = True
_dialect.supports_native_enum = True
_dialect.supports_smallserial = True  # 9.2+
_dialect._backslash_escapes = False
_dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
_dialect._has_native_hstore = True


def create_engine(dsn=None, *, minsize=1, maxsize=10, loop=None,
                  dialect=_dialect, timeout=TIMEOUT, **kwargs):
    """A coroutine for Engine creation.

    Returns Engine instance with embedded connection pool.

    The pool has *minsize* opened connections to PostgreSQL server.
    """

    coro = _create_engine(dsn=dsn, minsize=minsize, maxsize=maxsize,
                          loop=loop, dialect=dialect, timeout=timeout,
                          **kwargs)
    return _EngineContextManager(coro)


@asyncio.coroutine
def _create_engine(dsn=None, *, minsize=1, maxsize=10, loop=None,
                   dialect=_dialect, timeout=TIMEOUT, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()
    pool = yield from aiopg.create_pool(dsn, minsize=minsize, maxsize=maxsize,
                                        loop=loop, timeout=timeout, **kwargs)
    conn = yield from pool.acquire()
    try:
        real_dsn = conn.dsn
        return Engine(dialect, pool, real_dsn)
    finally:
        yield from pool.release(conn)


class Engine:
    """Connects a aiopg.Pool and
    sqlalchemy.engine.interfaces.Dialect together to provide a
    source of database connectivity and behavior.

    An Engine object is instantiated publicly using the
    create_engine coroutine.
    """

    def __init__(self, dialect, pool, dsn):
        self._dialect = dialect
        self._pool = pool
        self._dsn = dsn

    @property
    def dialect(self):
        """An dialect for engine."""
        return self._dialect

    @property
    def name(self):
        """A name of the dialect."""
        return self._dialect.name

    @property
    def driver(self):
        """A driver of the dialect."""
        return self._dialect.driver

    @property
    def dsn(self):
        """DSN connection info"""
        return self._dsn

    @property
    def timeout(self):
        return self._pool.timeout

    @property
    def minsize(self):
        return self._pool.minsize

    @property
    def maxsize(self):
        return self._pool.maxsize

    @property
    def size(self):
        return self._pool.size

    @property
    def freesize(self):
        return self._pool.freesize

    @property
    def closed(self):
        return self._pool.closed

    def close(self):
        """Close engine.

        Mark all engine connections to be closed on getting back to pool.
        Closed engine doesn't allow to acquire new connections.
        """
        self._pool.close()

    def terminate(self):
        """Terminate engine.

        Terminate engine pool with instantly closing all acquired
        connections also.
        """
        self._pool.terminate()

    @asyncio.coroutine
    def wait_closed(self):
        """Wait for closing all engine's connections."""
        yield from self._pool.wait_closed()

    def acquire(self):
        """Get a connection from pool."""
        coro = self._acquire()
        return _EngineAcquireContextManager(coro, self)

    @asyncio.coroutine
    def _acquire(self):
        raw = yield from self._pool.acquire()
        conn = SAConnection(raw, self)
        return conn

    def release(self, conn):
        """Revert back connection to pool."""
        if conn.in_transaction:
            raise InvalidRequestError("Cannot release a connection with "
                                      "not finished transaction")
        raw = conn.connection
        fut = self._pool.release(raw)
        return fut

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __iter__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (yield from engine) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from engine.acquire()
        #     try:
        #         <block>
        #     finally:
        #         engine.release(conn)
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

    if PY_35:  # pragma: no branch
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
            yield from self.wait_closed()


_EngineContextManager = _PoolContextManager
_EngineAcquireContextManager = _PoolAcquireContextManager


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from engine) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    __slots__ = ('_engine', '_conn')

    def __init__(self, engine, conn):
        self._engine = engine
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        try:
            self._engine.release(self._conn)
        finally:
            self._engine = None
            self._conn = None
