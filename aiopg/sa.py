"""Optional support for sqlalchemy.sql dynamic query generation."""

import asyncio
from collections import Sequence, Mapping

try:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
    from sqlalchemy.sql import ClauseElement
except ImportError:  # pragma: no cover
    raise ImportError('aiopg.sa requires sqlalchemy')


from .connection import connect as base_connect, Connection
from .cursor import Cursor
from .pool import create_pool as base_create_pool


dialect = PGDialect_psycopg2()


@asyncio.coroutine
def connect(dsn=None, *, loop=None, **kwargs):
    return (yield from base_connect(dsn, loop=loop,
                                    _connection_factory=SAConnection,
                                    **kwargs))


@asyncio.coroutine
def create_pool(dsn=None, *, minsize=10, maxsize=10,
                loop=None, **kwargs):
    return (yield from base_create_pool(dsn,
                                        minsize=minsize,
                                        maxsize=maxsize,
                                        loop=loop,
                                        _connection_factory=SAConnection,
                                        **kwargs))


class SACursor(Cursor):

    def __init__(self, conn, impl, dialect):
        super().__init__(conn, impl)
        self._dialect = dialect

    @property
    def dialect(self):
        """sqlalchemy dialect, PGDialect_psycopg2() by default."""
        return self._dialect

    @asyncio.coroutine
    def execute(self, operation, parameters=()):
        if isinstance(operation, ClauseElement):
            assert parameters == (), ("Don't mix sqlalchemy clause "
                                      "and execution with parameters")
            compiled = operation.compile(dialect=self._dialect)
            parameters = compiled.params
            return (yield from super().execute(str(compiled), parameters))
        else:
            return (yield from super().execute(operation, parameters))

    @asyncio.coroutine
    def scalar(self, operation):
        yield from self.execute(operation)
        if self.rowcount != 1:
            raise ValueError("operation return not exactly 1 row")
        ret = yield from self.fetchone()
        if isinstance(ret, Sequence):
            assert len(ret) == 1, "Bad SQL {!r}".format(operation)
            return ret[0]
        elif isinstance(ret, Mapping):
            assert len(ret) == 1, "Bad SQL {!r}".format(operation)
            return ret[next(iter(ret))]  # return the value for single key
        else:
            raise ValueError("the result of SQL execution is "
                             "something terrible ({!r})")


class SAConnection(Connection):

    dialect = dialect

    def __init__(self, dsn, loop, waiter, **kwargs):
        super().__init__(dsn, loop, waiter, **kwargs)

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False, dialect=dialect):
        impl = yield from self._cursor(name=name,
                                       cursor_factory=cursor_factory,
                                       scrollable=scrollable,
                                       withhold=withhold)
        return SACursor(self, impl, dialect)
