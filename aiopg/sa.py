"""Optional support for sqlalchemy.sql dynamic query generation."""

import asyncio
from collections import Sequence, Mapping
from sqlalchemy.dialects.postgresql.psycopg2 import PGCompiler_psycopg2
from sqlalchemy.sql import ClauseElement


from .connection import connect as base_connect, Connection
from .cursor import Cursor
from .pool import create_pool as base_create_pool


dialect = PGCompiler_psycopg2()


@asyncio.coroutine
def connect(dsn=None, *, loop=None, **kwargs):
    return (yield from base_connect(dsn, loop=loop,
                                    _connection_class=SAConnection, **kwargs))


@asyncio.coroutine
def create_pool(dsn=None, *, minsize=10, maxsize=10,
                loop=None, **kwargs):
    return (yield from base_create_pool(dsn,
                                        minsize=minsize,
                                        maxsize=maxsize,
                                        loop=loop,
                                        _connection_class=SAConnection,
                                        **kwargs))


class SACursor(Cursor):

    @asyncio.coroutine
    def execute(self, operation, parameters=()):
        if isinstance(operation, ClauseElement):
            assert parameters == (), ("Don't mix sqlalchemy clause "
                                      "and execution with parameters")
            compiled = operation.compile(dialect=dialect)
            parameters = compiled.params
            return (yield from super().execute(str(compiled), parameters))
        else:
            return (yield from super().execute(operation, parameters))

    @asyncio.coroutine
    def scalar(self, operation):
        ret = yield from self.execute(operation)
        if isinstance(ret, Sequence):
            assert len(ret) == 1, "Bad SQL {!r}".format(operation)
            return ret[0]
        elif isinstance(ret, Mapping):
            assert len(ret) == 1, "Bad SQL {!r}".format(operation)
            return ret[next(iter(ret))]  # return the value for single key
        else:
            raise ValueError("the result of SQL execution is "
                             "something terrible")
        return ret


class SAConnection(Connection):

    _aiopg_cursor_factory = SACursor
