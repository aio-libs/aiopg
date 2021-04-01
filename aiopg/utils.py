import asyncio
import sys
from collections.abc import Coroutine
from types import TracebackType
from typing import (
    Any,
    Generator,
    Generic,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
)

import psycopg2

from .connection import Connection
from .cursor import Cursor
from .pool import Pool
from .transaction import Transaction

if sys.version_info >= (3, 7, 0):
    __get_running_loop = asyncio.get_running_loop
else:
    def __get_running_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            raise RuntimeError('no running event loop')
        return loop


def get_running_loop() -> asyncio.AbstractEventLoop:
    return __get_running_loop()


def create_completed_future(
    loop: asyncio.AbstractEventLoop
) -> asyncio.Future[Any]:
    future = loop.create_future()
    future.set_result(None)
    return future


class Closable(Protocol):
    def close(self) -> None:
        ...


class AsyncClosable(Protocol):
    async def close(self) -> None:
        ...


TObj = TypeVar("TObj", bound=Union[Closable, AsyncClosable])


class _ContextManager(Coroutine[Any, Any, TObj], Generic[TObj]):
    __slots__ = ('_coro', '_obj')

    def __init__(self, coro: Coroutine[Any, Any, TObj]):
        self._coro = coro
        self._obj: Optional[TObj] = None

    def send(self, value: Any) -> 'Any':
        return self._coro.send(value)

    def throw(
        self,
        typ: Type[BaseException],
        val: Union[BaseException, object] = None,
        tb: Optional[TracebackType] = None
    ) -> Any:
        if val is None:
            return self._coro.throw(typ)
        if tb is None:
            return self._coro.throw(typ, val)
        return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        self._coro.close()

    def __await__(self) -> Generator[Any, Any, TObj]:
        return self._coro.__await__()

    async def __aenter__(self) -> TObj:
        self._obj = await self._coro
        assert self._obj
        return self._obj

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            if asyncio.iscoroutinefunction(self._obj.close):
                await self._obj.close()  # type: ignore
            else:
                self._obj.close()
        finally:
            self._obj = None


class _PoolContextManager(_ContextManager[Pool]):
    __slots__ = ()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            self._obj.close()
            await self._obj.wait_closed()
        finally:
            self._obj = None


class _TransactionPointContextManager(_ContextManager[Transaction]):
    __slots__ = ()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            if exc_type is not None:
                await self._obj.rollback_savepoint()
            else:
                await self._obj.release_savepoint()
        finally:
            self._obj = None


class _TransactionBeginContextManager(_ContextManager[Transaction]):
    __slots__ = ()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            if exc_type is not None:
                await self._obj.rollback()
            else:
                await self._obj.commit()
        finally:
            self._obj = None


class _PoolAcquireContextManager(_ContextManager[Connection]):
    __slots__ = ('_coro', '_obj', '_pool')

    def __init__(self, coro, pool):
        super().__init__(coro)
        self._pool = pool

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._pool is None or self._obj is None:
            return

        try:
            await self._pool.release(self._obj)
        finally:
            self._pool = None
            self._obj = None


class _PoolConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        async with pool as conn:
            cur = await conn.cursor()

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool: Pool, conn: Connection):
        self._pool: Optional[Pool] = pool
        self._conn: Optional[Connection] = conn

    def __enter__(self) -> Connection:
        assert self._conn
        return self._conn

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._pool is None or self._conn is None:
            return
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

    async def __aenter__(self) -> Connection:
        assert self._conn
        return self._conn

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._pool is None or self._conn is None:
            return
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _PoolCursorContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    cursor around a block:

        async with pool.cursor() as cur:
            await cur.execute("SELECT 1")

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn', '_cursor')

    def __init__(self, pool: Pool, conn: Connection, cursor: Cursor):
        self._pool = pool
        self._conn = conn
        self._cursor = cursor

    def __enter__(self) -> Cursor:
        return self._cursor

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        try:
            self._cursor.close()
        except psycopg2.ProgrammingError:
            # seen instances where the cursor fails to close:
            #   https://github.com/aio-libs/aiopg/issues/364
            # We close it here so we don't return a bad connection to the pool
            self._conn.close()
            raise
        finally:
            try:
                self._pool.release(self._conn)
            finally:
                self._pool = None  # type: ignore
                self._conn = None  # type: ignore
                self._cursor = None  # type: ignore
