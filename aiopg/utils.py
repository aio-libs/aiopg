import asyncio
import sys
import warnings
from collections.abc import Coroutine

import psycopg2

from .log import logger

try:
    ensure_future = asyncio.ensure_future
except AttributeError:
    ensure_future = getattr(asyncio, 'async')

if sys.version_info >= (3, 7, 0):
    __get_running_loop = asyncio.get_running_loop
else:
    def __get_running_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            raise RuntimeError('no running event loop')
        return loop


def get_running_loop(is_warn: bool = False) -> asyncio.AbstractEventLoop:
    loop = __get_running_loop()

    if is_warn:
        warnings.warn(
            'aiopg always uses "aiopg.get_running_loop", '
            'look the documentation.',
            DeprecationWarning,
            stacklevel=3
        )

        if loop.get_debug():
            logger.warning(
                'aiopg always uses "aiopg.get_running_loop", '
                'look the documentation.',
                exc_info=True
            )

    return loop


def create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


class _ContextManager(Coroutine):
    __slots__ = ('_coro', '_obj')

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    def __await__(self):
        resp = self._coro.__await__()
        return resp

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        self._obj = None


class _SAConnectionContextManager(_ContextManager):
    __slots__ = ()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._obj is None:
            self._obj = await self._coro

        try:
            return await self._obj.__anext__()
        except StopAsyncIteration:
            self._obj.close()
            self._obj = None
            raise


class _PoolContextManager(_ContextManager):
    __slots__ = ()

    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _TransactionPointContextManager(_ContextManager):
    __slots__ = ()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self._obj.rollback_savepoint()
        else:
            await self._obj.release_savepoint()

        self._obj = None


class _TransactionBeginContextManager(_ContextManager):
    __slots__ = ()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self._obj.rollback()
        else:
            await self._obj.commit()

        self._obj = None


class _TransactionContextManager(_ContextManager):
    __slots__ = ()

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            await self._obj.rollback()
        else:
            if self._obj.is_active:
                await self._obj.commit()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ('_coro', '_obj', '_pool')

    def __init__(self, coro, pool):
        super().__init__(coro)
        self._pool = pool

    async def __aexit__(self, exc_type, exc, tb):
        await self._pool.release(self._obj)
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

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        assert self._conn
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

    async def __aenter__(self):
        assert not self._conn
        self._conn = await self._pool.acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
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

    __slots__ = ('_pool', '_conn', '_cur')

    def __init__(self, pool, conn, cur):
        self._pool = pool
        self._conn = conn
        self._cur = cur

    def __enter__(self):
        return self._cur

    def __exit__(self, *args):
        try:
            self._cur.close()
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
                self._pool = None
                self._conn = None
                self._cur = None
