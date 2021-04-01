import asyncio
import collections
import warnings
from types import TracebackType
from typing import Any, Awaitable, Callable, Deque, Optional, Set, Type

import async_timeout
import psycopg2.extensions

from .connection import TIMEOUT, Connection, connect
from .utils import (
    _PoolAcquireContextManager,
    _PoolConnectionContextManager,
    _PoolContextManager,
    _PoolCursorContextManager,
    create_completed_future,
    get_running_loop,
)


def create_pool(
    dsn: Optional[str] = None,
    *,
    minsize: int = 1,
    maxsize: int = 10,
    timeout: float = TIMEOUT,
    pool_recycle: float = -1.0,
    enable_json: bool = True,
    enable_hstore: bool = True,
    enable_uuid: bool = True,
    echo: bool = False,
    on_connect: Optional[Callable[[Connection], Awaitable[None]]] = None,
    **kwargs: Any
) -> _PoolContextManager:
    coro = Pool.from_pool_fill(
        dsn, minsize, maxsize, timeout,
        enable_json=enable_json, enable_hstore=enable_hstore,
        enable_uuid=enable_uuid, echo=echo, on_connect=on_connect,
        pool_recycle=pool_recycle, **kwargs
    )
    return _PoolContextManager(coro)


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(
            self,
            dsn: str,
            minsize: int,
            maxsize: int,
            timeout: float,
            *,
            enable_json: bool,
            enable_hstore: bool,
            enable_uuid: bool,
            echo: bool,
            on_connect: Optional[Callable[[Connection], Awaitable[None]]],
            pool_recycle: float,
            **kwargs: Any
    ):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize and maxsize != 0:
            raise ValueError("maxsize should be not less than minsize")
        self._dsn = dsn
        self._minsize = minsize
        self._loop = get_running_loop()
        self._timeout = timeout
        self._recycle = pool_recycle
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore
        self._enable_uuid = enable_uuid
        self._echo = echo
        self._on_connect = on_connect
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free: Deque[Connection] = (
            collections.deque(maxlen=maxsize or None)
        )
        self._cond = asyncio.Condition()
        self._used: Set[Connection] = set()
        self._terminated: Set[Connection] = set()
        self._closing = False
        self._closed = False

    @property
    def echo(self) -> bool:
        return self._echo

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def maxsize(self) -> Optional[int]:
        return self._free.maxlen

    @property
    def size(self) -> int:
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self) -> int:
        return len(self._free)

    @property
    def timeout(self) -> float:
        return self._timeout

    async def clear(self) -> None:
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.close()
            self._cond.notify()

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self) -> None:
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)

        self._used.clear()

    async def wait_closed(self) -> None:
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            await conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    def acquire(self) -> _PoolAcquireContextManager:
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    @classmethod
    async def from_pool_fill(cls, *args: Any, **kwargs: Any) -> 'Pool':
        """constructor for filling the free pool with connections,
        the number is controlled by the minsize parameter
        """
        self = cls(*args, **kwargs)
        if self._minsize > 0:
            async with self._cond:
                await self._fill_free_pool(False)

        return self

    async def _acquire(self) -> Connection:
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with async_timeout.timeout(self._timeout), self._cond:
            while True:
                await self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def _fill_free_pool(self, override_min: bool) -> None:
        # iterate over free connections and remove timeouted ones
        n, free = 0, len(self._free)
        while n < free:
            conn = self._free[-1]
            if conn.closed:
                self._free.pop()
            elif -1 < self._recycle < self._loop.time() - conn.last_usage:
                await conn.close()
                self._free.pop()
            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(
                    self._dsn, timeout=self._timeout,
                    enable_json=self._enable_json,
                    enable_hstore=self._enable_hstore,
                    enable_uuid=self._enable_uuid,
                    echo=self._echo,
                    **self._conn_kwargs
                )
                if self._on_connect is not None:
                    await self._on_connect(conn)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < (self.maxsize or 0):
            self._acquiring += 1
            try:
                conn = await connect(
                    self._dsn,
                    timeout=self._timeout,
                    enable_json=self._enable_json,
                    enable_hstore=self._enable_hstore,
                    enable_uuid=self._enable_uuid,
                    echo=self._echo,
                    **self._conn_kwargs
                )
                if self._on_connect is not None:
                    await self._on_connect(conn)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self) -> None:
        async with self._cond:
            self._cond.notify()

    def release(self, conn: Connection) -> asyncio.Future[None]:
        """Release free connection back to the connection pool.
        """
        future = create_completed_future(self._loop)
        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return future
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if conn.closed:
            return future
        transaction_status = conn.raw.get_transaction_status()
        if transaction_status != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
            warnings.warn(
                f"Invalid transaction status on "
                f"released connection: {transaction_status}",
                ResourceWarning
            )
            conn.close()
            return future
        if self._closing:
            conn.close()
        else:
            self._free.append(conn)
        return asyncio.ensure_future(self._wakeup(), loop=self._loop)

    async def cursor(
        self,
        name: Optional[str] = None,
        cursor_factory=Any,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        *,
        timeout: Optional[float] = None
    ) -> _PoolCursorContextManager:
        conn = await self.acquire()
        cur = await conn.cursor(
            name=name,
            cursor_factory=cursor_factory,
            scrollable=scrollable,
            withhold=withhold,
            timeout=timeout
        )
        return _PoolCursorContextManager(self, conn, cur)

    def __await__(self) -> _PoolConnectionContextManager:
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (await pool) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = await pool.acquire()
        #     try:
        #         <block>
        #     finally:
        #         conn.release()
        conn = yield from self._acquire().__await__()
        return _PoolConnectionContextManager(self, conn)

    def __enter__(self) -> 'Pool':
        raise RuntimeError(
            '"await" should be used as context manager expression'
        )

    def __exit__(self, *args: Any) -> None:
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    async def __aenter__(self) -> 'Pool':
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()
        await self.wait_closed()

    def __del__(self) -> None:
        try:
            self._free
        except AttributeError:
            return  # frame has been cleared, __dict__ is empty
        if self._free:
            left = 0
            while self._free:
                conn = self._free.popleft()
                conn.close()
                left += 1
            warnings.warn(
                f"Unclosed {left} connections in {self!r}",
                ResourceWarning
            )
