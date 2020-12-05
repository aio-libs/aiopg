import asyncio
import collections
import warnings

from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from .connection import TIMEOUT, connect
from .utils import (
    _PoolAcquireContextManager,
    _PoolConnectionContextManager,
    _PoolContextManager,
    _PoolCursorContextManager,
    create_future,
    ensure_future,
    get_running_loop,
)


def create_pool(dsn=None, *, minsize=1, maxsize=10,
                timeout=TIMEOUT, pool_recycle=-1,
                enable_json=True, enable_hstore=True, enable_uuid=True,
                echo=False, on_connect=None,
                **kwargs):
    coro = Pool.from_pool_fill(
        dsn, minsize, maxsize, timeout,
        enable_json=enable_json, enable_hstore=enable_hstore,
        enable_uuid=enable_uuid, echo=echo, on_connect=on_connect,
        pool_recycle=pool_recycle, **kwargs
    )

    return _PoolContextManager(coro)


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, dsn, minsize, maxsize, timeout, *,
                 enable_json, enable_hstore, enable_uuid, echo,
                 on_connect, pool_recycle, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize and maxsize != 0:
            raise ValueError("maxsize should be not less than minsize")
        self._dsn = dsn
        self._minsize = minsize
        self._loop = get_running_loop(kwargs.pop('loop', None) is not None)
        self._timeout = timeout
        self._recycle = pool_recycle
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore
        self._enable_uuid = enable_uuid
        self._echo = echo
        self._on_connect = on_connect
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize or None)
        self._cond = asyncio.Condition()
        self._used = set()
        self._terminated = set()
        self._closing = False
        self._closed = False

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    @property
    def timeout(self):
        return self._timeout

    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.close()
            self._cond.notify()

    @property
    def closed(self):
        return self._closed

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)

        self._used.clear()

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    def acquire(self):
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    @classmethod
    async def from_pool_fill(cls, *args, **kwargs):
        """constructor for filling the free pool with connections,
        the number is controlled by the minsize parameter
        """
        self = cls(*args, **kwargs)
        if self._minsize > 0:
            async with self._cond:
                await self._fill_free_pool(False)

        return self

    async def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
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

    async def _fill_free_pool(self, override_min):
        # iterate over free connections and remove timeouted ones
        n, free = 0, len(self._free)
        while n < free:
            conn = self._free[-1]
            if conn.closed:
                self._free.pop()
            elif -1 < self._recycle < self._loop.time() - conn.last_usage:
                conn.close()
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
                    **self._conn_kwargs)
                if self._on_connect is not None:
                    await self._on_connect(conn)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await connect(
                    self._dsn, timeout=self._timeout,
                    enable_json=self._enable_json,
                    enable_hstore=self._enable_hstore,
                    enable_uuid=self._enable_uuid,
                    echo=self._echo,
                    **self._conn_kwargs)
                if self._on_connect is not None:
                    await self._on_connect(conn)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def release(self, conn):
        """Release free connection back to the connection pool.
        """
        fut = create_future(self._loop)
        fut.set_result(None)
        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return fut
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            tran_status = conn._conn.get_transaction_status()
            if tran_status != TRANSACTION_STATUS_IDLE:
                warnings.warn(
                    ("Invalid transaction status on "
                     "released connection: {}").format(tran_status),
                    ResourceWarning
                )
                conn.close()
                return fut
            if self._closing:
                conn.close()
            else:
                conn.free_cursor()
                self._free.append(conn)
            fut = ensure_future(self._wakeup(), loop=self._loop)
        return fut

    async def cursor(self, name=None, cursor_factory=None,
                     scrollable=None, withhold=False, *, timeout=None):
        conn = await self.acquire()
        cur = await conn.cursor(name=name, cursor_factory=cursor_factory,
                                scrollable=scrollable, withhold=withhold,
                                timeout=timeout)
        return _PoolCursorContextManager(self, conn, cur)

    def __await__(self):
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

    def __enter__(self):
        raise RuntimeError(
            '"await" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()

    def __del__(self):
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
                "Unclosed {} connections in {!r}".format(left, self),
                ResourceWarning)
