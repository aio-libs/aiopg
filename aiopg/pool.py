import asyncio

from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from .connection import connect
from .log import logger


@asyncio.coroutine
def create_pool(dsn=None, *, minsize=10, maxsize=10,
                loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(dsn, minsize, maxsize, loop, **kwargs)
    yield from pool._fill_free_pool()
    return pool


class Pool:
    """Connection pool"""

    def __init__(self, dsn, minsize, maxsize, loop, **kwargs):
        self._dsn = dsn
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._free = asyncio.queues.Queue(maxsize, loop=self._loop)
        self._used = set()

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxsize

    @property
    def size(self):
        return self.freesize + len(self._used)

    @property
    def freesize(self):
        return self._free.qsize()

    @asyncio.coroutine
    def clear(self):
        """Close all free connections in pool."""
        while not self._free.empty():
            conn = yield from self._free.get()
            yield from conn.close()

    @asyncio.coroutine
    def acquire(self):
        """Acquire free connection from the pool."""
        yield from self._fill_free_pool()
        if self.minsize > 0 or not self._free.empty():
            conn = yield from self._free.get()
        else:
            conn = yield from connect(
                self._dsn, loop=self._loop,
                **self._conn_kwargs)
        assert not conn.closed, conn
        assert conn not in self._used, (conn, self._used)
        self._used.add(conn)
        return conn

    @asyncio.coroutine
    def _fill_free_pool(self):
        while self.freesize < self.minsize and self.size < self.maxsize:
            conn = yield from connect(
                self._dsn, loop=self._loop,
                **self._conn_kwargs)
            yield from self._free.put(conn)

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is NOT a coroutine.
        """
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            tran_status = conn._conn.get_transaction_status()
            if tran_status != TRANSACTION_STATUS_IDLE:
                logger.warning(
                    "Invalid transaction status on released connection: %d",
                    tran_status)
                conn._close()
                return
            while True:
                try:
                    self._free.put_nowait(conn)
                except asyncio.QueueFull:
                    # close the oldest connection in free pool.
                    # that can happen if we acuire multiple
                    # connections from parallel tasks
                    # when minsize == 0
                    conn2 = self._free.get_nowait()
                    conn2._close()
                else:
                    break

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False):
        """XXX"""
        conn = yield from self.acquire()
        cur = yield from conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold)
        return _CursorContextManager(self, conn, cur)

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
        #     with (yield from pool) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from pool.acquire()
        #     try:
        #         <block>
        #     finally:
        #         conn.release()
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from pool) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _CursorContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    cursor around a block:

        with (yield from pool.cursor()) as cur:
            yield from cur.execute("SELECT 1")

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
            self._cur._impl.close()
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None
            self._cur = None
