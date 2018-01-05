import asyncio
import collections
import sys
import warnings


from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from .connection import connect, TIMEOUT
from .log import logger
from .utils import (PY_35, _PoolContextManager, _PoolConnectionContextManager,
                    _PoolCursorContextManager, _PoolAcquireContextManager,
                    ensure_future, create_future)


PY_341 = sys.version_info >= (3, 4, 1)


def create_pool(dsn=None, *, minsize=1, maxsize=10,
                loop=None, timeout=TIMEOUT, pool_recycle=-1,
                enable_json=True, enable_hstore=True, enable_uuid=True,
                echo=False, on_connect=None,
                **kwargs):
    coro = _create_pool(dsn=dsn, minsize=minsize, maxsize=maxsize, loop=loop,
                        timeout=timeout, pool_recycle=pool_recycle,
                        enable_json=enable_json, enable_hstore=enable_hstore,
                        enable_uuid=enable_uuid, echo=echo,
                        on_connect=on_connect, **kwargs)
    return _PoolContextManager(coro)


@asyncio.coroutine
def _create_pool(dsn=None, *, minsize=1, maxsize=10,
                 loop=None, timeout=TIMEOUT, pool_recycle=-1,
                 enable_json=True, enable_hstore=True, enable_uuid=True,
                 echo=False, on_connect=None,
                 **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(dsn, minsize, maxsize, loop, timeout,
                enable_json=enable_json, enable_hstore=enable_hstore,
                enable_uuid=enable_uuid, echo=echo, on_connect=on_connect,
                pool_recycle=pool_recycle, **kwargs)
    if minsize > 0:
        with (yield from pool._cond):
            yield from pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, dsn, minsize, maxsize, loop, timeout, *,
                 enable_json, enable_hstore, enable_uuid, echo,
                 on_connect, pool_recycle, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize and maxsize != 0:
            raise ValueError("maxsize should be not less than minsize")
        self._dsn = dsn
        self._minsize = minsize
        self._loop = loop
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
        self._cond = asyncio.Condition(loop=loop)
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

    @asyncio.coroutine
    def clear(self):
        """Close all free connections in pool."""
        with (yield from self._cond):
            while self._free:
                conn = self._free.popleft()
                yield from conn.close()
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

    @asyncio.coroutine
    def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            conn.close()

        with (yield from self._cond):
            while self.size > self.freesize:
                yield from self._cond.wait()

        self._closed = True

    def acquire(self):
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    @asyncio.coroutine
    def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        with (yield from self._cond):
            while True:
                yield from self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    if self._on_connect is not None:
                        yield from self._on_connect(conn)
                    return conn
                else:
                    yield from self._cond.wait()

    @asyncio.coroutine
    def _fill_free_pool(self, override_min):
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
                conn = yield from connect(
                    self._dsn, loop=self._loop, timeout=self._timeout,
                    enable_json=self._enable_json,
                    enable_hstore=self._enable_hstore,
                    enable_uuid=self._enable_uuid,
                    echo=self._echo,
                    **self._conn_kwargs)
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
                conn = yield from connect(
                    self._dsn, loop=self._loop, timeout=self._timeout,
                    enable_json=self._enable_json,
                    enable_hstore=self._enable_hstore,
                    enable_uuid=self._enable_uuid,
                    echo=self._echo,
                    **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    @asyncio.coroutine
    def _wakeup(self):
        with (yield from self._cond):
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
                logger.warning(
                    "Invalid transaction status on released connection: %d",
                    tran_status)
                conn.close()
                return fut
            if self._closing:
                conn.close()
            else:
                self._free.append(conn)
            fut = ensure_future(self._wakeup(), loop=self._loop)
        return fut

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False, *, timeout=None):
        """XXX"""
        conn = yield from self.acquire()
        cur = yield from conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold,
                                     timeout=timeout)
        return _PoolCursorContextManager(self, conn, cur)

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
        return _PoolConnectionContextManager(self, conn)

    if PY_35:  # pragma: no branch
        @asyncio.coroutine
        def __aenter__(self):
            return self

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            self.close()
            yield from self.wait_closed()

    if PY_341:  # pragma: no branch
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
