import asyncio
import sys
import psycopg2

PY_35 = sys.version_info >= (3, 5)
PY_352 = sys.version_info >= (3, 5, 2)

if PY_35:
    from collections.abc import Coroutine

    base = Coroutine
else:
    base = object

try:
    ensure_future = asyncio.ensure_future
except AttributeError:
    ensure_future = getattr(asyncio, 'async')


def create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


class _ContextManager(base):
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

    @asyncio.coroutine
    def __iter__(self):
        resp = yield from self._coro
        return resp

    if PY_35:
        def __await__(self):
            resp = yield from self._coro
            return resp

        @asyncio.coroutine
        def __aenter__(self):
            self._obj = yield from self._coro
            return self._obj

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            self._obj.close()
            self._obj = None


class _SAConnectionContextManager(_ContextManager):
    if PY_35:  # pragma: no branch
        if PY_352:
            def __aiter__(self):
                return self

            @asyncio.coroutine
            def __anext__(self):
                if self._obj is None:
                    self._obj = yield from self._coro

                try:
                    return (yield from self._obj.__anext__())
                except StopAsyncIteration:
                    self._obj.close()
                    self._obj = None
                    raise

        else:
            @asyncio.coroutine
            def __aiter__(self):
                result = yield from self._coro
                return result


class _PoolContextManager(_ContextManager):
    if PY_35:
        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            self._obj.close()
            yield from self._obj.wait_closed()
            self._obj = None


class _TransactionPointContextManager(_ContextManager):
    if PY_35:

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                yield from self._obj.rollback_savepoint()
            else:
                yield from self._obj.release_savepoint()

            self._obj = None


class _TransactionBeginContextManager(_ContextManager):
    if PY_35:

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                yield from self._obj.rollback()
            else:
                yield from self._obj.commit()

            self._obj = None


class _TransactionContextManager(_ContextManager):
    if PY_35:

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            if exc_type:
                yield from self._obj.rollback()
            else:
                if self._obj.is_active:
                    yield from self._obj.commit()
            self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ('_coro', '_obj', '_pool')

    def __init__(self, coro, pool):
        super().__init__(coro)
        self._pool = pool

    if PY_35:
        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            yield from self._pool.release(self._obj)
            self._pool = None
            self._obj = None


class _PoolConnectionContextManager:
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
        assert self._conn
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None

    if PY_35:
        @asyncio.coroutine
        def __aenter__(self):
            assert not self._conn
            self._conn = yield from self._pool.acquire()
            return self._conn

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_val, exc_tb):
            try:
                yield from self._pool.release(self._conn)
            finally:
                self._pool = None
                self._conn = None


class _PoolCursorContextManager:
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


if not PY_35:
    try:
        from asyncio import coroutines

        coroutines._COROUTINE_TYPES += (_ContextManager,)
    except BaseException:
        pass
