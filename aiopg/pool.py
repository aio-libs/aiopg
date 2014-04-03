import asyncio
import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from pgtulip.connection import Connection
from pgtulip.exceptions import BusyConnection


class Pool:
    """Simple connection pool for ``pgtulip.connection``s

    Used as drop in replacement for ``pgtulip.connection`` with the
    same methods"""

    def __init__(self, *args, **kwargs):
        self.a = args
        self.kw = kwargs

        self.loop = kwargs.get("loop") or asyncio.get_event_loop()
        self._maxsize = kwargs.get('maxsize', 5)
        self._pool = asyncio.queues.Queue(loop=self.loop)
        self._size = 0

    def setup(self):
        """Init connection in connection pool. You do not have to
        call this function in case of LazyPool"""
        conns = [Connection(*self.a, **self.kw) for i in range(self._maxsize)]
        yield from asyncio.wait([c.connect() for c in conns])
        [self._pool.put_nowait(c) for c in conns]
        self.size = self._maxsize

    def close(self):
        """Close all connections in pool."""
        pool = self._pool
        while pool.qsize() > 0:
            conn = pool.get_nowait()
            conn.close()

    @asyncio.coroutine
    def _acquire(self):
        """Acquire connection from pool (or wait for free one).

        :returns conn: connection object."""
        conn = yield from self._pool.get()
        return conn

    def _release(self, conn):
        """Release free connection back to the connection pool.

        :param conn: used pgconnection"""
        if conn.is_closed:
            raise psycopg2.extensions.OperationalError("""Connection is
            closed: %r""" % (conn,))
        elif conn.is_busy:
            raise BusyConnection("Busy connections must not be returned to pool")
        self._pool.put_nowait(conn)

    @asyncio.coroutine
    def run_query(self, operation, parameters=()):
        # TODO:
        conn = yield from self._acquire()
        data = yield from conn.run_qury(operation, parameters)
        self._release(conn)
        return data

    @asyncio.coroutine
    def morgify(self, sql_query, parameters=()):
        conn = yield from self._acquire()
        data = yield from conn.morgify(sql_query, parameters)
        self._release(conn)
        return data

    @asyncio.coroutine
    def run_transaction(self, sql_queries):
        conn = yield from self._acquire()
        data = yield from conn.run_transaction(sql_queries)
        self._release(conn)
        return data


class LazyPool(Pool):
    """Same thing as usual ``Pool`` but connections established in lazy way.
    Connection instantiated only when it is needed"""

    @asyncio.coroutine
    def _acquire(self):
        """Lazy version of acquire connection"""
        pool = self._pool

        if self._size >= self._maxsize or pool.qsize():
            conn = yield from pool.get()
        else:
            self._size += 1
            try:
                conn = yield from Connection(*self.a, **self.kw)
            except:
                self._size -= 1
                raise
        return conn
