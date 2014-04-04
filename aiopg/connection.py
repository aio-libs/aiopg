import asyncio

import psycopg2
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR)

from .cursor import Cursor


__all__ = ('connect',)


@asyncio.coroutine
def connect(dsn=None, *, loop=None, **kwargs):
    """XXX"""
    if loop is None:
        loop = asyncio.get_event_loop()

    waiter = asyncio.Future(loop=loop)
    conn = Connection(dsn, loop, waiter, **kwargs)
    yield from conn._poll(waiter)
    return conn


class Connection:
    """XXX"""

    def __init__(self, dsn, loop, waiter, **kwargs):
        self._loop = loop
        self._conn = psycopg2.connect(dsn, async=True, **kwargs)
        self._dsn = self._conn.dsn
        assert self._conn.isexecuting(), "Is conn async at all???"
        self._fileno = self._conn.fileno()
        self._waiter = waiter
        self._reading = False
        self._writing = False
        self._ready()

    def _ready(self):
        if self._waiter is None:
            self._fatal_error("Fatal error on aiopg connetion: "
                              "bad state in _ready callback")
            return

        try:
            state = self._conn.poll()
        except (psycopg2.Warning, psycopg2.Error) as exc:
            if self._reading:
                self._loop.remove_reader(self._fileno)
                self._reading = False
            if self._writing:
                self._loop.remove_writer(self._fileno)
                self._writing = False
            self._waiter.set_exception(exc)
        else:
            if state == POLL_OK:
                if self._reading:
                    self._loop.remove_reader(self._fileno)
                    self._reading = False
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
                self._waiter.set_result(None)
            elif state == POLL_READ:
                if not self._reading:
                    self._loop.add_reader(self._fileno, self._ready)
                    self._reading = True
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
            elif state == POLL_WRITE:
                if self._reading:
                    self._loop.remove_reader(self._fileno)
                    self._reading = False
                if not self._writing:
                    self._loop.add_writer(self._fileno, self._ready)
                    self._writing = True
            elif state == POLL_ERROR:
                self._fatal_error(psycopg2.OperationalError(
                    "aiopg poll() returned {}".format(state)))
            else:
                self._fatal_error("Fatal error on aiopg connetion: "
                                  "unknown anser from underlying .poll()")

    def _fatal_error(self, message):
        # Should be called from exception handler only.
        self._loop.call_exception_handler({
            'message': message,
            'connection': self,
            })
        self._close()

    @asyncio.coroutine
    def _create_waiter(self, func_name):
        while self._waiter is not None:
            yield from self._waiter
        self._waiter = asyncio.Future(loop=self._loop)
        return self._waiter

    @asyncio.coroutine
    def _poll(self, waiter):
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready()
        try:
            yield from self._waiter
        finally:
            self._waiter = None

    def _isexecuting(self):
        return self._conn.isexecuting()

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False):
        """XXX"""
        if cursor_factory is None:
            impl = self._conn.cursor(name=name,
                                     scrollable=scrollable, withhold=withhold)
        else:
            impl = self._conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold)
        return Cursor(self, impl)

    @asyncio.coroutine
    def close(self):
        """Remove the connection from the event_loop and close it."""
        # FIXME: process closing with uncommitted transaction
        # that should wait for _conn.poll() I guess.
        self._close()

    def _close(self):
        if self._reading:
            self._loop.remove_reader(self._fileno)
        if self._writing:
            self._loop.remove_writer(self._fileno)
        self._conn.close()

    @property
    def closed(self):
        """Read-only attribute reporting whether the database connection
        is open (False) or closed (True)."""
        return self._conn.closed

    @asyncio.coroutine
    def commit(self):
        """XXX"""
        waiter = yield from self._conn._create_waiter('commit')
        self._conn.commit()
        yield from self._conn._poll(waiter)

    @asyncio.coroutine
    def rollback(self):
        """XXX"""
        waiter = yield from self._conn._create_waiter('rollback')
        self._conn.rollback()
        yield from self._conn._poll(waiter)

    # TPC

    @asyncio.coroutine
    def xid(self, format_id, gtrid, bqual):
        return self._conn.xid(format_id, gtrid, bqual)

    @asyncio.coroutine
    def tpc_begin(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_begin cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_prepare(self):
        raise psycopg2.ProgrammingError(
            "tpc_prepare cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_commit(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_commit cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_rollback(self, xid=None):
        raise psycopg2.ProgrammingError(
            "tpc_rollback cannot be used in asynchronous mode")

    @asyncio.coroutine
    def tpc_recover(self):
        raise psycopg2.ProgrammingError(
            "tpc_recover cannot be used in asynchronous mode")

    @asyncio.coroutine
    def cancel(self):
        waiter = yield from self._conn._create_waiter('cancel')
        self._conn.cancel()
        yield from self._conn._poll(waiter)

    @asyncio.coroutine
    def reset(self):
        raise psycopg2.ProgrammingError(
            "reset cannot be used in asynchronous mode")

    @property
    def dsn(self):
        return self._dsn

    @asyncio.coroutine
    def set_session(self, *, isolation_level=None, readonly=None,
                    deferrable=None, autocommit=None):
        return self._conn.set_session(isolation_level=isolation_level,
                                      readonly=readonly,
                                      deferrable=deferrable,
                                      autocommit=autocommit)

    @property
    def autocommit(self):
        """XXX"""
        self._check_closed()
        return self._impl.autocommit

    @autocommit.setter
    def autocommit(self, val):
        """XXX"""
        self._check_closed()
        self._impl.autocommit = val

    @property
    def isolation_level(self):
        """XXX"""
        self._check_closed()
        return self._impl.isolation_level

    @asyncio.coroutine
    def set_isolation_level(self, val):
        self._check_closed()
        self._conn.set_isolation_level(val)

    @property
    def encoding(self):
        """XXX"""
        self._check_closed()
        return self._impl.encoding

    @asyncio.coroutine
    def set_client_encoding(self, val):
        self._check_closed()
        self._conn.set_client_encoding(val)
