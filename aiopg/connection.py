import asyncio

import psycopg2
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR)
from .exceptions import UnknownPollError, ConnectionClosedError

from .cursor import Cursor


__all__ = ('connect',)


@asyncio.coroutine
def connect(dsn=None, *, loop=None, **kwargs):
    """XXX"""
    if loop is None:
        loop = asyncio.get_event_loop()

    waiter = asyncio.Future(loop=loop)
    conn = Connection(dsn, loop, waiter, **kwargs)
    yield from waiter
    return conn


class Connection:
    """XXX"""

    def __init__(self, dsn, loop, waiter, **kwargs):
        self._loop = loop
        self._conn = psycopg2.connect(dsn, async=True, **kwargs)
        assert self._conn.isexecuting(), "Is conn async at all???"
        self._fileno = self._conn.fileno()
        self._waiter = waiter
        self._reading = False
        self._writing = False
        self._ready()

    def _ready(self):
        if self._waiter is None:
            self._fatal_error(None, "Bad state in aiopg _ready callback")
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
            self._waiter = None
        else:
            if state == POLL_OK:
                if self._reading:
                    self._loop.remove_reader(self._fileno)
                    self._reading = False
                if self._writing:
                    self._loop.remove_writer(self._fileno)
                    self._writing = False
                self._waiter.set_result(None)
                self._waiter = None
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
                self._fatal_error(UnknownPollError())

    def _fatal_error(self, exc, message='Fatal error on aiopg connetion'):
        # Should be called from exception handler only.
        self._loop.call_exception_handler({
            'message': message,
            'exception': exc,
            'connection': self,
            })
        self.close()

    @asyncio.coroutine
    def _poll(self):
        assert self._waiter is not None
        if not self._conn.isexecuting():
            # Underlying connection is not executing, the call is not async
            self._waiter = None
            return

        self._ready()
        try:
            yield from self._waiter
        finally:
            self._waiter = None

    def _create_waiter(self, func_name):
        if not self._conn:
            raise ConnectionClosedError()
        if self._waiter is not None:
            raise RuntimeError('%s() called while another coroutine is '
                               'already waiting for incoming data' % func_name)
        self._waiter = asyncio.Future(loop=self._loop)

    def _isexecuting(self):
        return self._conn.isexecuting()

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False):
        """XXX"""
        self._create_waiter('cursor')
        if cursor_factory is None:
            impl = self._conn.cursor(name=name,
                                     scrollable=scrollable, withhold=withhold)
        else:
            impl = self._conn.cursor(name=name, cursor_factory=cursor_factory,
                                     scrollable=scrollable, withhold=withhold)
        yield from self._poll()
        return Cursor(self, impl)

    # FIXME: add transaction and TPC methods

    def close(self):
        """Remove the connection from the event_loop and close it."""
        if self._conn is None:
            return
        self._loop.remove_reader(self._fileno)
        self._loop.remove_writer(self._fileno)
        self._conn.close()
        self._conn = None
