import asyncio

import psycopg2
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR)
from .exceptions import UnknownPollError, ConnectionClosedError


__all__ = ('connect',)


ALLOWED_ARGS = {'host', 'hostaddr', 'port', 'dbname', 'user',
                'password', 'connect_timeout', 'client_encoding',
                'options', 'application_name',
                'fallback_application_name', 'keepalives',
                'keepalives_idle', 'keepalives_interval',
                'keepalives_count', 'tty', 'sslmode', 'requiressl',
                'sslcompression', 'sslcert', 'sslkey', 'sslrootcert',
                'sslcrl', 'requirepeer', 'krbsrvname', 'gsslib',
                'service', 'database', 'connection_factory', 'cursor_factory'}


@asyncio.coroutine
def connect(dsn=None, *,
            loop=None, **kwargs):
    """XXX"""

    if loop is None:
        loop = asyncio.get_event_loop()

    for k in kwargs:
        if k not in ALLOWED_ARGS:
            raise TypeError("connect() got unexpected keyword argument '{}'"
                            .format(k))

    waiter = asyncio.Future(loop=loop)
    conn = Connection(dsn, loop, waiter, **kwargs)
    yield from waiter
    return conn


class Connection:
    """Psycopg connection wrapper class.
    :param string dsn:
    :param psycopg2.extensions.cursor cursor_factory: argument can be used
        to create non-standard cursors
    :param psycopg2.extensions.connection connection_factory: class is usually
        sub-classed only to provide an easy way to create customized cursors
        but other uses are possible
    :param asyncio.EventLoop loop: A list or tuple with query parameters.
            Defaults to an empty tuple."""
    def __init__(self, dsn, loop, waiter,
                 **kwargs):

        self._loop = loop

        self._conn = psycopg2.connect(
            dsn,
            cursor_factory=self._cursor_factory,
            connection_factory=connection_factory,
            async=True,
            **kwargs)
        self._fileno = self._conn.fileno()
        self._done_waiters = []
        loop.add_writer(self._fileno, self._ready, 'writing', None)
        self._waiter = waiter

    def _ready(self, action):
        if action == None:
            pass
        elif action == 'writing':
            self._loop.remove_writer(self._fileno)
        elif action == 'reading':
            self._loop.remove_reader(self._fileno)
        else:
            raise RuntimeError("Unknown action {!r}".format(action))
        try:
            state = self._conn.poll()
        except (psycopg2.Warning, psycopg2.Error) as error:
            self._waiter.set_exception(error)
            self._waiter = None
        else:
            if state == POLL_OK:
                self._waiter.set_result(None)
                self._waiter = None
            elif state == POLL_READ:
                self._loop.add_reader(self._fileno, self._ready,
                                      'reading', result)
            elif state == POLL_WRITE:
                self._loop.add_writer(self._fileno, self._ready,
                                      'writing', result)
            elif state == POLL_ERROR:
                raise psycopg2.OperationalError("poll() returned {}"
                                                .format(state))
            else:
                raise UnknownPollError()

    def _create_waiter(self, func_name):
        if self._waiter is not None:
            raise RuntimeError('%s() called while another coroutine is '
                               'already waiting for incoming data' % func_name)
        return futures.Future(loop=self._loop)

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False):
        """XXX"""
        if not self._conn:
            raise ConnectionClosedError()
        impl = self._conn.cursor(name=name, cursor_factory=cursor_factory,
                                 scrollable=scrollable, withhold=withhold)
        self._waiter = self._create_waiter('cursor')
        self.loop.add_writer(self.fileno, self._ready, 'writing')
        try:
            yield from self._waiter
        finally:
            self._waiter = None
        return Cursor(self, impl)

    @asyncio.coroutine
    def execute(self, operation, parameters=()):
        """Prepare and execute a database operation (query or command).

        sql_query -- an SQL query to execute

        parameters -- a list or tuple with query parameters, empty
        tuple by default.

        Returns cursor object.

        Passing parameters to SQL queries:
         http://initd.org/psycopg/docs/usage.html#query-parameters
        """
        if not self._conn:
            raise ConnectionClosedError()
        while self._waiter is not None:
            yield from self._waiter
        cursor = self._conn.cursor(cursor_factory=self._cursor_factory)
        cursor.execute(operation, parameters)
        self._waiter = fut = asyncio.Future(loop=self.loop)
        self.loop.add_writer(self.fileno, self._ready, 'writing', cursor)
        return (yield from fut)

    @asyncio.coroutine
    def callproc(self, procname, parameters=()):
        """Call stored procedure"""
        if not self._conn:
            raise ConnectionClosedError()
        while self._waiter is not None:
            yield from self._waiter
        cursor = self._conn.cursor(cursor_factory=self._cursor_factory)
        cursor.callproc(procname, parameters)
        self._waiter = fut = asyncio.Future(loop=self.loop)
        self.loop.add_writer(self.fileno, self._ready, 'reading', cursor)
        return (yield from fut)

    @asyncio.coroutine
    def morgify(self, sql_query, parameters=()):
        """Return a query string after arguments binding.

        Return a query string after arguments binding. The string returned is
        exactly the one that would be sent to the database running
        the execute() method or similar.

        :param string sql_query: An SQL query.
        :param tuple/list parameters: A list or tuple with query parameters.

        :return: resulting query as a byte string"""
        if not self._conn:
            raise ConnectionClosedError()
        cursor = self._conn.cursor(cursor_factory=self._cursor_factory)
        result = cursor.mogrify(sql_query, parameters)
        return result

    # FIXME: add transaction and TPC methods

    def register_hstore(self):
        # TODO: implement. Do we need this at all?
        raise NotImplementedError

    def close(self):
        """Remove the connection from the event_loop and close it."""
        if self._conn is None:
            return
        self._loop.remove_reader(self._fileno)
        self._loop.remove_writer(self._fileno)
        self._conn.close()
        self._conn = None
