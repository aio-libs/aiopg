import asyncio
from collections import deque

import psycopg2
from psycopg2.extensions import (
    connection as base_connection,
    cursor as base_cursor,
    POLL_OK, POLL_READ, POLL_WRITE, POLL_ERROR,
    TRANSACTION_STATUS_IDLE)
from pgtulip.exceptions import UnknownPollException


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
    def __init__(self, dsn, connection_factory=None, cursor_factory=None,
                 loop=None, **kwargs):

        self._connection_factory = connection_factory or base_connection
        self._cursor_factory = cursor_factory or base_cursor

        self.loop = loop or asyncio.get_event_loop()

        kwargs.update(
            cursor_factory=self._cursor_factory,
            connection_factory=self._connection_factory,
            async=True
        )
        self.connection = psycopg2.connect(dsn, **kwargs)
        self.fileno = self.connection.fileno()
        self._transaction_status = self.connection.get_transaction_status

    def poller(self, future, event=None):
        try:
            state = self.connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as error:
            self._remove_both(self.fileno)
            future.set_exception(error)
            return future
        else:
            if state == POLL_OK:
                future.set_result(event)
                self._remove_both(self.fileno)
            elif state == POLL_READ:
                self.loop.add_reader(self.fileno, self.poller, future, event)
            elif state == POLL_WRITE:
                self.loop.add_writer(self.fileno, self.poller, future, event)
            elif state == POLL_ERROR:
                raise psycopg2.OperationalError("poll() returned %s" % state)
            else:
                raise UnknownPollException


    def _remove_both(self, fd):
        self.loop.remove_reader(fd)
        self.loop.remove_writer(fd)

    def connect(self):
        """Initiate async connection to the database."""
        future = asyncio.Future(loop=self.loop)
        self.loop.add_writer(self.fileno, self.poller, future, self)
        yield from future

    def execute(self, operation, parameters=()):
        """Prepare and execute a database operation (query or command).

        :param string sql_query: an SQL query to execute
        :param tuple/list parameters: A list or tuple with query parameters.
            Defaults to an empty tuple.
        :returns tuple:

        .. _Passing parameters to SQL queries: http://initd.org/psycopg/docs/usage.html#query-parameters
        """
        cursor = self.connection.cursor(cursor_factory=self._cursor_factory)
        cursor.execute(operation, parameters)
        future = asyncio.Future(loop=self.loop)
        self.loop.add_writer(self.fileno, self.poller, future, cursor)
        return future

    def callproc(self, procname, parameters=()):
        """Call stored procedure"""
        cursor = self.connection.cursor()
        cursor.callproc(procname, parameters)
        future = asyncio.Future(loop=self.loop)
        self.loop.add_writer(self.fileno, self.poller, future, cursor)
        return future

    @asyncio.coroutine
    def morgify(self, sql_query, parameters=()):
        """Return a query string after arguments binding.

        Return a query string after arguments binding. The string returned is
        exactly the one that would be sent to the database running
        the execute() method or similar.

        :param string sql_query: An SQL query.
        :param tuple/list parameters: A list or tuple with query parameters.

        :return: resulting query as a byte string"""
        cursor = self.connection.cursor()
        result = cursor.mogrify(sql_query, parameters)
        return result

    @property
    def is_busy(self):
        """Check if the connection is busy or not."""
        return self.connection.isexecuting() or (self.connection.closed == 0 and
            self._transaction_status() != TRANSACTION_STATUS_IDLE)

    @property
    def is_closed(self):
        """Indicates whether the connection is closed or not."""
        # 0 = open, 1 = closed, 2 = 'something horrible happened'
        return self.connection.closed > 0

    def close(self):
        """Remove the connection from the event_loop and close it."""
        self.connection.close()


    @asyncio.coroutine
    def run_query(self, operation, parameters=()):
        """Execute and fetch SQL query

        :param string sql_query: an SQL query to execute
        :param tuple/list parameters: A list or tuple with query parameters.
            Defaults to an empty tuple.
        :returns tuple: query results
        """
        cursor = yield from self.execute(operation, parameters)
        return cursor.fetchall()

    @asyncio.coroutine
    def run_transaction(self, sql_queries):
        """
        Run multiple sql queries in transaction.

        :param tuple sql_queries: tuple with of (sql, params) pairs to execute
        :returns tuple: query results
        """
        cursors, q = [], deque()
        for sql in sql_queries:
            q.append((sql, ())) if isinstance(sql, str) else q.append(sql[:2])
        q.appendleft(("BEGIN;", ()))
        q.append(("COMMIT;", ()))

        for sql, params in q:
            try:
                cursors.append((yield from self.execute(sql, params)))
            except Exception:
                #TODO: fix exception
                self.execute("ROLLBACK;")
                raise
        # no need include cursors for BEGIN and COMMIT command
        results = [c.fetchall() for c in cursors[1:-1]]
        return results

    def register_hstore(self):
        # TODO: implement
        raise NotImplementedError
