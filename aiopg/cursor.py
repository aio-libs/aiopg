import asyncio
import psycopg2


class Cursor:

    def __init__(self, conn, impl):
        self._conn = conn
        self._impl = impl

    @property
    def description(self):
        """XXX"""
        return self._impl.description

    def close(self):
        """XXX"""
        self._impl.close()

    @property
    def closed(self):
        """XXX"""
        return self._impl.closed

    @property
    def connection(self):
        """XXX"""
        return self._conn

    @property
    def name(self):
        """XXX"""
        return self._impl.name

    @property
    def scrollable(self):
        """XXX"""
        return self._impl.scrollable

    @scrollable.setter
    def scrollable(self, val):
        """XXX"""
        self._impl.scrollable = val

    @property
    def withhold(self):
        """XXX 1"""
        return self._impl.withhold

    @withhold.setter
    def withhold(self, val):
        """XXX 2"""
        self._impl.withhold = val

    @asyncio.coroutine
    def execute(self, operation, parameters=()):
        """XXX"""
        waiter = self._conn._create_waiter('cursor.execute')
        self._impl.execute(operation, parameters)
        yield from self._conn._poll(waiter)

    @asyncio.coroutine
    def executemany(self, operation, seq_of_parameters):
        """XXX"""
        raise psycopg2.ProgrammingError(
            "executemany cannot be used in asynchronous mode")

    @asyncio.coroutine
    def callproc(self, procname, parameters):
        """XXX"""
        waiter = self._conn._create_waiter('cursor.callproc')
        self._impl.callproc(procname, parameters)
        yield from self._conn._poll(waiter)

    @asyncio.coroutine
    def mogrify(self, operation, parameters=()):
        """XXX"""
        ret = self._impl.mogrify(operation, parameters)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "mogrify")
        return ret

    @asyncio.coroutine
    def setinputsizes(self, sizes):
        """XXX"""
        self._impl.setinputsizes(sizes)

    @asyncio.coroutine
    def fetchone(self):
        """XXX"""
        ret = self._impl.fetchone()
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @asyncio.coroutine
    def fetchmany(self, size=None):
        if size is None:
            size = self._impl.arraysize
        ret = self._impl.fetchmany(size)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @asyncio.coroutine
    def fetchall(self):
        ret = self._impl.fetchall()
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @asyncio.coroutine
    def scroll(self, value, mode="relative"):
        ret = self._impl.scroll(value, mode)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cursors yet")
        return ret

    @property
    def arraysize(self):
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, val):
        self._impl.arraysize = val

    @property
    def itersize(self):
        return self._impl.itersize

    @itersize.setter
    def itersize(self, val):
        self._impl.itersize = val

    @property
    def rowcount(self):
        return self._impl.rowcount

    @property
    def rownumber(self):
        return self._impl.rownumber

    @property
    def lastrowid(self):
        return self._impl.lastrowid

    @property
    def query(self):
        return self._impl.query

    @property
    def statusmessage(self):
        return self._impl.statusmessage

    # @asyncio.coroutine
    # def cast(self, old, s):
    #     ret = self._impl.cast(old, s)
    #     assert not self._conn._isexecuting(), ("Don't support server side "
    #                                            "cast")
    #     return ret

    @property
    def tzinfo_factory(self):
        return self._impl.tzinfo_factory

    @tzinfo_factory.setter
    def tzinfo_factory(self, val):
        self._impl.tzinfo_factory = val

    @asyncio.coroutine
    def nextset(self):
        self._impl.nextset()  # raises psycopg2.NotSupportedError

    @asyncio.coroutine
    def setoutputsize(self, size, column=None):
        self._impl.setoutputsize(size, column)

    @asyncio.coroutine
    def copy_from(self, file, table, sep='\t', null='\\N', size=8192,
                  columns=None):
        raise psycopg2.ProgrammingError(
            "copy_from cannot be used in asynchronous mode")

    @asyncio.coroutine
    def copy_to(self, file, table, sep='\t', null='\\N', columns=None):
        raise psycopg2.ProgrammingError(
            "copy_to cannot be used in asynchronous mode")

    @asyncio.coroutine
    def copy_expert(self, sql, file, size=8192):
        raise psycopg2.ProgrammingError(
            "copy_expert cannot be used in asynchronous mode")
