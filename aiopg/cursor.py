import asyncio


class Cursor:

    def __init__(self, conn, impl):
        self._conn = conn
        self._impl = impl

    @property
    def description(self):
        """XXX"""
        return self._impl.description

    def close(self):
        if self._conn is None:
            return
        self._conn = None
        self._impl.close()

    @property
    def closed(self):
        return self._impl.closed

    @property
    def connection(self):
        return self._conn

    @property
    def name(self):
        return self._impl.name

    @property
    def scrollable(self):
        return self._impl.scrollable

    @scrollable.setter
    def scrollable(self, val):
        self._impl.scrollable = val

    @property
    def withhold(self):
        return self._impl.withhold

    @withhold.setter
    def withhold(self, val):
        self._impl.withhold = val

    @asyncio.coroutine
    def execute(self, operation, parameters=()):
        self._conn._create_waiter('cursor.execute')
        self._impl.execute(operation, parameters)
        yield from self._conn._poll()

    @asyncio.coroutine
    def executemany(self, operation, seq_of_parameters):
        self._conn._create_waiter('cursor.executemany')
        self._impl.executemany(operation, seq_of_parameters)
        yield from self._conn._poll()

    @asyncio.coroutine
    def callproc(self, procname, parameters):
        self._conn._create_waiter('cursor.callproc')
        self._impl.callproc(procname, parameters)
        yield from self._conn._poll()

    @asyncio.coroutine
    def mogrify(self, procname, parameters):
        self._conn._create_waiter('cursor.mogrify')
        self._impl.callproc(procname, parameters)
        yield from self._conn._poll()

    @asyncio.coroutine
    def setinputsizes(self, sizes):
        self._impl.setinputsizes(sizes)

    @asyncio.coroutine
    def fetchone(self):
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

    @asyncio.coroutine
    def cast(self, old, s):
        ret = self._impl.cast(old, s)
        assert not self._conn._isexecuting(), ("Don't support server side "
                                               "cast")
        return ret

    @property
    def tzinfo_factory(self):
        return self._impl.tzinfo_factory

    @tzinfo_factory.setter
    def tzinfo_factory(self, val):
        self._impl.tzinfo_factory = val

    @asyncio.coroutine
    def nextset(self):
        ret = self._impl.nextset()
        return ret

    @asyncio.coroutine
    def setoutputsizes(self, size, column=None):
        self._impl.setoutputsizes(size, column)

    @asyncio.coroutine
    def copy_from(self, file, table, sep='\t', null='\\N', size=8192,
                  columns=None):
        self._conn._create_waiter('cursor.copy_from')
        self._impl.copy_from(file, table,
                             sep=sep, null=null, size=size, columns=columns)
        yield from self._conn._poll()

    @asyncio.coroutine
    def copy_to(self, file, table, sep='\t', null='\\N', columns=None):
        self._conn._create_waiter('cursor.copy_to')
        self._impl.copy_to(file, table,
                           sep=sep, null=null, columns=columns)
        yield from self._conn._poll()

    @asyncio.coroutine
    def copy_expert(self, sql, file, size=8192):
        self._conn._create_waiter('cursor.copy_expert')
        self._impl.copy_expert(sql, file, size)
        yield from self._conn._poll()
