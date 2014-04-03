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
        return elf._impl.closed

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

    @coroutine
    def execute(self, operation, parameters=()):
        self._impl.execute(operation, parameters)
        yield from self._poll()

    @coroutine
    def executemany(self, operation, seq_of_parameters):
        self._impl.executemany(operation, seq_of_parameters)
        yield from self._poll()

    @coroutine
    def callproc(self, procname, parameters):
        self._impl.callproc(procname, parameters)
        yield from self._poll()

    @coroutine
    def mogrify(self, procname, parameters):
        self._impl.callproc(procname, parameters)
        yield from self._poll()

    @coroutine
    def setinputsizes(self, sizes):
        self._impl.setinputsizes(sizes)

    @coroutine
    def fetchone(self):
        ret = self._impl.fetchone()
        assert not self._conn.isexecuted(), ("Don't support server side "
                                             "cursors yet")
        return ret

    @coroutine
    def fetchmany(self, size=None):
        if size is None:
            size = self._impl.arraysize
        ret = self._impl.fetchmany(size)
        assert not self._conn.isexecuted(), ("Don't support server side "
                                             "cursors yet")
        return ret

    @coroutine
    def fetchall(self):
        ret = self._impl.fetchall()
        assert not self._conn.isexecuted(), ("Don't support server side "
                                             "cursors yet")
        return ret

    @coroutine
    def scroll(self, value, mode="relative"):
        ret = self._impl.scroll(value, mode)
        assert not self._conn.isexecuted(), ("Don't support server side "
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

    @coroutine
    def cast(self, old, s):
        ret = self._impl.cast(old, s)
        assert not self._conn.isexecuted(), ("Don't support server side "
                                             "cursors yet")
        return ret

    @property
    def tzinfo_factory(self):
        return self._impl.tzinfo_factory

    @scrollable.setter
    def tzinfo_factory(self, val):
        self._impl.tzinfo_factory = val

    @coroutine
    def nextset(self):
        ret = self._impl.nextset()
        assert not self._conn.isexecuted(), ("Don't support server side "
                                             "cursors yet")
        return ret

    @coroutine
    def setoutputsizes(self, size, column=None):
        self._impl.setoutputsizes(size, column)

    @coroutine
    def copy_from(file, table, sep='\t', null='\\N', size=8192, columns=None):
        self._impl.copy_from(file, table,
                             sep=sep, null=null, size=size, columns=columns)
        yield from self._poll()

    @coroutine
    def copy_to(file, table, sep='\t', null='\\N', columns=None):
        self._impl.copy_to(file, table,
                             sep=sep, null=null, columns=columns)
        yield from self._poll()

    @coroutine
    def copy_expert(sql, file, size=8192):
        self._impl.copy_expert(sql, file, size)
        yield from self._poll()
