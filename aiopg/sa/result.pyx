#cython: language_level=3, boundscheck=False, wraparound=False, nonecheck=False
from cpython cimport Py_XINCREF, PyObject
from cpython.tuple cimport PyTuple_SET_ITEM, PyTuple_GET_ITEM, PyTuple_GetItem
from cpython.dict cimport PyDict_GetItem
from cpython.list cimport PyList_New, PyList_SET_ITEM
from sqlalchemy.sql import expression, sqltypes

from . import exc


cdef class RowProxy:
    cdef ResultMetaData _result_metadata
    cdef tuple _row
    cdef dict _key_to_index

    def __cinit__(self, result_metadata, row, key_to_index):
        """RowProxy objects are constructed by ResultProxy objects."""
        self._result_metadata = result_metadata
        self._row = row
        self._key_to_index = key_to_index

    def __iter__(self):
        return iter(self._result_metadata.keys)

    def __len__(self):
        return len(self._row)

    cdef object _getitem(self, key):
        cdef PyObject* item
        cdef int index
        cdef tuple data

        item = PyDict_GetItem(self._key_to_index, key)
        if item is NULL:
            fallback_key = self._result_metadata._key_fallback(key)
            if fallback_key is None:
                raise KeyError(key)
            item = PyDict_GetItem(self._key_to_index, fallback_key)
            if item is NULL:
                raise KeyError(key)
        index = <object>item
        if index is None:
            raise exc.InvalidRequestError(
                "Ambiguous column name %s in result set! "
                "try 'use_labels' option on select statement." % repr(key)
            )
        item = PyTuple_GetItem(self._row, index)
        if item is NULL:
            return None
        return <object>item

    def __getitem__(self, item):
        cdef PyObject* result

        if isinstance(item, int):
            result = PyTuple_GetItem(self._row, item)
            if result is NULL:
                raise KeyError(item)
            return <object>result
        return self._getitem(item)

    cdef object _getattr(self, str name):
        try:
            return self._getitem(name)
        except KeyError as e:
            raise AttributeError(e.args[0])

    def __getattr__(self, str item):
        return self._getattr(item)

    def __contains__(self, key):
        return self._result_metadata._has_key(self._row, key)

    __hash__ = None

    def __eq__(self, other):
        if hasattr(other, 'as_tuple'):
            return self.as_tuple() == other.as_tuple()
        elif hasattr(other, 'index') and hasattr(other, 'count'):
            return self.as_tuple() == other
        else:
            return NotImplemented

    def __ne__(self, other):
        return not self == other

    def as_tuple(self):
        return tuple(self[k] for k in self)

    def __repr__(self):
        return repr(self.as_tuple())

    def keys(self):
        return list(self._result_metadata.keys)

    def values(self):
        return list(self._row)

    def items(self):
        return zip(self._result_metadata.keys, self._row)

    def get(self, item, default=None):
        try:
            return self._getitem(item)
        except KeyError:
            if default is not None:
                return default
            return None


cdef class ResultMetaData:
    """Handle cursor.description, applying additional info from an execution
    context."""
    cdef public list keys
    cdef public dict _keymap
    cdef public list _processors
    cdef public dict _key_to_index

    def __init__(self, ResultProxy result_proxy, cursor_description):
        map_type, map_column_name = self.result_map(result_proxy._result_map)

        # We do not strictly need to store the processor in the key mapping,
        # though it is faster in the Python version (probably because of the
        # saved attribute lookup self._processors)
        self._keymap = keymap = {}
        self._processors = processors = []
        self._key_to_index = key_to_index = {}
        self.keys = []
        dialect = result_proxy.dialect

        # `dbapi_type_map` property removed in SQLAlchemy 1.2+.
        # Usage of `getattr` only needed for backward compatibility with
        # older versions of SQLAlchemy.
        typemap = getattr(dialect, 'dbapi_type_map', {})

        assert dialect.case_sensitive, \
            "Doesn't support case insensitive database connection"

        assert not dialect.description_encoding, \
            "psycopg in py3k should not use this"

        ambiguous = []

        for i, rec in enumerate(cursor_description):
            colname = rec[0]
            coltype = rec[1]

            # PostgreSQL doesn't require this.
            # if dialect.requires_name_normalize:
            #     colname = dialect.normalize_name(colname)

            name = str(map_column_name.get(colname, colname))
            type_ = map_type.get(
                colname,
                typemap.get(coltype, sqltypes.NULLTYPE)
            )
            processor = type_._cached_result_processor(dialect, coltype)

            rec = (processor, i)

            if name in keymap:
                ambiguous.append(name)
            keymap[name] = rec
            key_to_index[name] = i

            self.keys.append(name)

        for name in ambiguous:
            keymap[name] = (None, None)
            key_to_index[name] = None

        for processor, i in keymap.values():
            if processor is not None:
                processors.append((processor, i))

    def result_map(self, data_map):
        data_map = data_map or {}
        map_type = {}
        map_column_name = {}
        for elem in data_map:
            name = elem[0]
            priority_name = getattr(elem[2][0], 'key', name)
            map_type[name] = elem[3]  # type column
            map_column_name[name] = priority_name

        return map_type, map_column_name

    def _key_fallback(self, key):
        map = self._keymap
        result = None
        # fallback for targeting a ColumnElement to a textual expression
        # this is a rare use case which only occurs when matching text()
        # or colummn('name') constructs to ColumnElements, or after a
        # pickle/unpickle roundtrip
        if isinstance(key, expression.ColumnElement):
            if (key._label and key._label in map):
                result = key._label
            elif (hasattr(key, 'key') and key.key in map):
                # match is only on name.
                result = key.key

        return result

    def _has_key(self, row, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key) in self._keymap


cdef class ResultProxy:
    """Wraps a DB-API cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by sqlalchemy schema.Column
    object. e.g.:

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ResultProxy also handles post-processing of result column
    data using sqlalchemy TypeEngine objects, which are referenced from
    the originating SQL statement that produced this result set.
    """
    cdef object _dialect
    cdef public object _result_map
    cdef object _cursor
    cdef object _connection
    cdef object _rowcount
    cdef object _metadata

    def __cinit__(self, connection, cursor, dialect, result_map=None):
        self._dialect = dialect
        self._result_map = result_map
        self._cursor = cursor
        self._connection = connection
        self._rowcount = cursor.rowcount
        self._metadata = None
        self._init_metadata()

    @property
    def dialect(self):
        """SQLAlchemy dialect."""
        return self._dialect

    @property
    def cursor(self):
        return self._cursor

    def keys(self):
        """Return the current set of string keys for rows."""
        if self._metadata:
            return tuple(self._metadata.keys)
        else:
            return ()

    @property
    def rowcount(self):
        """Return the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.

        .. note::

           Notes regarding .rowcount:


           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.

           * .rowcount is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are
             unbuffered.

           * Statements that use RETURNING may not return a correct
             rowcount.
        """
        return self._rowcount

    def _init_metadata(self):
        cursor_description = self.cursor.description
        if cursor_description is not None:
            self._metadata = ResultMetaData(self, cursor_description)
        else:
            self.close()

    @property
    def returns_rows(self):
        """True if this ResultProxy returns rows.

        I.e. if it is legal to call the methods .fetchone(),
        .fetchmany() and .fetchall()`.
        """
        return self._metadata is not None

    @property
    def closed(self):
        if self._cursor is None:
            return True

        return bool(self._cursor.closed)

    def close(self):
        """Close this ResultProxy.

        Closes the underlying DBAPI cursor corresponding to the execution.

        Note that any data cached within this ResultProxy is still available.
        For some types of results, this may include buffered rows.

        If this ResultProxy was generated from an implicit execution,
        the underlying Connection will also be closed (returns the
        underlying DBAPI connection to the connection pool.)

        This method is called automatically when:

        * all result rows are exhausted using the fetchXXX() methods.
        * cursor.description is None.
        """

        if not self.closed:
            self.cursor.close()
            # allow consistent errors
            self._cursor = None

    def __del__(self):
        self.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        ret = await self.fetchone()
        if ret is not None:
            return ret
        else:
            raise StopAsyncIteration

    def _non_result(self):
        if self._metadata is None:
            raise exc.ResourceClosedError(
                "This result object does not return rows. "
                "It has been closed automatically.")
        else:
            raise exc.ResourceClosedError("This result object is closed.")

    cdef list _process_rows(self, list rows):
        cdef int i
        cdef list results

        results = PyList_New(len(rows))
        metadata = self._metadata
        key_to_index = metadata._key_to_index
        processors = self._metadata._processors
        i = 0
        for row in rows:
            for processor, index in processors:
                value = <object>PyTuple_GET_ITEM(row, index)
                if value is not None:
                    value = processor(value)
                    Py_XINCREF(<PyObject*>value)

                PyTuple_SET_ITEM(row, index, value)
            row_proxy = RowProxy(
                metadata,
                row,
                key_to_index
            )
            Py_XINCREF(<PyObject*>row_proxy)
            check = PyList_SET_ITEM(results, i, row_proxy)
            i += 1

        return results

    async def fetchall(self):
        """Fetch all rows, just like DB-API cursor.fetchall()."""
        try:
            rows = await self.cursor.fetchall()
        except AttributeError:
            self._non_result()
        else:
            res = self._process_rows(rows)
            self.close()
            return res

    async def fetchone(self):
        """Fetch one row, just like DB-API cursor.fetchone().

        If a row is present, the cursor remains open after this is called.
        Else the cursor is automatically closed and None is returned.
        """
        try:
            row = await self.cursor.fetchone()
        except AttributeError:
            self._non_result()
        else:
            if row is not None:
                return self._process_rows([row])[0]
            else:
                self.close()
                return None

    async def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API
        cursor.fetchmany(size=cursor.arraysize).

        If rows are present, the cursor remains open after this is called.
        Else the cursor is automatically closed and an empty list is returned.
        """
        try:
            if size is None:
                rows = await self.cursor.fetchmany()
            else:
                rows = await self.cursor.fetchmany(size)
        except AttributeError:
            self._non_result()
        else:
            res = self._process_rows(rows)
            if len(res) == 0:
                self.close()
            return res

    async def first(self):
        """Fetch the first row and then close the result set unconditionally.

        Returns None if no row is present.
        """
        if self._metadata is None:
            self._non_result()
        try:
            return await self.fetchone()
        finally:
            self.close()

    async def scalar(self):
        """Fetch the first column of the first row, and close the result set.

        Returns None if no row is present.
        """
        row = await self.first()
        if row is not None:
            return row[0]
        else:
            return None
