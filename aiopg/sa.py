"""Optional support for sqlalchemy.sql dynamic query generation."""

import asyncio
from collections import Sequence, Mapping

try:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
    from sqlalchemy.sql import ClauseElement
except ImportError:  # pragma: no cover
    raise ImportError('aiopg.sa requires sqlalchemy')


from .connection import connect as base_connect, Connection
from .cursor import Cursor
from .pool import create_pool as base_create_pool


dialect = PGDialect_psycopg2()


@asyncio.coroutine
def connect(dsn=None, *, loop=None, **kwargs):
    return (yield from base_connect(dsn, loop=loop,
                                    _connection_factory=SAConnection,
                                    **kwargs))


@asyncio.coroutine
def create_pool(dsn=None, *, minsize=10, maxsize=10,
                loop=None, **kwargs):
    return (yield from base_create_pool(dsn,
                                        minsize=minsize,
                                        maxsize=maxsize,
                                        loop=loop,
                                        _connection_factory=SAConnection,
                                        **kwargs))


class SACursor(Cursor):

    def __init__(self, conn, impl, dialect):
        super().__init__(conn, impl)
        self._dialect = dialect

    @property
    def dialect(self):
        """sqlalchemy dialect, PGDialect_psycopg2() by default."""
        return self._dialect

    @asyncio.coroutine
    def execute(self, operation, parameters=()):
        if isinstance(operation, ClauseElement):
            assert parameters == (), ("Don't mix sqlalchemy clause "
                                      "and execution with parameters")
            compiled = operation.compile(dialect=self._dialect)
            parameters = compiled.params
            return (yield from super().execute(str(compiled), parameters))
        else:
            return (yield from super().execute(operation, parameters))

    @asyncio.coroutine
    def scalar(self, operation):
        yield from self.execute(operation)
        if self.rowcount != 1:
            raise ValueError("operation return not exactly 1 row")
        ret = yield from self.fetchone()
        if isinstance(ret, Sequence):
            assert len(ret) == 1, "Bad SQL {!r}".format(operation)
            return ret[0]
        elif isinstance(ret, Mapping):
            assert len(ret) == 1, "Bad SQL {!r}".format(operation)
            return ret[next(iter(ret))]  # return the value for single key
        else:
            raise ValueError("the result of SQL execution is "
                             "something terrible ({!r})".format(ret))


class SAConnection(Connection):

    dialect = dialect

    def __init__(self, dsn, loop, waiter, **kwargs):
        super().__init__(dsn, loop, waiter, **kwargs)

    @asyncio.coroutine
    def cursor(self, name=None, cursor_factory=None,
               scrollable=None, withhold=False, dialect=dialect):
        impl = yield from self._cursor(name=name,
                                       cursor_factory=cursor_factory,
                                       scrollable=scrollable,
                                       withhold=withhold)
        return SACursor(self, impl, dialect)


# -----------------------------------------------------------------------------


class Connection:

    def __init__(self):
        self._connection = connection

    def execute(self, obj, *multiparams, **params):
        pass

    def scalar(self, obj, *multiparams, **params):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_vzl, exc_tb):
        self.close()

    @property
    def closed(self):
        pass

    @property
    def info(self):
        return ## self.connection.info

    def connect(self):
        return self._branch()  # ???

    @property
    def connection(self):
        return self._connection

    def begin(self):
        """Begin a transaction and return a transaction handle.

        The returned object is an instance of :class:`.Transaction`.
        This object represents the "scope" of the transaction,
        which completes when either the :meth:`.Transaction.rollback`
        or :meth:`.Transaction.commit` method is called.

        Nested calls to :meth:`.begin` on the same :class:`.Connection`
        will return new :class:`.Transaction` objects that represent
        an emulated transaction within the scope of the enclosing
        transaction, that is::

            trans = conn.begin()   # outermost transaction
            trans2 = conn.begin()  # "nested"
            trans2.commit()        # does nothing
            trans.commit()         # actually commits

        Calls to :meth:`.Transaction.commit` only have an effect
        when invoked via the outermost :class:`.Transaction` object, though the
        :meth:`.Transaction.rollback` method of any of the
        :class:`.Transaction` objects will roll back the
        transaction.

        See also:

        :meth:`.Connection.begin_nested` - use a SAVEPOINT

        :meth:`.Connection.begin_twophase` - use a two phase /XID transaction

        :meth:`.Engine.begin` - context manager available from
        :class:`.Engine`.

        """
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            return self._transaction
        else:
            return Transaction(self, self._transaction)

    def begin_nested(self):
        """Begin a nested transaction and return a transaction handle.

        The returned object is an instance of :class:`.NestedTransaction`.

        Nested transactions require SAVEPOINT support in the
        underlying database.  Any transaction in the hierarchy may
        ``commit`` and ``rollback``, however the outermost transaction
        still controls the overall ``commit`` or ``rollback`` of the
        transaction of a whole.

        See also :meth:`.Connection.begin`,
        :meth:`.Connection.begin_twophase`.
        """
        if self._transaction is None:
            self._transaction = RootTransaction(self)
        else:
            self._transaction = NestedTransaction(self, self._transaction)
        return self._transaction

    def begin_twophase(self, xid=None):
        """Begin a two-phase or XA transaction and return a transaction
        handle.

        The returned object is an instance of :class:`.TwoPhaseTransaction`,
        which in addition to the methods provided by
        :class:`.Transaction`, also provides a
        :meth:`~.TwoPhaseTransaction.prepare` method.

        :param xid: the two phase transaction id.  If not supplied, a
          random id will be generated.

        See also :meth:`.Connection.begin`,
        :meth:`.Connection.begin_twophase`.

        """

        if self._transaction is not None:
            raise exc.InvalidRequestError(
                "Cannot start a two phase transaction when a transaction "
                "is already in progress.")
        if xid is None:
            xid = self.engine.dialect.create_xid()
        self._transaction = TwoPhaseTransaction(self, xid)
        return self._transaction

    def recover_twophase(self):
        return self.engine.dialect.do_recover_twophase(self)

    def rollback_prepared(self, xid, recover=False):
        self.engine.dialect.do_rollback_twophase(self, xid, recover=recover)

    def commit_prepared(self, xid, recover=False):
        self.engine.dialect.do_commit_twophase(self, xid, recover=recover)

    def in_transaction(self):
        """Return True if a transaction is in progress."""

        return self._transaction is not None

    def close(self):
        """Close this :class:`.Connection`.

        This results in a release of the underlying database
        resources, that is, the DBAPI connection referenced
        internally. The DBAPI connection is typically restored
        back to the connection-holding :class:`.Pool` referenced
        by the :class:`.Engine` that produced this
        :class:`.Connection`. Any transactional state present on
        the DBAPI connection is also unconditionally released via
        the DBAPI connection's ``rollback()`` method, regardless
        of any :class:`.Transaction` object that may be
        outstanding with regards to this :class:`.Connection`.

        After :meth:`~.Connection.close` is called, the
        :class:`.Connection` is permanently in a closed state,
        and will allow no further operations.

        """
        try:
            conn = self._connection
        except AttributeError:
            pass
        else:
            conn.close()
            del self._connection
        self._can_reconnect = False
        self._transaction = None


class Transaction(object):
    """Represent a database transaction in progress.

    The :class:`.Transaction` object is procured by
    calling the :meth:`~.Connection.begin` method of
    :class:`.Connection`::

        from sqlalchemy import create_engine
        engine = create_engine("postgresql://scott:tiger@localhost/test")
        connection = engine.connect()
        trans = connection.begin()
        connection.execute("insert into x (a, b) values (1, 2)")
        trans.commit()

    The object provides :meth:`.rollback` and :meth:`.commit`
    methods in order to control transaction boundaries.  It
    also implements a context manager interface so that
    the Python ``with`` statement can be used with the
    :meth:`.Connection.begin` method::

        with connection.begin():
            connection.execute("insert into x (a, b) values (1, 2)")

    The Transaction object is **not** threadsafe.

    See also:  :meth:`.Connection.begin`, :meth:`.Connection.begin_twophase`,
    :meth:`.Connection.begin_nested`.

    .. index::
      single: thread safety; Transaction
    """

    def __init__(self, connection, parent):
        self.connection = connection
        self._parent = parent or self
        self.is_active = True

    def close(self):
        """Close this :class:`.Transaction`.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosing transaction.

        """
        if not self._parent.is_active:
            return
        if self._parent is self:
            self.rollback()

    def rollback(self):
        """Roll back this :class:`.Transaction`.

        """
        if not self._parent.is_active:
            return
        self._do_rollback()
        self.is_active = False

    def _do_rollback(self):
        self._parent.rollback()

    def commit(self):
        """Commit this :class:`.Transaction`."""

        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        self._do_commit()
        self.is_active = False

    def _do_commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None and self.is_active:
            try:
                self.commit()
            except:
                with util.safe_reraise():
                    self.rollback()
        else:
            self.rollback()


class RootTransaction(Transaction):
    def __init__(self, connection):
        super(RootTransaction, self).__init__(connection, None)
        self.connection._begin_impl(self)

    def _do_rollback(self):
        if self.is_active:
            self.connection._rollback_impl()

    def _do_commit(self):
        if self.is_active:
            self.connection._commit_impl()


class NestedTransaction(Transaction):
    """Represent a 'nested', or SAVEPOINT transaction.

    A new :class:`.NestedTransaction` object may be procured
    using the :meth:`.Connection.begin_nested` method.

    The interface is the same as that of :class:`.Transaction`.

    """
    def __init__(self, connection, parent):
        super(NestedTransaction, self).__init__(connection, parent)
        self._savepoint = self.connection._savepoint_impl()

    def _do_rollback(self):
        if self.is_active:
            self.connection._rollback_to_savepoint_impl(
                                    self._savepoint, self._parent)

    def _do_commit(self):
        if self.is_active:
            self.connection._release_savepoint_impl(
                                    self._savepoint, self._parent)


class TwoPhaseTransaction(Transaction):
    """Represent a two-phase transaction.

    A new :class:`.TwoPhaseTransaction` object may be procured
    using the :meth:`.Connection.begin_twophase` method.

    The interface is the same as that of :class:`.Transaction`
    with the addition of the :meth:`prepare` method.

    """
    def __init__(self, connection, xid):
        super(TwoPhaseTransaction, self).__init__(connection, None)
        self._is_prepared = False
        self.xid = xid
        self.connection._begin_twophase_impl(self)

    def prepare(self):
        """Prepare this :class:`.TwoPhaseTransaction`.

        After a PREPARE, the transaction can be committed.

        """
        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        self.connection._prepare_twophase_impl(self.xid)
        self._is_prepared = True

    def _do_rollback(self):
        self.connection._rollback_twophase_impl(self.xid, self._is_prepared)

    def _do_commit(self):
        self.connection._commit_twophase_impl(self.xid, self._is_prepared)



class ResultProxy:
    """Wraps a DB-API cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by ``schema.Column``
    object. e.g.::

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ``ResultProxy`` also handles post-processing of result column
    data using ``TypeEngine`` objects, which are referenced from
    the originating SQL statement that produced this result set.

    """

    @util.memoized_property
    def rowcount(self):
        """Return the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.

        .. note::

           Notes regarding :attr:`.ResultProxy.rowcount`:


           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.
             On backends that feature both styles, such as MySQL,
             rowcount is configured by default to return the match
             count in all cases.

           * :attr:`.ResultProxy.rowcount` is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are
             unbuffered.

           * :attr:`.ResultProxy.rowcount` may not be fully implemented by
             all dialects.  In particular, most DBAPIs do not support an
             aggregate rowcount result from an executemany call.
             The :meth:`.ResultProxy.supports_sane_rowcount` and
             :meth:`.ResultProxy.supports_sane_multi_rowcount` methods
             will report from the dialect if each usage is known to be
             supported.

           * Statements that use RETURNING may not return a correct
             rowcount.

        """
        try:
            return self.context.rowcount
        except Exception as e:
            self.connection._handle_dbapi_exception(
                              e, None, None, self.cursor, self.context)

    @property
    def lastrowid(self):
        """return the 'lastrowid' accessor on the DBAPI cursor.

        This is a DBAPI specific method and is only functional
        for those backends which support it, for statements
        where it is appropriate.  It's behavior is not
        consistent across backends.

        Usage of this method is normally unnecessary when
        using insert() expression constructs; the
        :attr:`~ResultProxy.inserted_primary_key` attribute provides a
        tuple of primary key values for a newly inserted row,
        regardless of database backend.

        """
        try:
            return self._saved_cursor.lastrowid
        except Exception as e:
            self.connection._handle_dbapi_exception(
                                 e, None, None,
                                 self._saved_cursor, self.context)

    @property
    def returns_rows(self):
        """True if this :class:`.ResultProxy` returns rows.

        I.e. if it is legal to call the methods
        :meth:`~.ResultProxy.fetchone`,
        :meth:`~.ResultProxy.fetchmany`
        :meth:`~.ResultProxy.fetchall`.

        """
        return self._metadata is not None

    def close(self, _autoclose_connection=True):
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
            self.closed = True
            self.connection._safe_close_cursor(self.cursor)
            if _autoclose_connection and \
                self.connection.should_close_with_result:
                self.connection.close()
            # allow consistent errors
            self.cursor = None

    def __iter__(self):
        while True:
            row = yield from self.fetchone()
            if row is None:
                raise StopIteration
            else:
                yield row

    @asyncio.coroutine
    def fetchall(self):
        pass

    @asyncio.coroutine
    def fetchone(self):
        pass

    @asyncio.coroutine
    def fetchmany(self, size=None):
        pass
