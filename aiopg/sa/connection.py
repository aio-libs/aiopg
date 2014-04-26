import asyncio
import weakref

from sqlalchemy.sql import ClauseElement
from .result import ResultProxy
from . import exc
from .transaction import (RootTransaction, Transaction,
                          NestedTransaction, TwoPhaseTransaction)


class SAConnection:

    def __init__(self, connection, dialect):
        self._dialect = dialect
        self._connection = connection
        self._transaction = None
        self._savepoint_seq = 0
        self._weak_results = weakref.WeakSet()

    @asyncio.coroutine
    def execute(self, query, *multiparams, **params):
        cursor = yield from self._connection.cursor()

        if isinstance(query, str):
            distilled_params = _distill_params(multiparams, params)
            result_map = None
            yield from cursor.execute(query, distilled_params)
        elif isinstance(query, ClauseElement):
            if multiparams or params:
                raise exc.ArgumentError("Don't mix sqlalchemy clause "
                                        "and execution with parameters")
            compiled = query.compile(dialect=self._dialect)
            parameters = compiled.params
            result_map = compiled.result_map
            yield from cursor.execute(str(compiled), parameters)
        else:
            raise exc.ArgumentError("sql statement should be str or "
                                    "SQLAlchemy data "
                                    "selection/modification clause")

        # TODO: add weakref to ResultProxy to close cursor on decref
        ret = ResultProxy(self, cursor, self._dialect, result_map)
        self._weak_results.add(ret)
        return ret

    @asyncio.coroutine
    def scalar(self, query, *multiparams, **params):
        res = yield from self.execute(query, *multiparams, **params)
        return (yield from res.scalar())

    @property
    def closed(self):
        return self._connection is None

    @property
    def info(self):
        return self._connection.info

    @property
    def connection(self):
        return self._connection

    @asyncio.coroutine
    def begin(self):
        """Begin a transaction and return a transaction handle.

        The returned object is an instance of Transaction.
        This object represents the "scope" of the transaction,
        which completes when either the .rollback
        or .commit method is called.

        Nested calls to .begin on the same SAConnection instance
        will return new Transaction objects that represent
        an emulated transaction within the scope of the enclosing
        transaction, that is::

            trans = yield from conn.begin()   # outermost transaction
            trans2 = yield from conn.begin()  # "nested"
            yield from trans2.commit()        # does nothing
            yield from trans.commit()         # actually commits

        Calls to .commit only have an effect
        when invoked via the outermost Transaction object, though the
        .rollback method of any of the
        Transaction objects will roll back the
        transaction.

        See also:
          .begin_nested - use a SAVEPOINT
          .begin_twophase - use a two phase/XA transaction
        """
        if self._transaction is None:
            self._transaction = RootTransaction(self)
            yield from self._begin_impl()
            return self._transaction
        else:
            return Transaction(self, self._transaction)

    @asyncio.coroutine
    def _begin_impl(self):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('BEGIN')
        finally:
            cur.close()

    @asyncio.coroutine
    def _commit_impl(self):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('COMMIT')
        finally:
            cur.close()

    @asyncio.coroutine
    def _rollback_impl(self):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('ROLLBACK')
        finally:
            cur.close()

    @asyncio.coroutine
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
            yield from self._begin_impl()
        else:
            self._transaction = NestedTransaction(self, self._transaction)
            self._transaction._savepoint = yield from self._savepoint_impl()
        return self._transaction

    @asyncio.coroutine
    def _savepoint_impl(self, name=None):
        self._savepoint_seq += 1
        name = 'aiopg_sa_savepoint_%s' % self._savepoint_seq

        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('SAVEPOINT ' + name)
            return name
        finally:
            cur.close()

    @asyncio.coroutine
    def _rollback_to_savepoint_impl(self, name, parent):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('ROLLBACK TO SAVEPOINT ' + name)
        finally:
            cur.close()
        self._transaction = parent

    @asyncio.coroutine
    def _release_savepoint_impl(self, name, parent):
        cur = yield from self._connection.cursor()
        try:
            yield from cur.execute('RELEASE SAVEPOINT ' + name)
        finally:
            cur.close()
        self._transaction = parent

    @asyncio.coroutine
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
            xid = self._dialect.create_xid()
        self._transaction = TwoPhaseTransaction(self, xid)
        yield from self._begin_impl()
        return self._transaction

    @asyncio.coroutine
    def _prepare_twophase_impl(self, xid):
        yield from self.execute("PREPARE TRANSACTION '%s'" % xid)

    @asyncio.coroutine
    def recover_twophase(self):
        """Return a list of prepared twophase transaction ids."""
        result = yield from self.execute("SELECT gid FROM pg_prepared_xacts")
        return [row[0] for row in result]

    @asyncio.coroutine
    def rollback_prepared(self, xid, *, is_prepared=True):
        """Rollback prepared twophase transaction."""
        if is_prepared:
            yield from self.execute("ROLLBACK PREPARED '%s'" % xid)
        else:
            yield from self._rollback_impl()

    @asyncio.coroutine
    def commit_prepared(self, xid, *, is_prepared=True):
        """Commit prepared twophase transaction."""
        if is_prepared:
            self.execute("COMMIT PREPARED '%s'" % xid)
        else:
            yield from self._commit_impl()

    @property
    def in_transaction(self):
        """Return True if a transaction is in progress."""
        return self._transaction is not None and self._transaction.is_active

    @asyncio.coroutine
    def close(self):
        """Close this SAConnection.

        This results in a release of the underlying database
        resources, that is, the underlying connection referenced
        internally. The underlying connection is typically restored
        back to the connection-holding Pool referenced by the Engine
        that produced this SAConnection. Any transactional state
        present on the underlying connection is also unconditionally
        released via calling Transaction.rollback() method.

        After .close() is called, the SAConnection is permanently in a
        closed state, and will allow no further operations.
        """
        try:
            self._connection
        except AttributeError:
            pass
        else:
            if self._transaction is not None:
                yield from self._transaction.rollback()
            # don't close underlying connection, it can be reused by pool
            # conn.close()
            del self._connection
        self._can_reconnect = False
        self._transaction = None


def _distill_params(multiparams, params):
    """Given arguments from the calling form *multiparams, **params,
    return a list of bind parameter structures, usually a list of
    dictionaries.

    In the case of 'raw' execution which accepts positional parameters,
    it may be a list of tuples or lists.

    """

    if not multiparams:
        if params:
            return [params]
        else:
            return []
    elif len(multiparams) == 1:
        zero = multiparams[0]
        if isinstance(zero, (list, tuple)):
            if not zero or hasattr(zero[0], '__iter__') and \
                    not hasattr(zero[0], 'strip'):
                # execute(stmt, [{}, {}, {}, ...])
                # execute(stmt, [(), (), (), ...])
                return zero
            else:
                # execute(stmt, ("value", "value"))
                return [zero]
        elif hasattr(zero, 'keys'):
            # execute(stmt, {"key":"value"})
            return [zero]
        else:
            # execute(stmt, "value")
            return [[zero]]
    else:
        if (hasattr(multiparams[0], '__iter__') and
                not hasattr(multiparams[0], 'strip')):
            return multiparams
        else:
            return [multiparams]
