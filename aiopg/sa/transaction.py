import asyncio
from . import exc


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
        self._connection = connection
        self._parent = parent or self
        self._is_active = True

    @property
    def is_active(self):
        return self._is_active

    @property
    def connection(self):
        return self._connection

    @asyncio.coroutine
    def close(self):
        """Close this :class:`.Transaction`.

        If this transaction is the base transaction in a begin/commit
        nesting, the transaction will rollback().  Otherwise, the
        method returns.

        This is used to cancel a Transaction without affecting the scope of
        an enclosing transaction.

        """
        if not self._parent._is_active:
            return
        if self._parent is self:
            yield from self.rollback()

    @asyncio.coroutine
    def rollback(self):
        """Roll back this :class:`.Transaction`.

        """
        if not self._parent._is_active:
            return
        yield from self._do_rollback()
        self._is_active = False

    @asyncio.coroutine
    def _do_rollback(self):
        yield from self._parent.rollback()

    @asyncio.coroutine
    def commit(self):
        """Commit this :class:`.Transaction`."""

        if not self._parent._is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        yield from self._do_commit()
        self._is_active = False

    @asyncio.coroutine
    def _do_commit(self):
        pass


class RootTransaction(Transaction):

    def __init__(self, connection):
        super().__init__(connection, None)

    @asyncio.coroutine
    def _do_rollback(self):
        if self._is_active:
            cur = yield from self._connection.cursor()
            try:
                yield from cur.execute('ROLLBACK')
            finally:
                cur.close()

    @asyncio.coroutine
    def _do_commit(self):
        if self._is_active:
            cur = yield from self._connection.cursor()
            try:
                yield from cur.execute('COMMIT')
            finally:
                cur.close()


class NestedTransaction(Transaction):
    """Represent a 'nested', or SAVEPOINT transaction.

    A new :class:`.NestedTransaction` object may be procured
    using the :meth:`.Connection.begin_nested` method.

    The interface is the same as that of :class:`.Transaction`.

    """

    _savepoint = None

    def __init__(self, connection, parent):
        super(NestedTransaction, self).__init__(connection, parent)
        self._savepoint = self.connection._savepoint_impl()

    @asyncio.coroutine
    def _do_rollback(self):
        assert self._savepoint is not None, "Broken transaction logic"
        if self._is_active:
            yield from self._connection._rollback_to_savepoint_impl(
                self._savepoint, self._parent)

    @asyncio.coroutine
    def _do_commit(self):
        assert self._savepoint is not None, "Broken transaction logic"
        if self._is_active:
            yield from self._connection._release_savepoint_impl(
                self._savepoint, self._parent)


class TwoPhaseTransaction(Transaction):
    """Represent a two-phase transaction.

    A new :class:`.TwoPhaseTransaction` object may be procured
    using the :meth:`.Connection.begin_twophase` method.

    The interface is the same as that of :class:`.Transaction`
    with the addition of the :meth:`prepare` method.

    """
    def __init__(self, connection, xid):
        super().__init__(connection, None)
        self._is_prepared = False
        self._xid = xid
        self.connection._begin_twophase_impl(self)

    @property
    def xid(self):
        return self._xid

    @asyncio.coroutine
    def prepare(self):
        """Prepare this :class:`.TwoPhaseTransaction`.

        After a PREPARE, the transaction can be committed.

        """
        if not self._parent.is_active:
            raise exc.InvalidRequestError("This transaction is inactive")
        yield from self._connection._prepare_twophase_impl(self._xid)
        self._is_prepared = True

    @asyncio.coroutine
    def _do_rollback(self):
        yield from self._connection._rollback_twophase_impl(
            self._xid, self._is_prepared)

    @asyncio.coroutine
    def _do_commit(self):
        yield from self._connection._commit_twophase_impl(
            self._xid, self._is_prepared)
