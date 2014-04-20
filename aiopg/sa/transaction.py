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
