.. _aiopg-rpc:

:mod:`aiopg.sa` --- support for SQLAlchemy functional SQL layer
===============================================================

.. module:: aiopg.sa
   :synopsis: support for SQLAlchemy functional SQL layer
.. currentmodule:: aiopg.sa


Intro
-----

While :ref:`core API <aiopg-core>` provides a core support for access
to :term:`PostgreSQL` database, I found manipulations with raw SQL
strings too annoying.

Fortunately we can use excellent :ref:`core_toplevel` as **SQL query builder**.

Example::

   TBD


Connection
----------

Class :class:`SAConnection` is a wrapper for :class:`aiopg.Connection`.

The class provides methods to execute *SQL queries* and to work *with
SQL transactions*.


.. class:: SAConnection

   .. method::execute(query, *multiparams, **params)

      Executes a *SQL* *query* with optional parameters.

      This method is a :ref:`coroutine <coroutine>`.

      :param query: TBD
      :returns: :class:`ResultProxy` instance with results of SQL
                query execution.

   .. method:: scalar(query, *multiparams, **params)

      Executes a *SQL* *query* and returns a scalar value.

      This method is a :ref:`coroutine <coroutine>`.

      .. seealso:: :meth:`SAConnection.execute` and :meth:`ResultProxy.scalar`.

   .. attribute:: closed

      Is connections closed? The readonly property.

   .. method:: begin()

      Begin a transaction and return a transaction handle.

      The returned object is an instance of :class:`.Transaction`.
      This object represents the "scope" of the transaction,
      which completes when either the :meth:`.Transaction.rollback`
      or :meth:`.Transaction.commit` method is called.

      Nested calls to :meth:`.begin` on the same :class:`.SAConnection`
      will return new :class:`.Transaction` objects that represent
      an emulated transaction within the scope of the enclosing
      transaction, that is::

            trans = yield from conn.begin()   # outermost transaction
            trans2 = yield from conn.begin()  # "inner"
            yield from trans2.commit()        # does nothing
            yield from trans.commit()         # actually commits

        Calls to :meth:`.Transaction.commit` only have an effect
        when invoked via the outermost :class:`.Transaction` object, though the
        :meth:`.Transaction.rollback` method of any of the
        :class:`.Transaction` objects will roll back the
        transaction.

        .. seealso::

           :meth:`.SAConnection.begin_nested` - use a SAVEPOINT

           :meth:`.Connection.begin_twophase` - use a two phase /XA transaction




ResultProxy
-----------

RowProxy
--------


Transaction objects
-------------------


Engine
------
