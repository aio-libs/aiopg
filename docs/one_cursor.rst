.. _aiopg-one-cursor:

One connection, one cursor or forced close
==========================================

Rationale
---------

The :meth:`aiopg.sa.SAConnection.execute` method creates a new cursor
each time it is called. But since we release the :ref:`aiopg-sa-connection`,
weak references to the :ref:`aiopg-core-cursor` remain, which means that the closing
does not occur.

The code must be restructured so that instead the pool is transferred
so that each execution has its own :ref:`aiopg-core-cursor`.

Example:

.. code-block:: py3

    async with engine.acquire() as conn:
        assert res.cursor.closed is False

        res = await conn.execute(tbl.select())
        assert res.cursor.closed is False

        row = await res.fetchone()
        assert res.cursor.closed is False

    assert res.cursor.closed is False

After exiting `async with` the :ref:`aiopg-sa-connection` was closed,
but the :ref:`aiopg-core-cursor` remained open.

Implementation
--------------

It was decided to select one connection, one :ref:`aiopg-core-cursor`.
For the interface :ref:`aiopg-core-connection` interface
over :term:`psycopg2-binary`,
we added the :meth:`aiopg.Connection.free_cursor`
method to clean the cursor if it is open.

The :attr:`aiopg.Connection.free_cursor` method is called in several places:

    * at the time call method :meth:`aiopg.Connection.cursor`
    * at the time call method :meth:`aiopg.Connection.close`
    * at the time call method :meth:`aiopg.Pool.release`
    * at the time call method :meth:`aiopg.sa.SAConnection.execute`
    * at the time call method :meth:`aiopg.Engine.release`

.. warning::
    At the time call method :meth:`aiopg.Connection.cursor`
    or :meth:`aiopg.sa.SAConnection.execute`,
    if the current :ref:`aiopg-core-connection` have
    a open :ref:`aiopg-core-cursor`, a warning will be issued before closing.

    .. code-block:: py3

        warnings.warn(
            ('You can only have one cursor per connection. '
             'The cursor for connection will be '
             'closed forcibly {!r}.'
            ).format(self),
            ResourceWarning
        )
