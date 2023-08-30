.. _aiopg-run-loop:

Only use get_running_loop
=========================

Rationale
---------

:func:`asyncio.get_event_loop()` returns the
running loop :class:`asyncio.AbstractEventLoop` instead of **default**,
which may be different, e.g.

.. code-block:: py3

    loop = asyncio.new_event_loop()
    loop.run_until_complete(f())

.. note::

    :func:`asyncio.set_event_loop` was not called and default
    loop :class:`asyncio.AbstractEventLoop`
    is not equal to actually executed one.
