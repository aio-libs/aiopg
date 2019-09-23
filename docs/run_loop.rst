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


Implementation
--------------

For the version below ``python3.7`` we added this implementation.

.. code-block:: py3

    if sys.version_info >= (3, 7, 0):
        __get_running_loop = asyncio.get_running_loop
    else:
        def __get_running_loop() -> asyncio.AbstractEventLoop:
            loop = asyncio.get_event_loop()
            if not loop.is_running():
                raise RuntimeError('no running event loop')
            return loop

This allows you to get a loop :class:`asyncio.AbstractEventLoop` correctly
and causes :exc:`DeprecationWarning` if you explicitly
pass a loop :class:`asyncio.AbstractEventLoop` for class or method:

    * :class:`aiopg.Pool` and :meth:`aiopg.create_pool`
    * :class:`aiopg.Connection` and :meth:`aiopg.connect`
    * :meth:`aiopg.sa.create_engine`

.. code-block:: py3

    def get_running_loop(is_warn=False):
        loop = __get_running_loop()

        if is_warn:
            warnings.warn(
                'aiopg always uses "aiopg.get_running_loop", '
                'look the documentation.',
                DeprecationWarning,
                stacklevel=3
            )

            if loop.get_debug():
                logger.warning(
                    'aiopg always uses "aiopg.get_running_loop", '
                    'look the documentation.',
                    exc_info=True
                )

        return loop
