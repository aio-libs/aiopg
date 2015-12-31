.. _aiopg-examples:

========================
Examples of aiopg usage
========================

Below is a list of examples from `aiopg/examples
<https://github.com/aio-libs/aiopg/tree/master/examples>`_

Every example is a correct tiny python program.

.. _aiopg-examples-new-style:

async/await style
=================

.. _aiopg-examples-simple:

Low-level API
-------------

.. literalinclude:: ../examples/simple.py


.. _aiopg-examples-notify:

Usage of LISTEN/NOTIFY commands
-------------------------------

.. literalinclude:: ../examples/notify.py


.. _aiopg-examples-sa-simple:

Simple sqlalchemy usage
-----------------------

.. literalinclude:: ../examples/simple_sa.py


.. _aiopg-examples-sa-complex:

Complex sqlalchemy queries
---------------------------

.. literalinclude:: ../examples/sa.py


.. _aiopg-examples-old-style:

yield from/@coroutine style
============================

.. _aiopg-examples-simple-old-style:

Old style Low-level API
-----------------------

.. literalinclude:: ../examples/simple_old_style.py


.. _aiopg-examples-notify-old-style:

Usage of LISTEN/NOTIFY commands using old-style API
-------------------------------------------------------

.. literalinclude:: ../examples/notify_old_style.py
