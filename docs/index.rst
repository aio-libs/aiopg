.. aiopg documentation master file, created by
   sphinx-quickstart on Sat Apr  5 00:00:44 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aiopg
=================================

.. _GitHub: https://github.com/aio-libs/aiopg


The library is an adoptation of :mod:`psycopg2` for :mod:`asyncio`.

It uses :mod:`psycopg2` connections in **asynchronous** mode internally.

Literally it is an (almost) transparent wrapper for psycopg2
connection and cursor, but with only exception.

You should to use ``yield from conn.f()`` instead of just call ``conn.f()`` for
every method.

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.

For documentation about connection and cursor methods/properties
please go to psycopg docs: http://initd.org/psycopg/docs/

.. note:: psycopg2 creates new connections with ``autocommit=True``
          option in asynchronous mode. Autocommitting cannot be disabled.


Installation
--------------------

   pip3 install aiozmq

Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aiopg/issues>`_ if you have found a bug
or have some suggestion for library improvement.

The library uses `Travis <https://travis-ci.org/aio-libs/aiopg>`_ for
Continious Integration.

Dependencies
------------

- Python 3.3 and :mod:`asyncio` or Python 3.4+
- psycopg2

Authors and License
-------------------

The ``aiozmq`` package is initially written by Nikolay Kim, now
maintained by Andrew Svetlov.  It's BSD licensed and freely available.
Feel free to improve this package and send a pull request to GitHub_.


Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
