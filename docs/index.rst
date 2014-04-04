.. aiopg documentation master file, created by
   sphinx-quickstart on Sat Apr  5 00:00:44 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aiopg
=================================

The library is an adoptation of psycopg2 for asyncio.

It uses psycopg2 connections in **asynchronous** mode internally.

Literally it is an (almost) transparent wrapper for psycopg
connection and cursor, but with only exception.

You should to use ``yield from conn.f()`` instead of just call ``conn.f()`` for
every method.

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.

For documentation about connection and cursor methods/properties
please go to psycopg docs: http://initd.org/psycopg/docs/

The last part describes the **differences** from psycopg.

Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

