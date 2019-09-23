.. _glossary:


========
Glossary
========

.. if you add new entries, keep the alphabetical sorting!

.. glossary::

   DBAPI

      :pep:`249` -- Python Database API Specification v2.0

   ipdb

      ipdb exports functions to access the IPython debugger, which
      features tab completion, syntax highlighting, better tracebacks,
      better introspection with the same interface as the pdb module.

   libpq

      The standard C library to communicate with :term:`PostgreSQL` server.

      http://www.postgresql.org/docs/9.3/interactive/libpq.html

   pep8

      Python style guide checker

      *pep8* is a tool to check your Python code against some of the
      style conventions in :pep:`8` -- Style Guide for Python Code.

   PostgreSQL

      A popular database server.

      http://www.postgresql.org/

   PostgreSQL Error Codes

       All messages emitted by the PostgreSQL server are assigned
       five-character error codes that follow the
       SQL standard's conventions for “SQLSTATE” codes.

       https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE

   psycopg2-binary

      Psycopg is the most popular PostgreSQL database adapter for
      the Python programming language.
      Its main features are the complete implementation of
      the Python DB API 2.0 specification and the thread safety
      (several threads can share the same connection).

      https://pypi.org/project/psycopg2-binary/

   pyflakes

      passive checker of Python programs

      A simple program which checks Python source files for errors.

      Pyflakes analyzes programs and detects various errors. It works
      by parsing the source file, not importing it, so it is safe to
      use on modules with side effects. It's also much faster.

      https://pypi.python.org/pypi/pyflakes

   sqlalchemy

      The Python SQL Toolkit and Object Relational Mapper.

      http://www.sqlalchemy.org/

   venv

      standard python module for creating lightweight “virtual
      environments” with their own site directories, optionally
      isolated from system site directories. Each virtual environment
      has its own Python binary (allowing creation of environments
      with various Python versions) and can have its own independent
      set of installed Python packages in its site directories.

      https://docs.python.org/dev/library/venv.html

   virtualenv

      The tool to create isolated Python environments. It's not
      included into python stdlib's but it is very popular instrument.

      :term:`venv` and :term:`virtualenv` does almost the same, *venv*
      has been developed after the *virtualenv*.

      https://virtualenv.readthedocs.io/en/latest/

   virtualenvwrapper

      virtualenvwrapper is a set of extensions to Ian Bicking’s
      :term:`virtualenv` tool. The extensions include wrappers for
      creating and deleting virtual environments and otherwise
      managing your development workflow, making it easier to work on
      more than one project at a time without introducing conflicts in
      their dependencies.

      virtualenvwrapper is my choice, highly recommend the tool to everyone.

      https://virtualenvwrapper.readthedocs.io/en/latest/
