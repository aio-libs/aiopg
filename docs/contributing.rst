Instruction for contributors
===============================

Developer invironment
----------------------------

At first please clone git repo::

   $ git clone git@github.com:aio-libs/aiopg.git
   $ cd aiopg

After that you need to create and activate virtual environment.  I
guess to use :term:`virtualenvwrapper` but just :term:`virtualenv` or
:term:`venv` also ok::

   $ mkvirtualenv aiopg -p `which python3`

As for regular installation you need for :term:`libpq` library::

   $ sudo apt-get install libpq-dev

In virtual environment you have to install *aiopg* itself and
development tools (the second ones are needed for test suite run etc)::

   $ pip install -Ue .
   $ pip install -U requirements.txt

At the least you should create user and database for test suite::

    $ sudo -u postgres psql

    # CREATE DATABASE aiopg;
    # CREATE USER aiopg WITH PASSWORD 'passwd';
    # GRANT ALL PRIVILEGES ON DATABASE aiopg TO aiopg;

    # CREATE EXTENSION hstore;


That's all.

For running *aiopg* unittests please type::

   $ make test

The command runs :term:`pep8` and :term:`pyflakes` first and executes
*aiopg* tests after style check.


When you are working on solving some issue you probably want to run
some specific test, not the whole suite::

   $ python runtests.py test_initial_empty

For debug sessions I prefer to use :term:`ipdb`, just insert that
lines into your code in place where you want to catch execution process::

   import ipdb; ipdb.set_trace()

The library is covered by tests good enough. To run coverage report please use::

   $ make cov


Contribution
-------------

I like to get well-formed Pull Requiest on github_.

With code fix and tests.

But if you cannot make good test yourself but want to report about
some problem -- please make an issue at
https://github.com/aio-libs/aiopg/issues




.. _github: https://github.com/
