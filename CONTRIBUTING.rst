Instruction for contributors
============================

Developer environment
---------------------

First clone the git repo::

   $ git clone git@github.com:aio-libs/aiopg.git
   $ cd aiopg

After that you need to create and activate a virtual environment.  I
recommend using :term:`virtualenvwrapper` but just :term:`virtualenv` or
:term:`venv` will also work.  For ``virtualenvwrapper``::

   $ mkvirtualenv aiopg -p `which python3`

For ``venv`` (for example; put the directory wherever you want)::

   $ python3 -m venv ../venv_directory
   $ source ../venv_directory/bin/activate

Just as when doing a normal install, you need the :term:`libpq` library::

   $ sudo apt-get install libpq-dev

In the virtual environment you need to install *aiopg* itself and some
additional development tools (the development tools are needed for running
the test suite and other development tasks)::

   $ pip install -Ue .
   $ pip install -Ur requirements.txt

You will also need to create a postgres user and database for the test suite::

    $ sudo -u postgres psql

    # CREATE DATABASE aiopg;
    # CREATE USER aiopg WITH PASSWORD 'passwd';
    # GRANT ALL PRIVILEGES ON DATABASE aiopg TO aiopg;

    # \connect aiopg
    # CREATE EXTENSION hstore;

You can use the ``setup_test_db.sql`` script to do the setup::

    $ sudo -u postgres aiopg <setup_test_db.sql

That's all.

To run all of the *aiopg* tests do::

   $ make test

This command runs :term:`pep8` and :term:`pyflakes` first and then executes
the *aiopg* unit tests.


When you are working on solving an issue you will probably want to run
some specific test, not the whole suite::

   $ python runtests.py test_initial_empty

For debug sessions I prefer to use :term:`ipdb`, which is installed
as part of the development tools.  Insert the following line into your
code in the place where you want to start interactively debugging the
execution process::

   import ipdb; ipdb.set_trace()

The library is reasonably well covered by tests.  There is a make
target for generating the coverage report::

   $ make cov


Contribution
------------

I like to get well-formed Pull Requests on github_.  The pull request
should include both the code fix and tests for the bug.

If you cannot make a good test yourself or want to report a problem,
please open an issue at https://github.com/aio-libs/aiopg/issues.



.. _github: https://github.com/
