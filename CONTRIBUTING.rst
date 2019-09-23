Instruction for contributors
============================

Developer environment
---------------------

First clone the git repo:

.. code-block:: shell

    $ git clone git@github.com:aio-libs/aiopg.git
    $ cd aiopg

After that you need to create and activate a virtual environment.  I
recommend using :term:`virtualenvwrapper` but just :term:`virtualenv` or
:term:`venv` will also work. For :term:`virtualenvwrapper`:

.. code-block:: shell

    $ mkvirtualenv aiopg -p `which python3`

For `venv` (for example; put the directory wherever you want):

.. code-block:: shell

    $ python3 -m venv ../venv_directory
    $ source ../venv_directory/bin/activate

Just as when doing a normal install, you need the :term:`libpq` library:

.. code-block:: shell

    $ sudo apt-get install libpq-dev

**UPD**

The latest ``aiopg`` test suite uses docker container for running
Postgres server. See
https://docs.docker.com/engine/installation/linux/ubuntulinux/ for
instructions for Docker installing.

No local Postgres server needed.

In the virtual environment you need to install *aiopg* itself and some
additional development tools (the development tools are needed for running
the test suite and other development tasks)

.. code-block:: shell

    $ pip install -Ue .
    $ pip install -Ur requirements.txt

That's all.

To run all of the *aiopg* tests do:

.. code-block:: shell

    $ make test

This command runs :term:`pep8` and :term:`pyflakes` first and then executes
the *aiopg* unit tests.


When you are working on solving an issue you will probably want to run
some specific test, not the whole suite:

.. code-block:: shell

    $ py.test -s -k test_initial_empty

For debug sessions I prefer to use :term:`ipdb`, which is installed
as part of the development tools. Insert the following line into your
code in the place where you want to start interactively debugging the
execution process:

.. code-block:: py3

    import ipdb; ipdb.set_trace()

The library is reasonably well covered by tests.  There is a make
target for generating the coverage report:

.. code-block:: shell

    $ make cov


Contribution
------------

I like to get well-formed Pull Requests on github_.  The pull request
should include both the code fix and tests for the bug.

If you cannot make a good test yourself or want to report a problem,
please open an issue at https://github.com/aio-libs/aiopg/issues.



.. _github: https://github.com/
