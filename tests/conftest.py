import asyncio
import collections
import gc
import logging
import pytest
import re
import socket
import sys
import warnings


import aiopg
from aiopg import sa


@pytest.fixture
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return f


@pytest.yield_fixture
def loop(request):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(pyfuncitem.obj(**testargs))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


def pytest_ignore_collect(path, config):
    if 'pep492' in str(path):
        if sys.version_info < (3, 5, 0):
            return True


@pytest.fixture
def pg_params():
    return dict(database='aiopg',
                user='aiopg',
                password='passwd',
                host='127.0.0.1')


@pytest.yield_fixture()
def make_connection(loop):

    conn = None

    @asyncio.coroutine
    def go(*, no_loop=False, **kwargs):
        nonlocal conn
        useloop = None if no_loop else loop
        conn = yield from aiopg.connect(database='aiopg',
                                        user='aiopg',
                                        password='passwd',
                                        host='127.0.0.1',
                                        loop=useloop,
                                        **kwargs)
        conn2 = yield from aiopg.connect(database='aiopg',
                                         user='aiopg',
                                         password='passwd',
                                         host='127.0.0.1',
                                         loop=useloop)
        cur = yield from conn2.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS foo")
        yield from conn2.close()
        return conn

    yield go

    if conn is not None:
        loop.run_until_complete(conn.close())


@pytest.yield_fixture()
def create_pool(loop):
    pool = None

    @asyncio.coroutine
    def go(*, no_loop=False, **kwargs):
        nonlocal pool
        useloop = None if no_loop else loop
        pool = yield from aiopg.create_pool(database='aiopg',
                                            user='aiopg',
                                            password='passwd',
                                            host='127.0.0.1',
                                            loop=useloop,
                                            **kwargs)
        return pool

    yield go

    if pool is not None:
        pool.terminate()


@pytest.yield_fixture
def make_engine(loop):
    engine = None

    @asyncio.coroutine
    def go(*, use_loop=True, **kwargs):
        if use_loop:
            engine = (yield from sa.create_engine(database='aiopg',
                                                  user='aiopg',
                                                  password='passwd',
                                                  host='127.0.0.1',
                                                  loop=loop,
                                                  **kwargs))
        else:
            engine = (yield from sa.create_engine(database='aiopg',
                                                  user='aiopg',
                                                  password='passwd',
                                                  host='127.0.0.1',
                                                  **kwargs))
        return engine

    yield go

    if engine is not None:
        engine.close()
        loop.run_until_complete(engine.wait_closed())


@pytest.yield_fixture()
def make_sa_connection(make_engine):
    conn = None
    engine = None

    @asyncio.coroutine
    def go(*, use_loop=True, **kwargs):
        nonlocal conn, engine
        engine = yield from make_engine(use_loop=use_loop, **kwargs)
        conn = yield from engine.acquire()
        return conn

    yield go

    if conn is not None:
        engine.release(conn)


class _AssertWarnsContext:
    """A context manager used to implement TestCase.assertWarns* methods."""

    def __init__(self, expected, expected_regex=None):
        self.expected = expected
        if expected_regex is not None:
            expected_regex = re.compile(expected_regex)
        self.expected_regex = expected_regex
        self.obj_name = None

    def __enter__(self):
        # The __warningregistry__'s need to be in a pristine state for tests
        # to work properly.
        for v in sys.modules.values():
            if getattr(v, '__warningregistry__', None):
                v.__warningregistry__ = {}
        self.warnings_manager = warnings.catch_warnings(record=True)
        self.warnings = self.warnings_manager.__enter__()
        warnings.simplefilter("always", self.expected)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.warnings_manager.__exit__(exc_type, exc_value, tb)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return
        try:
            exc_name = self.expected.__name__
        except AttributeError:
            exc_name = str(self.expected)
        first_matching = None
        for m in self.warnings:
            w = m.message
            if not isinstance(w, self.expected):
                continue
            if first_matching is None:
                first_matching = w
            if (self.expected_regex is not None and
                    not self.expected_regex.search(str(w))):
                continue
            # store warning for later retrieval
            self.warning = w
            self.filename = m.filename
            self.lineno = m.lineno
            return
        # Now we simply try to choose a helpful failure message
        if first_matching is not None:
            __tracebackhide__ = True
            assert 0, '"{}" does not match "{}"'.format(
                self.expected_regex.pattern, str(first_matching))
        if self.obj_name:
            __tracebackhide__ = True
            assert 0, "{} not triggered by {}".format(exc_name,
                                                      self.obj_name)
        else:
            __tracebackhide__ = True
            assert 0, "{} not triggered".format(exc_name)


_LoggingWatcher = collections.namedtuple("_LoggingWatcher",
                                         ["records", "output"])


class _CapturingHandler(logging.Handler):
    """
    A logging handler capturing all (raw and formatted) logging output.
    """

    def __init__(self):
        logging.Handler.__init__(self)
        self.watcher = _LoggingWatcher([], [])

    def flush(self):
        pass

    def emit(self, record):
        self.watcher.records.append(record)
        msg = self.format(record)
        self.watcher.output.append(msg)


class _AssertLogsContext:
    """A context manager used to implement TestCase.assertLogs()."""

    LOGGING_FORMAT = "%(levelname)s:%(name)s:%(message)s"

    def __init__(self, logger_name=None, level=None):
        self.logger_name = logger_name
        if level:
            self.level = logging._nameToLevel.get(level, level)
        else:
            self.level = logging.INFO
        self.msg = None

    def __enter__(self):
        if isinstance(self.logger_name, logging.Logger):
            logger = self.logger = self.logger_name
        else:
            logger = self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter(self.LOGGING_FORMAT)
        handler = _CapturingHandler()
        handler.setFormatter(formatter)
        self.watcher = handler.watcher
        self.old_handlers = logger.handlers[:]
        self.old_level = logger.level
        self.old_propagate = logger.propagate
        logger.handlers = [handler]
        logger.setLevel(self.level)
        logger.propagate = False
        return handler.watcher

    def __exit__(self, exc_type, exc_value, tb):
        self.logger.handlers = self.old_handlers
        self.logger.propagate = self.old_propagate
        self.logger.setLevel(self.old_level)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return False
        if len(self.watcher.records) == 0:
            __tracebackhide__ = True
            assert 0, ("no logs of level {} or higher triggered on {}"
                       .format(logging.getLevelName(self.level),
                               self.logger.name))


@pytest.yield_fixture
def warning():
    yield _AssertWarnsContext


@pytest.yield_fixture
def log():
    yield _AssertLogsContext
