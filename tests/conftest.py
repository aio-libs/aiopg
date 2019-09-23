import asyncio
import collections
import contextlib
import gc
import logging
import re
import socket
import sys
import time
import uuid
import warnings

import psycopg2
import pytest
from docker import APIClient

import aiopg
from aiopg import sa

warnings.filterwarnings(
    'error', '.*',
    category=ResourceWarning,
    module=r'aiopg(\.\w+)+',
    append=False
)


@pytest.fixture(scope='session')
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]

    return f


@pytest.fixture
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
    if collector.funcnamefilter(name) and asyncio.iscoroutinefunction(obj):
        return list(collector._genfunctions(name, obj))


@contextlib.contextmanager
def _passthrough_loop_context(loop):
    if loop:
        # loop already exists, pass it straight through
        yield loop
    else:
        # this shadows loop_context's standard behavior
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        yield loop
        closed = loop.is_closed()
        if not closed:
            loop.call_soon(loop.stop)
            loop.run_forever()
            loop.close()
            gc.collect()
        asyncio.set_event_loop(None)


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if asyncio.iscoroutinefunction(pyfuncitem.function):
        existing_loop = pyfuncitem.funcargs.get('loop', None)
        with _passthrough_loop_context(existing_loop) as _loop:
            testargs = {arg: pyfuncitem.funcargs[arg]
                        for arg in pyfuncitem._fixtureinfo.argnames}

            task = _loop.create_task(pyfuncitem.obj(**testargs))
            _loop.run_until_complete(task)

        return True


@pytest.fixture(scope='session')
def session_id():
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    return APIClient(version='auto')


def pytest_addoption(parser):
    parser.addoption("--pg_tag", action="append", default=[],
                     help=("Postgres server versions. "
                           "May be used several times. "
                           "Available values: 9.3, 9.4, 9.5, all"))
    parser.addoption("--no-pull", action="store_true", default=False,
                     help="Don't perform docker images pulling")


def pytest_generate_tests(metafunc):
    if 'pg_tag' in metafunc.fixturenames:
        tags = set(metafunc.config.option.pg_tag)
        if not tags:
            tags = ['9.5']
        elif 'all' in tags:
            tags = ['9.3', '9.4', '9.5']
        else:
            tags = list(tags)
        metafunc.parametrize("pg_tag", tags, scope='session')


@pytest.fixture(scope='session')
def pg_server(unused_port, docker, session_id, pg_tag, request):
    if not request.config.option.no_pull:
        docker.pull('postgres:{}'.format(pg_tag))

    container_args = dict(
        image='postgres:{}'.format(pg_tag),
        name='aiopg-test-server-{}-{}'.format(pg_tag, session_id),
        ports=[5432],
        detach=True,
    )

    # bound IPs do not work on OSX
    host = "127.0.0.1"
    host_port = unused_port()
    container_args['host_config'] = docker.create_host_config(
        port_bindings={5432: (host, host_port)})

    container = docker.create_container(**container_args)

    try:
        docker.start(container=container['Id'])
        server_params = dict(database='postgres',
                             user='postgres',
                             password='mysecretpassword',
                             host=host,
                             port=host_port)
        delay = 0.001
        for i in range(100):
            try:
                conn = psycopg2.connect(**server_params)
                cur = conn.cursor()
                cur.execute("CREATE EXTENSION hstore;")
                cur.close()
                conn.close()
                break
            except psycopg2.Error:
                time.sleep(delay)
                delay *= 2
        else:
            pytest.fail("Cannot start postgres server")

        container['host'] = host
        container['port'] = host_port
        container['pg_params'] = server_params

        yield container
    finally:
        docker.kill(container=container['Id'])
        docker.remove_container(container['Id'])


@pytest.fixture
def pg_params(pg_server):
    return dict(**pg_server['pg_params'])


@pytest.fixture
def make_connection(loop, pg_params):
    conns = []

    async def go(**kwargs):
        nonlocal conn
        params = pg_params.copy()
        params.update(kwargs)
        conn = await aiopg.connect(**params)
        conn2 = await aiopg.connect(**params)
        cur = await conn2.cursor()
        await cur.execute("DROP TABLE IF EXISTS foo")
        await conn2.close()
        conns.append(conn)
        return conn

    yield go

    for conn in conns:
        loop.run_until_complete(conn.close())


@pytest.fixture
def create_pool(pg_params, loop):
    pool = None

    async def go(**kwargs):
        nonlocal pool
        params = pg_params.copy()
        params.update(kwargs)
        pool = await aiopg.create_pool(**params)
        return pool

    yield go

    if pool is not None:
        pool.terminate()
        loop.run_until_complete(pool.wait_closed())


@pytest.fixture
def make_engine(loop, pg_params):
    engine = None

    async def go(**kwargs):
        nonlocal engine
        pg_params.update(kwargs)
        engine = await sa.create_engine(**pg_params)
        return engine

    yield go

    if engine is not None:
        engine.close()
        loop.run_until_complete(engine.wait_closed())


@pytest.fixture
def make_sa_connection(make_engine):
    conn = None
    engine = None

    async def go(**kwargs):
        nonlocal conn, engine
        engine = await make_engine(**kwargs)
        conn = await engine.acquire()
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


@pytest.fixture
def warning():
    yield _AssertWarnsContext


@pytest.fixture
def log():
    yield _AssertLogsContext
