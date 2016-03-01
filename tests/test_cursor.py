import asyncio
import psycopg2
import psycopg2.tz
import pytest
import time

from aiopg.connection import TIMEOUT


@pytest.fixture
def connect(make_connection):

    @asyncio.coroutine
    def go(**kwargs):
        conn = yield from make_connection(**kwargs)
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("CREATE TABLE tbl (id int, name varchar(255))")
        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            yield from cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
        yield from cur.execute("DROP TABLE IF EXISTS tbl2")
        yield from cur.execute("""CREATE TABLE tbl2
                                  (id int, name varchar(255))
                                  WITH OIDS""")
        yield from cur.execute("DROP FUNCTION IF EXISTS inc(val integer)")
        yield from cur.execute("""CREATE FUNCTION inc(val integer)
                                  RETURNS integer AS $$
                                  BEGIN
                                  RETURN val + 1;
                                  END; $$
                                  LANGUAGE PLPGSQL;""")
        return conn

    return go


@pytest.mark.run_loop
def test_description(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert cur.description is None
    yield from cur.execute('SELECT * from tbl;')

    assert len(cur.description) == 2, \
        'cursor.description describes too many columns'

    assert len(cur.description[0]) == 7, \
        'cursor.description[x] tuples must have 7 elements'

    assert cur.description[0][0].lower() == 'id', \
        'cursor.description[x][0] must return column name'

    assert cur.description[1][0].lower() == 'name', \
        'cursor.description[x][0] must return column name'

    # Make sure self.description gets reset, cursor should be
    # set to None in case of none resulting queries like DDL
    yield from cur.execute('DROP TABLE IF EXISTS foobar;')
    assert cur.description is None


@pytest.mark.run_loop
def test_raw(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert cur._impl is cur.raw


@pytest.mark.run_loop
def test_close(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    cur.close()
    assert cur.closed
    with pytest.raises(psycopg2.InterfaceError):
        yield from cur.execute('SELECT 1')


@pytest.mark.run_loop
def test_close_twice(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    cur.close()
    cur.close()
    assert cur.closed
    with pytest.raises(psycopg2.InterfaceError):
        yield from cur.execute('SELECT 1')
    assert conn._waiter is None


@pytest.mark.run_loop
def test_connection(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert cur.connection is conn


@pytest.mark.run_loop
def test_name(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert cur.name is None


@pytest.mark.run_loop
def test_scrollable(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert cur.scrollable is None
    with pytest.raises(psycopg2.ProgrammingError):
        cur.scrollable = True


@pytest.mark.run_loop
def test_withhold(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert not cur.withhold
    with pytest.raises(psycopg2.ProgrammingError):
        cur.withhold = True
    assert not cur.withhold


@pytest.mark.run_loop
def test_execute(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1')
    ret = yield from cur.fetchone()
    assert (1,) == ret


@pytest.mark.run_loop
def test_executemany(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    with pytest.raises(psycopg2.ProgrammingError):
        yield from cur.executemany('SELECT %s', ['1', '2'])


@pytest.mark.run_loop
def test_mogrify(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    ret = yield from cur.mogrify('SELECT %s', ['1'])
    assert b"SELECT '1'" == ret


@pytest.mark.run_loop
def test_setinputsizes(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.setinputsizes(10)


@pytest.mark.run_loop
def test_fetchmany(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT * from tbl;')
    ret = yield from cur.fetchmany()
    assert [(1, 'a')] == ret

    yield from cur.execute('SELECT * from tbl;')
    ret = yield from cur.fetchmany(2)
    assert [(1, 'a'), (2, 'b')] == ret


@pytest.mark.run_loop
def test_fetchall(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT * from tbl;')
    ret = yield from cur.fetchall()
    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


@pytest.mark.run_loop
def test_scroll(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT * from tbl;')
    yield from cur.scroll(1)
    ret = yield from cur.fetchone()
    assert (2, 'b') == ret


@pytest.mark.run_loop
def test_arraysize(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert 1 == cur.arraysize

    cur.arraysize = 10
    assert 10 == cur.arraysize


@pytest.mark.run_loop
def test_itersize(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert 2000 == cur.itersize

    cur.itersize = 10
    assert 10 == cur.itersize


@pytest.mark.run_loop
def test_rows(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT * from tbl')
    assert 3 == cur.rowcount
    assert 0 == cur.rownumber
    yield from cur.fetchone()
    assert 1 == cur.rownumber

    assert 0 == cur.lastrowid
    yield from cur.execute('INSERT INTO tbl2 VALUES (%s, %s)',
                           (4, 'd'))
    assert 0 != cur.lastrowid


@pytest.mark.run_loop
def test_query(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1')
    assert b'SELECT 1' == cur.query


@pytest.mark.run_loop
def test_statusmessage(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute('SELECT 1')
    assert 'SELECT 1' == cur.statusmessage


@pytest.mark.run_loop
def test_tzinfo_factory(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert psycopg2.tz.FixedOffsetTimezone is cur.tzinfo_factory

    cur.tzinfo_factory = psycopg2.tz.LocalTimezone
    assert psycopg2.tz.LocalTimezone is cur.tzinfo_factory


@pytest.mark.run_loop
def test_nextset(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    with pytest.raises(psycopg2.NotSupportedError):
        yield from cur.nextset()


@pytest.mark.run_loop
def test_setoutputsize(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.setoutputsize(4, 1)


@pytest.mark.run_loop
def test_copy_family(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()

    with pytest.raises(psycopg2.ProgrammingError):
        yield from cur.copy_from('file', 'table')

    with pytest.raises(psycopg2.ProgrammingError):
        yield from cur.copy_to('file', 'table')

    with pytest.raises(psycopg2.ProgrammingError):
        yield from cur.copy_expert('sql', 'table')


@pytest.mark.run_loop
def test_callproc(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.callproc('inc', [1])
    ret = yield from cur.fetchone()
    assert (2,) == ret

    cur.close()
    with pytest.raises(psycopg2.InterfaceError):
        yield from cur.callproc('inc', [1])
    assert conn._waiter is None


@pytest.mark.run_loop
def test_execute_timeout(connect):
    timeout = 0.1
    conn = yield from connect()
    cur = yield from conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


@pytest.mark.run_loop
def test_execute_override_timeout(connect):
    timeout = 0.1
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert TIMEOUT == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.execute("SELECT pg_sleep(1)", timeout=timeout)
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


@pytest.mark.run_loop
def test_callproc_timeout(connect):
    timeout = 0.1
    conn = yield from connect()
    cur = yield from conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.callproc("pg_sleep", [1])
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


@pytest.mark.run_loop
def test_callproc_override_timeout(connect):
    timeout = 0.1
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert TIMEOUT == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        yield from cur.callproc("pg_sleep", [1], timeout=timeout)
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


@pytest.mark.run_loop
def test_echo(connect):
    conn = yield from connect(echo=True)
    cur = yield from conn.cursor()
    assert cur.echo


@pytest.mark.run_loop
def test_echo_false(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    assert not cur.echo


@pytest.mark.run_loop
def test_iter(connect):
    conn = yield from connect()
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT * FROM tbl")
    data = [(1, 'a'), (2, 'b'), (3, 'c')]
    for item, tst in zip(cur, data):
        assert item == tst


@pytest.mark.run_loop
def test_echo_callproc(connect):
    conn = yield from connect(echo=True)
    cur = yield from conn.cursor()

    # TODO: check log records
    yield from cur.callproc('inc', [1])
    ret = yield from cur.fetchone()
    assert (2,) == ret
    cur.close()
