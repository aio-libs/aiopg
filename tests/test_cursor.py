import asyncio
import time

import psycopg2
import psycopg2.tz
import pytest
from aiopg.connection import TIMEOUT


@pytest.fixture
def connect(make_connection):

    async def go(**kwargs):
        conn = await make_connection(**kwargs)
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS tbl")
        await cur.execute("CREATE TABLE tbl (id int, name varchar(255))")
        for i in [(1, 'a'), (2, 'b'), (3, 'c')]:
            await cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
        await cur.execute("DROP TABLE IF EXISTS tbl2")
        await cur.execute("""CREATE TABLE tbl2
                                  (id int, name varchar(255))
                                  WITH OIDS""")
        await cur.execute("DROP FUNCTION IF EXISTS inc(val integer)")
        await cur.execute("""CREATE FUNCTION inc(val integer)
                                  RETURNS integer AS $$
                                  BEGIN
                                  RETURN val + 1;
                                  END; $$
                                  LANGUAGE PLPGSQL;""")
        return conn

    return go


async def test_description(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur.description is None
    await cur.execute('SELECT * from tbl;')

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
    await cur.execute('DROP TABLE IF EXISTS foobar;')
    assert cur.description is None


async def test_raw(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur._impl is cur.raw


async def test_close(connect):
    conn = await connect()
    cur = await conn.cursor()
    cur.close()
    assert cur.closed
    with pytest.raises(psycopg2.InterfaceError):
        await cur.execute('SELECT 1')


async def test_close_twice(connect):
    conn = await connect()
    cur = await conn.cursor()
    cur.close()
    cur.close()
    assert cur.closed
    with pytest.raises(psycopg2.InterfaceError):
        await cur.execute('SELECT 1')
    assert conn._waiter is None


async def test_connection(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur.connection is conn


async def test_name(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur.name is None


async def test_scrollable(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur.scrollable is None
    with pytest.raises(psycopg2.ProgrammingError):
        cur.scrollable = True


async def test_withhold(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert not cur.withhold
    with pytest.raises(psycopg2.ProgrammingError):
        cur.withhold = True
    assert not cur.withhold


async def test_execute(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT 1')
    ret = await cur.fetchone()
    assert (1,) == ret


async def test_executemany(connect):
    conn = await connect()
    cur = await conn.cursor()
    with pytest.raises(psycopg2.ProgrammingError):
        await cur.executemany('SELECT %s', ['1', '2'])


async def test_mogrify(connect):
    conn = await connect()
    cur = await conn.cursor()
    ret = await cur.mogrify('SELECT %s', ['1'])
    assert b"SELECT '1'" == ret


async def test_setinputsizes(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.setinputsizes(10)


async def test_fetchmany(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT * from tbl;')
    ret = await cur.fetchmany()
    assert [(1, 'a')] == ret

    await cur.execute('SELECT * from tbl;')
    ret = await cur.fetchmany(2)
    assert [(1, 'a'), (2, 'b')] == ret


async def test_fetchall(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT * from tbl;')
    ret = await cur.fetchall()
    assert [(1, 'a'), (2, 'b'), (3, 'c')] == ret


async def test_scroll(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT * from tbl;')
    await cur.scroll(1)
    ret = await cur.fetchone()
    assert (2, 'b') == ret


async def test_arraysize(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert 1 == cur.arraysize

    cur.arraysize = 10
    assert 10 == cur.arraysize


async def test_itersize(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert 2000 == cur.itersize

    cur.itersize = 10
    assert 10 == cur.itersize


async def test_rows(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT * from tbl')
    assert 3 == cur.rowcount
    assert 0 == cur.rownumber
    await cur.fetchone()
    assert 1 == cur.rownumber

    assert 0 == cur.lastrowid
    await cur.execute('INSERT INTO tbl2 VALUES (%s, %s)',
                      (4, 'd'))
    assert 0 != cur.lastrowid


async def test_query(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT 1')
    assert b'SELECT 1' == cur.query


async def test_statusmessage(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute('SELECT 1')
    assert 'SELECT 1' == cur.statusmessage


async def test_tzinfo_factory(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert psycopg2.tz.FixedOffsetTimezone is cur.tzinfo_factory

    cur.tzinfo_factory = psycopg2.tz.LocalTimezone
    assert psycopg2.tz.LocalTimezone is cur.tzinfo_factory


async def test_nextset(connect):
    conn = await connect()
    cur = await conn.cursor()
    with pytest.raises(psycopg2.NotSupportedError):
        await cur.nextset()


async def test_setoutputsize(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.setoutputsize(4, 1)


async def test_copy_family(connect):
    conn = await connect()
    cur = await conn.cursor()

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_from('file', 'table')

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_to('file', 'table')

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_expert('sql', 'table')


async def test_callproc(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.callproc('inc', [1])
    ret = await cur.fetchone()
    assert (2,) == ret

    cur.close()
    with pytest.raises(psycopg2.InterfaceError):
        await cur.callproc('inc', [1])
    assert conn._waiter is None


async def test_execute_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_execute_override_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor()
    assert TIMEOUT == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(1)", timeout=timeout)
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_callproc_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.callproc("pg_sleep", [1])
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_callproc_override_timeout(connect):
    timeout = 0.1
    conn = await connect()
    cur = await conn.cursor()
    assert TIMEOUT == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.callproc("pg_sleep", [1], timeout=timeout)
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_echo(connect):
    conn = await connect(echo=True)
    cur = await conn.cursor()
    assert cur.echo


async def test_echo_false(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert not cur.echo


async def test_iter(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM tbl")
    data = [(1, 'a'), (2, 'b'), (3, 'c')]
    for item, tst in zip(cur, data):
        assert item == tst


async def test_echo_callproc(connect):
    conn = await connect(echo=True)
    cur = await conn.cursor()

    # TODO: check log records
    await cur.callproc('inc', [1])
    ret = await cur.fetchone()
    assert (2,) == ret
    cur.close()
