import asyncio
import collections
import datetime
import json
import math
import sys
import time
from unittest.mock import Mock, patch

import async_timeout
import psycopg2
import psycopg2.errors
import psycopg2.extras
import psycopg2.tz
import pytest
from flaky import flaky

import aiopg
from aiopg import IsolationLevel, ReadCommittedCompiler, ReplicationMessage
from aiopg.connection import TIMEOUT, _ReplicationCursor

REPLICATION_CONN_FACTORIES = [
    psycopg2.extras.LogicalReplicationConnection,
    psycopg2.extras.PhysicalReplicationConnection,
]

# postgres has expanded pg_replication_slots view's set of columns
# since version 9.6
REPL_SLOT_VIEW_COLUMNS_BASE = [
    "slot_name",
    "plugin",
    "slot_type",
    "datoid",
    "database",
    "active",
    "active_pid",
    "xmin",
    "catalog_xmin",
    "restart_lsn",
    "confirmed_flush_lsn",
]
REPL_SLOT_VIEW_COLUMNS_LT_10 = REPL_SLOT_VIEW_COLUMNS_BASE.copy()
REPL_SLOT_VIEW_COLUMNS_LT_13 = REPL_SLOT_VIEW_COLUMNS_BASE.copy()
REPL_SLOT_VIEW_COLUMNS_LT_13.insert(5, "temporary")
REPL_SLOT_VIEW_COLUMNS_GTE_13 = REPL_SLOT_VIEW_COLUMNS_LT_13.copy()
REPL_SLOT_VIEW_COLUMNS_GTE_13.extend(["wal_status", "safe_wal_size"])


@pytest.fixture
def connect(make_connection):
    async def go(**kwargs):
        conn = await make_connection(**kwargs)
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS tbl")
            await cur.execute("CREATE TABLE tbl (id int, name varchar(255))")
            for i in [(1, "a"), (2, "b"), (3, "c")]:
                await cur.execute("INSERT INTO tbl VALUES(%s, %s)", i)
            await cur.execute("DROP TABLE IF EXISTS tbl2")
            await cur.execute(
                """CREATE TABLE tbl2
                                      (id int, name varchar(255))"""
            )
            await cur.execute("DROP FUNCTION IF EXISTS inc(val integer)")
            await cur.execute(
                """CREATE FUNCTION inc(val integer)
                                      RETURNS integer AS $$
                                      BEGIN
                                      RETURN val + 1;
                                      END; $$
                                      LANGUAGE PLPGSQL;"""
            )
        return conn

    return go


@pytest.fixture
def connect_replication(make_replication_connection):
    async def go(**kwargs):
        output_plugin = kwargs.pop("output_plugin", None)
        conn = await make_replication_connection(**kwargs)
        if isinstance(conn.raw, psycopg2.extras.LogicalReplicationConnection):
            output_plugin = output_plugin or "wal2json"
        else:
            output_plugin = None

        async with conn.cursor() as cur:
            await cur.create_replication_slot(
                slot_name="test_slot",
                output_plugin=output_plugin,
            )

        return conn

    return go


@pytest.fixture
def cursor(connect, loop):
    async def go():
        return await (await connect()).cursor()

    cur = loop.run_until_complete(go())
    yield cur
    cur.close()


@pytest.fixture
def pg_to_psycopg2_lsn():
    # psycopg2's hex LSN representation is always 4 bytes long,
    # while LSNs returned by postgres are variable in length and may omit
    # the leading 0
    def go(pg_lsn):
        lsn = pg_lsn.split("/")
        lsn = f"{int(lsn[0], 16):X}/{int(lsn[1], 16):08X}"
        return lsn

    return go


@pytest.fixture
def lsn_to_decimal():
    def go(lsn):
        return int(lsn.split("/")[1], 16)

    return go


@pytest.fixture
def get_xlogpos(lsn_to_decimal, pg_to_psycopg2_lsn):
    async def go(repl_cur, as_int=True):
        # return server's current xlog flush location
        await repl_cur.execute("IDENTIFY_SYSTEM")
        xlogpos = (await repl_cur.fetchone())[2]
        return (
            lsn_to_decimal(xlogpos) if as_int else pg_to_psycopg2_lsn(xlogpos)
        )

    return go


@pytest.fixture
def repl_slots_view_row(pg_tag):
    # postgres has expanded pg_replication_slots view's set of columns
    # since version 9.6
    if float(pg_tag) < 10:
        row = collections.namedtuple(
            "ReplSlotsViewRow",
            REPL_SLOT_VIEW_COLUMNS_LT_10,
        )
    elif float(pg_tag) < 13:
        row = collections.namedtuple(
            "ReplSlotsViewRow",
            REPL_SLOT_VIEW_COLUMNS_LT_13,
        )
    else:
        row = collections.namedtuple(
            "ReplSlotsViewRow",
            REPL_SLOT_VIEW_COLUMNS_GTE_13,
        )

    return row


@pytest.fixture
def get_repl_slot_details(repl_slots_view_row):
    async def go(cursor, slot_name):
        query = """
        SELECT * FROM pg_replication_slots
        WHERE slot_name = '{slot_name}'
        LIMIT 1;
        """
        await cursor.execute(query.format(slot_name=slot_name))
        raw_row = await cursor.fetchone()
        return repl_slots_view_row(*raw_row) if raw_row is not None else None

    return go


@pytest.fixture
def tbl_insert_wal_segment_size(pg_tag):
    return {
        "9.6": 112,
        "10": 112,
        "11": 112,
        "12": 112,
        "13": 112,
    }.get(pg_tag, 112)


async def test_description(cursor):
    async with cursor as cur:
        assert cur.description is None
        await cur.execute("SELECT * from tbl;")

        assert (
            len(cur.description) == 2
        ), "cursor.description describes too many columns"

        assert (
            len(cur.description[0]) == 7
        ), "cursor.description[x] tuples must have 7 elements"

        assert (
            cur.description[0][0].lower() == "id"
        ), "cursor.description[x][0] must return column name"

        assert (
            cur.description[1][0].lower() == "name"
        ), "cursor.description[x][0] must return column name"

        # Make sure self.description gets reset, cursor should be
        # set to None in case of none resulting queries like DDL
        await cur.execute("DROP TABLE IF EXISTS foobar;")
        assert cur.description is None


async def test_raw(cursor):
    assert cursor._impl is cursor.raw


async def test_close(cursor):
    cursor.close()
    assert cursor.closed
    with pytest.raises(psycopg2.InterfaceError):
        await cursor.execute("SELECT 1")


async def test_close_twice(connect):
    conn = await connect()
    cur = await conn.cursor()
    cur.close()
    cur.close()
    assert cur.closed
    with pytest.raises(psycopg2.InterfaceError):
        await cur.execute("SELECT 1")
    assert conn._waiter is None


async def test_connection(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert cur.connection is conn


async def test_name(cursor):
    assert cursor.name is None


async def test_scrollable(cursor):
    assert cursor.scrollable is None
    with pytest.raises(psycopg2.ProgrammingError):
        cursor.scrollable = True


async def test_withhold(cursor):
    assert not cursor.withhold
    with pytest.raises(psycopg2.ProgrammingError):
        cursor.withhold = True
    assert not cursor.withhold


async def test_execute(cursor):
    await cursor.execute("SELECT 1")
    ret = await cursor.fetchone()
    assert (1,) == ret


async def test_executemany(cursor):
    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.executemany("SELECT %s", ["1", "2"])


def test_mogrify(cursor):
    ret = cursor.mogrify("SELECT %s", ["1"])
    assert b"SELECT '1'" == ret


async def test_setinputsizes(cursor):
    await cursor.setinputsizes(10)


async def test_fetchmany(cursor):
    await cursor.execute("SELECT * from tbl;")
    ret = await cursor.fetchmany()
    assert [(1, "a")] == ret

    await cursor.execute("SELECT * from tbl;")
    ret = await cursor.fetchmany(2)
    assert [(1, "a"), (2, "b")] == ret


async def test_fetchall(cursor):
    await cursor.execute("SELECT * from tbl;")
    ret = await cursor.fetchall()
    assert [(1, "a"), (2, "b"), (3, "c")] == ret


async def test_scroll(cursor):
    await cursor.execute("SELECT * from tbl;")
    await cursor.scroll(1)
    ret = await cursor.fetchone()
    assert (2, "b") == ret


async def test_arraysize(cursor):
    assert 1 == cursor.arraysize

    cursor.arraysize = 10
    assert 10 == cursor.arraysize


async def test_itersize(cursor):
    assert 2000 == cursor.itersize

    cursor.itersize = 10
    assert 10 == cursor.itersize


async def test_rows(cursor):
    await cursor.execute("SELECT * from tbl")
    assert 3 == cursor.rowcount
    assert 0 == cursor.rownumber
    await cursor.fetchone()
    assert 1 == cursor.rownumber


async def test_query(cursor):
    await cursor.execute("SELECT 1")
    assert b"SELECT 1" == cursor.query


async def test_statusmessage(cursor):
    await cursor.execute("SELECT 1")
    assert "SELECT 1" == cursor.statusmessage


async def test_tzinfo_factory(cursor):
    assert datetime.timezone is cursor.tzinfo_factory

    cursor.tzinfo_factory = psycopg2.tz.LocalTimezone
    assert psycopg2.tz.LocalTimezone is cursor.tzinfo_factory


async def test_nextset(cursor):
    with pytest.raises(psycopg2.NotSupportedError):
        await cursor.nextset()


async def test_setoutputsize(cursor):
    await cursor.setoutputsize(4, 1)


async def test_copy_family(connect):
    conn = await connect()
    cur = await conn.cursor()

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_from("file", "table")

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_to("file", "table")

    with pytest.raises(psycopg2.ProgrammingError):
        await cur.copy_expert("sql", "table")


async def test_callproc(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.callproc("inc", [1])
    ret = await cur.fetchone()
    assert (2,) == ret

    cur.close()
    with pytest.raises(psycopg2.InterfaceError):
        await cur.callproc("inc", [1])
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


async def test_isolation_level(connect):
    conn = await connect()
    cur = await conn.cursor(isolation_level=IsolationLevel.read_committed)
    assert isinstance(cur._transaction._isolation, ReadCommittedCompiler)


async def test_iter(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM tbl")
    rows = []
    async for r in cur:
        rows.append(r)

    data = [(1, "a"), (2, "b"), (3, "c")]
    for item, tst in zip(rows, data):
        assert item == tst


async def test_echo_callproc(log, connect):
    conn = await connect(echo=True)
    cur = await conn.cursor()

    with log("aiopg") as watcher:
        await cur.callproc("inc", [1])

    ret = await cur.fetchone()
    assert (2,) == ret
    assert len(watcher.output) == 2
    assert watcher.output == ["INFO:aiopg:CALL inc", "INFO:aiopg:[1]"]
    cur.close()


async def test_replication_family_standard_cursor(cursor):
    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.create_replication_slot(slot_name="test_slot")

    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.drop_replication_slot("test_slot")

    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.start_replication(slot_name="test_slot")

    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.start_replication_expert(
            "START_REPLICATION SLOT test_slot LOGICAL 0/00000000"
        )

    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.send_feedback()

    with pytest.raises(psycopg2.ProgrammingError):
        async for _ in cursor.message_stream():
            pass

    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.consume_stream(lambda msg: msg)

    with pytest.raises(psycopg2.ProgrammingError):
        await cursor.read_message()

    with pytest.raises(psycopg2.ProgrammingError):
        _ = cursor.wal_end

    with pytest.raises(psycopg2.ProgrammingError):
        _ = cursor.feedback_timestamp

    with pytest.raises(psycopg2.ProgrammingError):
        _ = cursor.io_timestamp


@pytest.mark.parametrize(
    "conn_factory,output_plugin,slot_type,slot_type_str",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "test_decoding",
            psycopg2.extras.REPLICATION_LOGICAL,
            "logical",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            None,
            psycopg2.extras.REPLICATION_PHYSICAL,
            "physical",
        ),
    ],
)
async def test_create_replication_slot(
    make_replication_connection,
    cursor,
    get_repl_slot_details,
    conn_factory,
    output_plugin,
    slot_type,
    slot_type_str,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()

    await repl_cur.create_replication_slot(
        slot_name="test_slot",
        slot_type=slot_type,
        output_plugin=output_plugin,
    )

    res = await get_repl_slot_details(cursor, "test_slot")
    assert res
    assert res.slot_name == "test_slot"
    assert res.plugin == output_plugin
    assert res.slot_type == slot_type_str


@pytest.mark.parametrize(
    "conn_factory,output_plugin,slot_type",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "test_decoding",
            "logical",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            None,
            "physical",
        ),
    ],
)
async def test_create_replication_slot_inferred_from_conn_type(
    make_replication_connection,
    cursor,
    get_repl_slot_details,
    conn_factory,
    output_plugin,
    slot_type,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()

    await repl_cur.create_replication_slot(
        slot_name="test_slot",
        output_plugin=output_plugin,
    )

    res = await get_repl_slot_details(cursor, "test_slot")
    assert res
    assert res.slot_type == slot_type


async def test_create_replication_slot_logical_no_output_plugin(
    make_replication_connection,
):
    repl_conn = await make_replication_connection(
        connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    repl_cur = await repl_conn.cursor()

    with pytest.raises(psycopg2.ProgrammingError):
        await repl_cur.create_replication_slot(slot_name="test_slot")


async def test_create_replication_slot_physical_with_output_plugin(
    make_replication_connection,
):
    repl_conn = await make_replication_connection(
        connection_factory=psycopg2.extras.PhysicalReplicationConnection
    )
    repl_cur = await repl_conn.cursor()

    with pytest.raises(psycopg2.ProgrammingError):
        await repl_cur.create_replication_slot(
            slot_name="test_slot",
            output_plugin="test_decoding",
        )


@pytest.mark.parametrize(
    "conn_factory,output_plugin",
    [
        (psycopg2.extras.LogicalReplicationConnection, "test_decoding"),
        (psycopg2.extras.PhysicalReplicationConnection, None),
    ],
)
async def test_create_replication_slot_existing(
    make_replication_connection,
    conn_factory,
    output_plugin,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.create_replication_slot(
        slot_name="test_slot",
        output_plugin=output_plugin,
    )

    with pytest.raises(psycopg2.errors.DuplicateObject):
        await repl_cur.create_replication_slot(
            slot_name="test_slot",
            output_plugin=output_plugin,
        )


@pytest.mark.parametrize(
    "conn_factory,output_plugin,log_str",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "test_decoding",
            "INFO:aiopg:CREATE_REPLICATION_SLOT test_slot "
            "LOGICAL test_decoding",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            None,
            "INFO:aiopg:CREATE_REPLICATION_SLOT test_slot PHYSICAL",
        ),
    ],
)
async def test_echo_create_replication_slot(
    log,
    make_replication_connection,
    conn_factory,
    output_plugin,
    log_str,
):
    repl_conn = await make_replication_connection(
        echo=True,
        connection_factory=conn_factory,
    )
    repl_cur = await repl_conn.cursor()

    with log("aiopg") as watcher:
        await repl_cur.create_replication_slot(
            slot_name="test_slot",
            output_plugin=output_plugin,
        )

    assert len(watcher.output) == 1
    assert watcher.output[0] == log_str


@pytest.mark.parametrize(
    "conn_factory,output_plugin",
    [
        (psycopg2.extras.LogicalReplicationConnection, "test_decoding"),
        (psycopg2.extras.PhysicalReplicationConnection, None),
    ],
)
async def test_drop_replication_slot(
    make_replication_connection,
    cursor,
    get_repl_slot_details,
    conn_factory,
    output_plugin,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.create_replication_slot(
        slot_name="test_slot",
        output_plugin=output_plugin,
    )

    await repl_cur.drop_replication_slot("test_slot")

    assert not await get_repl_slot_details(cursor, "test_slot")


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_drop_replication_slot_non_existing(
    make_replication_connection,
    conn_factory,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()

    with pytest.raises(psycopg2.errors.UndefinedObject):
        await repl_cur.drop_replication_slot("test_slot")


@pytest.mark.parametrize(
    "conn_factory,output_plugin",
    [
        (psycopg2.extras.LogicalReplicationConnection, "test_decoding"),
        (psycopg2.extras.PhysicalReplicationConnection, None),
    ],
)
async def test_echo_drop_replication_slot(
    log,
    make_replication_connection,
    conn_factory,
    output_plugin,
):
    repl_conn = await make_replication_connection(
        echo=True,
        connection_factory=conn_factory,
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.create_replication_slot(
        slot_name="test_slot",
        output_plugin=output_plugin,
    )

    with log("aiopg") as watcher:
        await repl_cur.drop_replication_slot("test_slot")

    assert len(watcher.output) == 1
    assert watcher.output[0] == "INFO:aiopg:DROP_REPLICATION_SLOT test_slot"


@pytest.mark.parametrize(
    "conn_factory,slot_type",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            psycopg2.extras.REPLICATION_LOGICAL,
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            psycopg2.extras.REPLICATION_PHYSICAL,
        ),
    ],
)
async def test_start_replication(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    conn_factory,
    slot_type,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    before_start_repl = await get_repl_slot_details(cursor, "test_slot")
    assert not before_start_repl.active

    await repl_cur.start_replication(
        slot_name="test_slot",
        slot_type=slot_type,
        status_interval=1,
        start_lsn=xlogpos,
    )
    # with physical replication, a slot won't be marked as active
    # until you start consuming from the stream
    if slot_type == psycopg2.extras.REPLICATION_PHYSICAL:
        async with async_timeout.timeout(timeout=1.1, loop=loop):
            await repl_cur.read_message()

    after_start_repl = await get_repl_slot_details(cursor, "test_slot")
    assert after_start_repl.active


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_start_replication_slot_type_inferred_from_conn_type(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    before_start_repl = await get_repl_slot_details(cursor, "test_slot")
    assert not before_start_repl.active

    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=xlogpos,
    )
    # with physical replication, a slot won't be marked as active
    # until you start consuming from the stream
    if conn_factory == psycopg2.extras.PhysicalReplicationConnection:
        async with async_timeout.timeout(timeout=1.1, loop=loop):
            await repl_cur.read_message()

    after_start_repl = await get_repl_slot_details(cursor, "test_slot")
    assert after_start_repl.active


@pytest.mark.parametrize(
    "conn_factory,slot_type",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            psycopg2.extras.REPLICATION_PHYSICAL,
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            psycopg2.extras.REPLICATION_LOGICAL,
        ),
    ],
)
async def test_start_replication_wrong_slot_type(
    connect_replication,
    get_xlogpos,
    conn_factory,
    slot_type,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    with pytest.raises(psycopg2.errors.ObjectNotInPrerequisiteState):
        await repl_cur.start_replication(
            slot_name="test_slot",
            start_lsn=xlogpos,
            slot_type=slot_type,
        )


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_start_replication_status_interval_default(
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    assert repl_cur._status_interval is None
    await repl_cur.start_replication(slot_name="test_slot", start_lsn=xlogpos)
    assert repl_cur._status_interval == 10


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_start_replication_status_interval_override(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    assert repl_cur._status_interval is None
    await repl_cur.start_replication(
        slot_name="test_slot",
        start_lsn=xlogpos,
        status_interval=1,
    )
    assert repl_cur._status_interval == 1

    t1 = time.time()
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        await repl_cur.read_message()
    t2 = time.time()

    dt = t2 - t1
    round_func = math.ceil if dt < 1 else math.floor
    assert round_func(dt) == 1, dt


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_start_replication_status_interval_invalid(
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    assert repl_cur._status_interval is None
    with pytest.raises(psycopg2.ProgrammingError):
        await repl_cur.start_replication(
            slot_name="test_slot",
            start_lsn=xlogpos,
            status_interval=0.5,
        )
    assert repl_cur._status_interval is None


async def test_start_replication_logical_options(
    loop,
    connect_replication,
    cursor,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        options={"add-tables": "public.tbl"},
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))
    await cursor.execute("INSERT INTO tbl2 VALUES(%s, %s)", (1, "a"))

    msgs = []
    async with async_timeout.timeout(timeout=2, loop=loop):
        async for msg in repl_cur.message_stream():
            msgs.append(msg)
            if len(msgs) >= 2:
                break

    # wal2json postgres plugin allows you to filter tables from which you want
    # to receive meaningful replication messages, changes from other
    # tables will be streamed as dummy events
    # because we've only specified the "tbl" table within the plugin options
    # the 'insert' event from "tbl" should be a fully-fledged replication
    # message, on the other hand, "tbl2" table's event should
    # be a dummy/empty message
    assert len(json.loads(msgs[0].payload)["change"]) == 1
    assert len(json.loads(msgs[1].payload)["change"]) == 0


async def test_start_replication_physical_options(
    connect_replication,
    get_xlogpos,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.PhysicalReplicationConnection
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    with pytest.raises(psycopg2.ProgrammingError):
        await repl_cur.start_replication(
            slot_name="test_slot",
            start_lsn=xlogpos,
            options={"add-tables": "public.tbl"},
        )


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_start_replication_no_slot(
    make_replication_connection,
    conn_factory,
    get_xlogpos,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)

    with pytest.raises(psycopg2.errors.UndefinedObject):
        await repl_cur.start_replication(
            slot_name="test_slot",
            start_lsn=xlogpos,
        )


async def test_start_replication_logical_decode(
    loop,
    connect_replication,
    cursor,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        output_plugin="test_decoding",
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        decode=True,
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert not isinstance(msg.payload, bytes)


async def test_start_replication_logical_no_decode(
    loop,
    connect_replication,
    cursor,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        output_plugin="test_decoding",
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(slot_name="test_slot", status_interval=1)
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert isinstance(msg.payload, bytes)


@pytest.mark.parametrize(
    "conn_factory,start_repl_options,log_str",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            {"add-tables": "public.tbl"},
            "INFO:aiopg:START_REPLICATION SLOT test_slot LOGICAL {xlogpos} "
            "(add-tables public.tbl)",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            None,
            "INFO:aiopg:START_REPLICATION SLOT test_slot {xlogpos}",
        ),
    ],
)
async def test_echo_start_replication(
    log,
    connect_replication,
    get_xlogpos,
    conn_factory,
    start_repl_options,
    log_str,
):
    repl_conn = await connect_replication(
        echo=True,
        connection_factory=conn_factory,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur, as_int=False)

    with log("aiopg") as watcher:
        await repl_cur.start_replication(
            slot_name="test_slot",
            start_lsn=xlogpos,
            options=start_repl_options,
        )

    assert len(watcher.output) == 1
    assert watcher.output[0] == log_str.format(xlogpos=xlogpos)


@pytest.mark.parametrize(
    "conn_factory,start_repl_query",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "START_REPLICATION SLOT test_slot LOGICAL {xlogpos}",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            "START_REPLICATION SLOT test_slot {xlogpos}",
        ),
    ],
)
async def test_start_replication_expert(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    conn_factory,
    start_repl_query,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()

    before_start_repl = await get_repl_slot_details(cursor, "test_slot")
    assert not before_start_repl.active

    await repl_cur.start_replication_expert(
        start_repl_query.format(
            xlogpos=await get_xlogpos(repl_cur, as_int=False)
        ),
        status_interval=1,
    )
    # with physical replication, a slot won't be marked as active
    # until you start consuming from the stream
    if conn_factory == psycopg2.extras.PhysicalReplicationConnection:
        async with async_timeout.timeout(timeout=1.1, loop=loop):
            await repl_cur.read_message()

    after_start_repl = await get_repl_slot_details(cursor, "test_slot")
    assert after_start_repl.active


@pytest.mark.parametrize(
    "conn_factory,start_repl_query",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "START_REPLICATION SLOT test_slot LOGICAL {xlogpos}",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            "START_REPLICATION SLOT test_slot {xlogpos}",
        ),
    ],
)
async def test_start_replication_expert_status_interval_default(
    connect_replication,
    get_xlogpos,
    conn_factory,
    start_repl_query,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()

    assert repl_cur._status_interval is None
    await repl_cur.start_replication_expert(
        start_repl_query.format(
            xlogpos=await get_xlogpos(repl_cur, as_int=False)
        )
    )
    assert repl_cur._status_interval == 10


@pytest.mark.parametrize(
    "conn_factory,start_repl_query",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "START_REPLICATION SLOT test_slot LOGICAL {xlogpos}",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            "START_REPLICATION SLOT test_slot {xlogpos}",
        ),
    ],
)
async def test_start_replication_expert_status_interval_override(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
    start_repl_query,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()

    assert repl_cur._status_interval is None
    await repl_cur.start_replication_expert(
        start_repl_query.format(
            xlogpos=await get_xlogpos(repl_cur, as_int=False)
        ),
        status_interval=1,
    )
    assert repl_cur._status_interval == 1

    t1 = time.time()
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        await repl_cur.read_message()
    t2 = time.time()

    dt = t2 - t1
    round_func = math.ceil if dt < 1 else math.floor
    assert round_func(dt) == 1, dt


@pytest.mark.parametrize(
    "conn_factory,start_repl_query",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "START_REPLICATION SLOT test_slot LOGICAL {xlogpos}",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            "START_REPLICATION SLOT test_slot {xlogpos}",
        ),
    ],
)
async def test_start_replication_expert_status_interval_invalid(
    connect_replication,
    get_xlogpos,
    conn_factory,
    start_repl_query,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()

    assert repl_cur._status_interval is None
    with pytest.raises(psycopg2.ProgrammingError):
        await repl_cur.start_replication_expert(
            start_repl_query.format(
                xlogpos=await get_xlogpos(repl_cur, as_int=False)
            ),
            status_interval=0.5,
        )
    assert repl_cur._status_interval is None


async def test_start_replication_expert_logical_decode(
    loop,
    connect_replication,
    cursor,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        output_plugin="test_decoding",
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication_expert(
        "START_REPLICATION SLOT test_slot LOGICAL 0/00000000",
        status_interval=1,
        decode=True,
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert not isinstance(msg.payload, bytes)


async def test_start_replication_expert_logical_no_decode(
    loop,
    connect_replication,
    cursor,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        output_plugin="test_decoding",
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication_expert(
        "START_REPLICATION SLOT test_slot LOGICAL 0/00000000",
        status_interval=1,
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert isinstance(msg.payload, bytes)


@pytest.mark.parametrize(
    "conn_factory,start_repl_query,log_str",
    [
        (
            psycopg2.extras.LogicalReplicationConnection,
            "START_REPLICATION SLOT test_slot LOGICAL {xlogpos}",
            "INFO:aiopg:START_REPLICATION SLOT test_slot LOGICAL {xlogpos}",
        ),
        (
            psycopg2.extras.PhysicalReplicationConnection,
            "START_REPLICATION SLOT test_slot {xlogpos}",
            "INFO:aiopg:START_REPLICATION SLOT test_slot {xlogpos}",
        ),
    ],
)
async def test_echo_start_replication_expert(
    log,
    connect_replication,
    get_xlogpos,
    conn_factory,
    start_repl_query,
    log_str,
):
    repl_conn = await connect_replication(
        echo=True,
        connection_factory=conn_factory,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur, as_int=False)

    with log("aiopg") as watcher:
        await repl_cur.start_replication_expert(
            start_repl_query.format(xlogpos=xlogpos)
        )

    assert len(watcher.output) == 1
    assert watcher.output[0] == log_str.format(xlogpos=xlogpos)


@pytest.mark.parametrize(
    "conn_factory,output_plugin",
    [
        (psycopg2.extras.LogicalReplicationConnection, "test_decoding"),
        (psycopg2.extras.PhysicalReplicationConnection, None),
    ],
)
async def test_execute_replication_command_timeout(
    make_replication_connection,
    conn_factory,
    output_plugin,
):
    repl_conn = await make_replication_connection(
        connection_factory=conn_factory
    )
    repl_cur = await repl_conn.cursor()

    with pytest.raises(asyncio.TimeoutError):
        await repl_cur._execute_replication_command(
            command_name="create_replication_slot",
            timeout=0,
            slot_name="test_slot",
            output_plugin=output_plugin,
        )
    assert repl_cur._impl.closed


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=xlogpos,
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert isinstance(msg, ReplicationMessage)


# for some reason and very rarely, a random replication message gets
# received during extensive test runs - the size of the payload differs
# from the logically decoded WAL entry size corresponding to
# the "never to be inserted"  `(4, "d")` tuple.
@flaky(max_runs=3, min_passes=1)
@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_timeout(
    loop,
    retrieve_task_result,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    async def insert_row():
        await asyncio.sleep(10)
        await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        start_lsn=await get_xlogpos(repl_cur),
    )
    # launch in a separate task so that `read_message()` times out
    # before a row gets inserted
    task = loop.create_task(insert_row())
    task.add_done_callback(retrieve_task_result)

    t1 = time.time()
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message(timeout=1)
    t2 = time.time()

    dt = t2 - t1
    round_func = math.ceil if dt < 1 else math.floor
    assert round_func(dt) == 1, dt
    assert msg is None
    task.cancel()


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_poll_callback_done_future(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )
    dummy_fut = loop.create_future()
    dummy_fut.set_result(None)

    assert repl_cur._read_message(dummy_fut) is None


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_poll_callback_exception(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )
    repl_cur._impl.close()
    dummy_fut = loop.create_future()

    repl_cur._read_message(dummy_fut)

    assert isinstance(dummy_fut.exception(), psycopg2.InterfaceError)


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_poll_callback_sysexit_exception(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )
    dummy_fut = loop.create_future()

    repl_cur._impl.read_message = Mock(side_effect=SystemExit)
    with pytest.raises(SystemExit):
        repl_cur._read_message(dummy_fut)

    repl_cur._impl.read_message = Mock(side_effect=KeyboardInterrupt)
    with pytest.raises(KeyboardInterrupt):
        repl_cur._read_message(dummy_fut)


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_message_stream(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=xlogpos,
    )
    for i in [(4, "d"), (5, "e")]:
        await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", i)

    msgs = []
    async with async_timeout.timeout(timeout=2.2, loop=loop):
        async for msg in repl_cur.message_stream():
            msgs.append(msg)
            if len(msgs) >= 2:
                break

    assert len(msgs) == 2
    for msg in msgs:
        assert isinstance(msg, ReplicationMessage)


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_consume_stream_corofunc_consumer(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    async def consumer_callback(message):
        nonlocal msgs
        await asyncio.sleep(0.01)
        msgs.append(message)
        if len(msgs) >= 2:
            raise psycopg2.extras.StopReplication

    msgs = []
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=xlogpos,
    )
    for i in [(4, "d"), (5, "e")]:
        await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", i)

    async with async_timeout.timeout(timeout=2.2, loop=loop):
        await repl_cur.consume_stream(consumer=consumer_callback)

    assert len(msgs) == 2
    for msg in msgs:
        assert isinstance(msg, ReplicationMessage)


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_consume_stream_callable_consumer(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    def consumer_callback(message):
        nonlocal msgs
        msgs.append(message)
        if len(msgs) >= 2:
            raise psycopg2.extras.StopReplication

    msgs = []
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=xlogpos,
    )
    for i in [(4, "d"), (5, "e")]:
        await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", i)

    async with async_timeout.timeout(timeout=2.2, loop=loop):
        await repl_cur.consume_stream(consumer=consumer_callback)

    assert len(msgs) == 2
    for msg in msgs:
        assert isinstance(msg, ReplicationMessage)


# although each test is run sequentially,
# sometimes unexpected entries are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_replication_message_physical(
    connect_replication,
    cursor,
    get_xlogpos,
    tbl_insert_wal_segment_size,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.PhysicalReplicationConnection
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=xlogpos,
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1):
        msg = await repl_cur.read_message()

    assert isinstance(msg, ReplicationMessage)
    assert isinstance(msg.cursor, _ReplicationCursor)
    assert isinstance(msg.data_size, int)
    assert msg.data_size == tbl_insert_wal_segment_size
    assert msg.data_start == xlogpos
    assert msg.wal_end == xlogpos + msg.data_size
    assert msg.wal_end == msg.data_start + msg.data_size
    send_time_approx_jitter = abs(
        (
            msg.send_time.replace(microsecond=0)
            - datetime.datetime.now().replace(microsecond=0)
        ).total_seconds()
    )
    assert send_time_approx_jitter <= 2
    assert isinstance(msg.payload, bytes)


# although each test is run sequentially,
# sometimes unexpected entries are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_replication_message_logical(
    connect_replication,
    cursor,
    pg_tag,
    get_xlogpos,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(slot_name="test_slot", status_interval=1)
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1):
        msg = await repl_cur.read_message()

    assert isinstance(msg, ReplicationMessage)
    assert isinstance(msg.cursor, _ReplicationCursor)
    assert isinstance(msg.data_size, int)
    # for some reason, on postgres 9.6 only, the original xlogpos is always
    # shifted by 40 bytes, although the size of the raw WAL segment is still
    # 112 bytes
    assert msg.data_start == xlogpos if float(pg_tag) >= 10 else xlogpos + 40
    assert msg.wal_end == xlogpos if float(pg_tag) >= 10 else xlogpos + 40
    assert msg.wal_end == msg.data_start
    send_time_approx_jitter = abs(
        (
            msg.send_time.replace(microsecond=0)
            - datetime.datetime.now().replace(microsecond=0)
        ).total_seconds()
    )
    assert send_time_approx_jitter <= 2
    msg_payload = json.loads(msg.payload)
    assert len(msg_payload["change"]) == 1
    msg_payload = msg_payload["change"][0]
    assert msg_payload["kind"] == "insert"
    assert msg_payload["schema"] == "public"
    assert msg_payload["table"] == "tbl"
    assert msg_payload["columnvalues"][0] == 4
    assert msg_payload["columnvalues"][1] == "d"


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_replication_message_repr(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    def xlog_fmt(xlog):
        return "{:x}/{:x}".format(xlog >> 32, xlog & 0xFFFFFFFF)

    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert repr(msg) == (
        "<{cls_name} data_size={data_size}, data_start={data_start}, "
        "wal_end={wal_end}, send_time={send_time}>".format(
            cls_name=f"{type(msg).__module__}::{type(msg).__name__}",
            data_size=msg.data_size,
            data_start=xlog_fmt(msg.data_start),
            wal_end=xlog_fmt(msg.wal_end),
            send_time=msg._impl.__repr__().split(":")[-1][1:-1],
        )
    )


# although each test is run sequentially,
# sometimes unexpected entries are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_send_feedback_force_logical(
    loop,
    pg_params,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    lsn_to_decimal,
    tbl_insert_wal_segment_size,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(slot_name="test_slot", start_lsn=xlogpos)
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    # send feedback message immediately telling Postgres that WAL entries
    # before and including this message should become unavailable for this
    # replication slot and can be removed by the checkpointer process some
    # time in the future
    await msg.cursor.send_feedback(
        flush_lsn=msg.data_start + tbl_insert_wal_segment_size,
        force=True,
    )

    flush_lsn = lsn_to_decimal(
        (await get_repl_slot_details(cursor, "test_slot")).confirmed_flush_lsn
    )
    # ensure that confirmed_flush_lsn has been updated for this
    # replication slot
    assert flush_lsn == msg.data_start + tbl_insert_wal_segment_size
    # simulate connection loss
    repl_cur.close()
    await repl_conn.close()

    # simulate reconnection
    repl_conn = await aiopg.connect(
        **pg_params.copy(),
        enable_hstore=False,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    # no other postgres backends are currently active, so the server's xlogpos
    # after reconnection should be equal to the last confirmed flush LSN
    # before connection loss
    assert xlogpos == flush_lsn

    await repl_cur.start_replication(slot_name="test_slot", status_interval=1)
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    # ensure that the last replication message is not redelivered by the server
    # after reconnection
    assert msg is None

    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (5, "c"))
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    # ensure that new WAL entries are still delivered as replication messages
    assert isinstance(msg, ReplicationMessage)


# although each test is run sequentially,
# sometimes unexpected entries are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_send_feedback_auto_logical(
    loop,
    pg_params,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    lsn_to_decimal,
    tbl_insert_wal_segment_size,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(slot_name="test_slot", status_interval=1)
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    msg = None
    try:
        async with async_timeout.timeout(timeout=1.2, loop=loop):
            async for m in repl_cur.message_stream():
                msg = m
                # update pyscopg2's internal structures so that a feedback
                # message will be automatically sent when the
                # current `status_interval` of 1 second is reached
                # this will tell Postgres that WAL entries before and including
                # this message should become unavailable for this replication
                # slot and can be removed by the checkpointer process
                # sometime in the future
                await m.cursor.send_feedback(
                    flush_lsn=msg.data_start + tbl_insert_wal_segment_size
                )
    except asyncio.TimeoutError:
        pass

    assert isinstance(msg, ReplicationMessage)

    flush_lsn = lsn_to_decimal(
        (await get_repl_slot_details(cursor, "test_slot")).confirmed_flush_lsn
    )
    # ensure that confirmed_flush_lsn has been updated for this
    # replication slot
    assert flush_lsn == msg.data_start + tbl_insert_wal_segment_size
    # simulate connection loss
    repl_cur.close()
    await repl_conn.close()

    # simulate reconnection
    repl_conn = await aiopg.connect(
        **pg_params.copy(),
        enable_hstore=False,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    # no other postgres backends are currently active, so the server's xlogpos
    # after reconnection should be equal to the last confirmed flush LSN
    # before connection loss
    assert xlogpos == flush_lsn

    await repl_cur.start_replication(slot_name="test_slot", status_interval=1)
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    # ensure that the last replication message is not redelivered by the server
    # after reconnection
    assert msg is None

    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (5, "c"))
    async with async_timeout.timeout(timeout=1.2, loop=loop):
        msg = await repl_cur.read_message()

    # ensure that new WAL entries are still delivered as replication messages
    assert isinstance(msg, ReplicationMessage)


# although each test is run sequentially,
# sometimes unexpected entries are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_without_send_feedback_logical(
    loop,
    pg_params,
    pg_tag,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    lsn_to_decimal,
    tbl_insert_wal_segment_size,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos_before = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(slot_name="test_slot")
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    flush_lsn_before = lsn_to_decimal(
        (await get_repl_slot_details(cursor, "test_slot")).confirmed_flush_lsn
    )
    # ensure that the slot's confirmed_flush_lsn hasn't changed
    assert xlogpos_before == flush_lsn_before
    # simulate connection loss
    repl_cur.close()
    await repl_conn.close()

    # simulate reconnection
    repl_conn = await aiopg.connect(
        **pg_params.copy(),
        enable_hstore=False,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    xlogpos_after = await get_xlogpos(repl_cur)
    flush_lsn_after = lsn_to_decimal(
        (await get_repl_slot_details(cursor, "test_slot")).confirmed_flush_lsn
    )

    # ensure that the new xlogpos has advanced by the size of the previously
    # fsynced WAL entry
    if float(pg_tag) < 10:
        # for some reason, on postgres 9.6 only, the original xlogpos is always
        # shifted by 40 bytes, although the size of the raw WAL segment
        # is still 112 bytes
        assert (
            xlogpos_after == xlogpos_before + tbl_insert_wal_segment_size + 40
        )
    else:
        assert xlogpos_after == xlogpos_before + tbl_insert_wal_segment_size
    # ensure that the slot's confirmed_flush_lsn hasn't changed
    # after reconnection
    assert flush_lsn_after == flush_lsn_before
    await repl_cur.start_replication(slot_name="test_slot")

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        dup_msg = await repl_cur.read_message()

    # ensure that the last message before connection loss is redelivered by the
    # server because there was no feedback message sent
    assert isinstance(dup_msg, ReplicationMessage)
    assert dup_msg.payload == msg.payload


# although each test is run sequentially, sometimes unexpected entries
# are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_send_feedback_force_physical(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    lsn_to_decimal,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.PhysicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        start_lsn=await get_xlogpos(repl_cur),
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    # send feedback message immediately telling postgres that WAL entries
    # before and including this message can be automatically removed
    # during checkpoints
    await msg.cursor.send_feedback(
        flush_lsn=msg.data_start + msg.data_size,
        force=True,
    )

    restart_lsn = lsn_to_decimal(
        (await get_repl_slot_details(cursor, "test_slot")).restart_lsn
    )
    # ensure that restart_lsn has been updated for this
    # replication slot
    assert restart_lsn == msg.data_start + msg.data_size


# although each test is run sequentially, sometimes unexpected entries
# are fsynced to WAL breaking the test assertions
# (possibly done by some postgres background process)
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
async def test_send_feedback_auto_physical(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
    lsn_to_decimal,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.PhysicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    msg = None
    try:
        async with async_timeout.timeout(timeout=1.2, loop=loop):
            async for m in repl_cur.message_stream():
                msg = m
                # update psycopg2's internal structures so that a feedback
                # message will be sent automatically when the current
                # `status_interval` of 1 second is reached,
                # this will tell postgres that WAL entries before and including
                # this message can be automatically removed during checkpoints
                await m.cursor.send_feedback(
                    flush_lsn=msg.data_start + msg.data_size
                )
    except asyncio.TimeoutError:
        pass

    assert isinstance(msg, ReplicationMessage)
    restart_lsn = lsn_to_decimal(
        (await get_repl_slot_details(cursor, "test_slot")).restart_lsn
    )
    # ensure that restart_lsn has been updated for this
    # replication slot
    assert restart_lsn == msg.data_start + msg.data_size


async def test_without_send_feedback_physical(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    get_repl_slot_details,
):
    repl_conn = await connect_replication(
        connection_factory=psycopg2.extras.PhysicalReplicationConnection,
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        start_lsn=await get_xlogpos(repl_cur),
    )
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        await repl_cur.read_message()

    restart_lsn = (
        await get_repl_slot_details(cursor, "test_slot")
    ).restart_lsn
    # ensure that this slot's restart_lsn is None because it has
    # never been reserved
    assert restart_lsn is None


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_wal_end(
    loop,
    pg_tag,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(slot_name="test_slot", start_lsn=xlogpos)
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1.1, loop=loop):
        msg = await repl_cur.read_message()

    assert repl_cur.wal_end == msg.wal_end
    if conn_factory == psycopg2.extras.PhysicalReplicationConnection:
        assert repl_cur.wal_end == xlogpos + msg.data_size
    else:
        if float(pg_tag) < 10:
            # for some reason, on postgres 9.6 only, the original xlogpos
            # is always shifted by 40 bytes, although the size of the raw
            # WAL segment is still 112 bytes
            assert repl_cur.wal_end == xlogpos + 40
        else:
            assert repl_cur.wal_end == xlogpos


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_feedback_timestamp(
    loop,
    connect_replication,
    cursor,
    get_xlogpos,
    tbl_insert_wal_segment_size,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    xlogpos = await get_xlogpos(repl_cur)
    await repl_cur.start_replication(slot_name="test_slot", start_lsn=xlogpos)
    await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    async with async_timeout.timeout(timeout=1, loop=loop):
        msg = await repl_cur.read_message()

    feedback_timestamp_approx_jitter = abs(
        (
            repl_cur.feedback_timestamp.replace(microsecond=0)
            - datetime.datetime.now().replace(microsecond=0)
        ).total_seconds()
    )
    assert feedback_timestamp_approx_jitter <= 2

    await asyncio.sleep(1)
    await msg.cursor.send_feedback(
        flush_lsn=msg.data_start + tbl_insert_wal_segment_size,
        force=True,
    )

    feedback_timestamp_approx_jitter = abs(
        (
            repl_cur.feedback_timestamp.replace(microsecond=0)
            - datetime.datetime.now().replace(microsecond=0)
        ).total_seconds()
    )
    assert feedback_timestamp_approx_jitter <= 2


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_io_timestamp(
    loop,
    retrieve_task_result,
    connect_replication,
    cursor,
    get_xlogpos,
    conn_factory,
):
    async def insert_row():
        await asyncio.sleep(1)
        await cursor.execute("INSERT INTO tbl VALUES(%s, %s)", (4, "d"))

    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=2,
        start_lsn=await get_xlogpos(repl_cur),
    )

    io_timestamp_approx_jitter = abs(
        (
            repl_cur.io_timestamp.replace(microsecond=0)
            - datetime.datetime.now().replace(microsecond=0)
        ).total_seconds()
    )
    assert io_timestamp_approx_jitter <= 2

    task = loop.create_task(insert_row())
    task.add_done_callback(retrieve_task_result)
    async with async_timeout.timeout(timeout=2.2, loop=loop):
        await repl_cur.read_message()

    io_timestamp_approx_jitter = abs(
        (
            repl_cur.io_timestamp.replace(microsecond=0)
            - datetime.datetime.now().replace(microsecond=0)
        ).total_seconds()
    )
    assert io_timestamp_approx_jitter <= 2
    task.cancel()


@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_done_callback(
    loop,
    retrieve_task_result,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    def get_callback_by_fd(fd):
        return loop._selector.get_key(fd).data[0]._callback

    async def read_message():
        async with async_timeout.timeout(timeout=1.1, loop=loop):
            await repl_cur.read_message()

    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    repl_conn_fd = repl_cur._impl.fileno()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )

    assert get_callback_by_fd(repl_conn_fd) == aiopg.Connection._ready

    task = loop.create_task(read_message())
    task.add_done_callback(retrieve_task_result)

    await asyncio.sleep(0)
    assert get_callback_by_fd(repl_conn_fd) == repl_cur._read_message
    await asyncio.sleep(1.1)
    assert get_callback_by_fd(repl_conn_fd) == aiopg.Connection._ready
    task.cancel()


# sometimes the connection's fd isn't closed in a timely manner as specified
# by the test
# is flaky mostly only during long test runs
@flaky(max_runs=3, min_passes=1)
@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_done_callback_bad_descriptor(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    def break_fd():
        loop.remove_reader(repl_conn_fd)
        repl_cur._conn._conn.close()

    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    repl_conn_fd = repl_cur._impl.fileno()
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )

    loop.call_later(0.9, break_fd)
    async with async_timeout.timeout(timeout=1.1, loop=loop):
        await repl_cur.read_message()

    # skip one loop iteration, so that `read_message()`'s
    # `_read_message_done()` done callback gets called
    await asyncio.sleep(0)
    with pytest.raises(KeyError):
        loop._selector.get_key(repl_conn_fd)
    assert repl_cur._conn._fileno is None


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="No need to do a 'select' syscall on platforms except Windows",
)
@pytest.mark.parametrize("conn_factory", REPLICATION_CONN_FACTORIES)
async def test_read_message_done_callback_verify_fd_via_select(
    loop,
    connect_replication,
    get_xlogpos,
    conn_factory,
):
    repl_conn = await connect_replication(connection_factory=conn_factory)
    repl_cur = await repl_conn.cursor()
    repl_conn_fd = repl_cur._impl.fileno()
    dummy_fut = loop.create_future()
    dummy_fut.set_result(None)
    await repl_cur.start_replication(
        slot_name="test_slot",
        status_interval=1,
        start_lsn=await get_xlogpos(repl_cur),
    )

    with patch("select.select") as mocked_select:
        repl_cur._read_message_done(repl_conn_fd, dummy_fut)

    mocked_select.assert_called_once_with([repl_conn_fd], [], [], 0)


# postgres doesn't support SQL queries on logical replication connections
# before version 10
@pytest.mark.skip_pg_tag_older_than(10)
async def test_replication_logical_query_support(make_replication_connection):
    repl_conn = await make_replication_connection(
        connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    repl_cur = await repl_conn.cursor()
    await repl_cur.execute("SELECT 1")


# postgres doesn't support SQL queries on logical replication connections
# before version 10
@pytest.mark.skip_pg_tag_older_than(10)
async def test_replication_logical_hstore_support(make_replication_connection):
    await make_replication_connection(
        enable_hstore=True,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
