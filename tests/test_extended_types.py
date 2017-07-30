import asyncio
import uuid

from aiopg.psycopg2_compat.extras import Json


@asyncio.coroutine
def test_uuid(make_connection):
    conn = yield from make_connection()
    _id = uuid.uuid1()
    cur = yield from conn.cursor()
    try:
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("""CREATE TABLE tbl (id UUID)""")
        yield from cur.execute(
            "INSERT INTO tbl (id) VALUES (%s)", [_id])
        yield from cur.execute("SELECT * FROM tbl")
        item = yield from cur.fetchone()
        assert (_id,) == item
    finally:
        cur.close()


@asyncio.coroutine
def test_json(make_connection):
    conn = yield from make_connection()
    data = {'a': 1, 'b': 'str'}
    cur = yield from conn.cursor()
    try:
        yield from cur.execute("DROP TABLE IF EXISTS tbl")
        yield from cur.execute("""CREATE TABLE tbl (
                              id SERIAL,
                              val JSON)""")
        yield from cur.execute(
            "INSERT INTO tbl (val) VALUES (%s)", [Json(data)])
        yield from cur.execute("SELECT * FROM tbl")
        item = yield from cur.fetchone()
        assert (1, {'b': 'str', 'a': 1}) == item
    finally:
        cur.close()
