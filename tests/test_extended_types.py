import pytest
import uuid

from psycopg2.extras import Json


@pytest.mark.run_loop
def test_uuid(connect):
    conn = yield from connect()
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


@pytest.mark.run_loop
def test_json(connect):
    conn = yield from connect()
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
