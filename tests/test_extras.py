import pytest

from aiopg.extras import _paginate, execute_batch


@pytest.fixture
def connect(make_connection):
    async def go(**kwargs):
        conn = await make_connection(**kwargs)
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS tbl_extras")
            await cur.execute("CREATE TABLE tbl_extras (id int)")
        return conn

    return go


@pytest.fixture
def cursor(connect, loop):
    async def go():
        return await (await connect()).cursor()

    cur = loop.run_until_complete(go())
    yield cur
    cur.close()


def test__paginate():
    data = [
        [1, 2, 3],
        [4, 5, 6],
        [7],
    ]
    for index, val in enumerate(_paginate((1, 2, 3, 4, 5, 6, 7), page_size=3)):
        assert data[index] == list(val)


def test__paginate_even():
    data = [
        [1, 2, 3],
        [4, 5, 6],
    ]
    for index, val in enumerate(_paginate((1, 2, 3, 4, 5, 6), page_size=3)):
        assert data[index] == list(val)


async def test_execute_batch(cursor):
    args = [(1,), (2,), (3,), (4,)]
    sql = 'insert into tbl_extras values(%s)'
    await execute_batch(cursor, sql, argslist=args, page_size=3)

    await cursor.execute('SELECT * from tbl_extras')
    ret = await cursor.fetchall()
    assert list(ret) == args
