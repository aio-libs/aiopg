import warnings

import pytest
import sqlalchemy as sa

meta = sa.MetaData()
tbl = sa.Table(
    'sa_tbl5', meta,
    sa.Column('ID', sa.String, primary_key=True, key='id'),
    sa.Column('Name', sa.String(255), key='name'),
)


@pytest.fixture
def connect(make_sa_connection, loop):
    async def start():
        conn = await make_sa_connection()
        await conn.execute('DROP TABLE IF EXISTS sa_tbl5')
        await conn.execute(
            'CREATE TABLE sa_tbl5 ('
            '"ID" VARCHAR(255) NOT NULL, '
            '"Name" VARCHAR(255), '
            'PRIMARY KEY ("ID"))'
        )

        await conn.execute(
            tbl.insert().values(id='test1', name='test_name'))
        await conn.execute(
            tbl.insert().values(id='test2', name='test_name'))
        await conn.execute(
            tbl.insert().values(id='test3', name='test_name'))

        return conn

    return loop.run_until_complete(start())


async def test_insert(connect):
    await connect.execute(tbl.insert().values(id='test-4', name='test_name'))
    await connect.execute(tbl.insert().values(id='test-5', name='test_name'))
    assert 5 == len(await (await connect.execute(tbl.select())).fetchall())


async def test_insert_make_engine(make_engine, connect):
    engine = await make_engine()
    async with engine.acquire() as conn:
        assert conn._cursor is None

        await conn.execute(tbl.insert().values(id='test-4', name='test_name'))
        assert conn._cursor.closed is True

        resp = await conn.execute(tbl.select())
        assert resp.cursor.closed is False
        assert conn._cursor.closed is False

        await conn.execute(tbl.insert().values(id='test-5', name='test_name'))
        assert conn._cursor.closed is True

        resp = await conn.execute(tbl.select())
        assert resp.cursor.closed is False

    assert conn._cursor.closed is True

    assert conn.closed == 0

    assert 5 == len(await (await connect.execute(tbl.select())).fetchall())


async def test_two_cursor_create_context_manager(make_connection):
    conn = await make_connection()

    error_ms = (
        'You can only have one cursor per connection. '
        'The cursor for connection will be closed forcibly'
        ' {!r}.'
    )

    with warnings.catch_warnings(record=True) as wars:
        warnings.simplefilter("always")
        async with conn.cursor() as cur:
            error_ms = error_ms.format(conn)
            assert cur.closed is False

            async with conn.cursor() as cur2:
                assert cur.closed is True
                assert cur2.closed is False

            assert len(wars) == 1
            war = wars.pop()
            assert issubclass(war.category, ResourceWarning)
            assert str(war.message) == error_ms
            assert cur2.closed is True

    assert cur.closed is True
