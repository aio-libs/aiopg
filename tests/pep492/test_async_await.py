import asyncio
import pytest
import aiopg
import aiopg.sa
from aiopg.sa import SAConnection


@asyncio.coroutine
async def test_cursor_await(make_connection):
    conn = await make_connection()

    cursor = await conn.cursor()
    await cursor.execute('SELECT 42;')
    resp = await cursor.fetchone()
    assert resp == (42, )
    cursor.close()


@asyncio.coroutine
async def test_connect_context_manager(loop, pg_params):
    async with aiopg.connect(loop=loop, **pg_params) as conn:
        cursor = await conn.cursor()
        await cursor.execute('SELECT 42')
        resp = await cursor.fetchone()
        assert resp == (42, )
        cursor.close()
    assert conn.closed


@asyncio.coroutine
async def test_connection_context_manager(make_connection):
    conn = await make_connection()
    assert not conn.closed
    async with conn:
        cursor = await conn.cursor()
        await cursor.execute('SELECT 42;')
        resp = await cursor.fetchone()
        assert resp == (42, )
        cursor.close()
    assert conn.closed


@asyncio.coroutine
async def test_cursor_create_with_context_manager(make_connection):
    conn = await make_connection()

    async with conn.cursor() as cursor:
        await cursor.execute('SELECT 42;')
        resp = await cursor.fetchone()
        assert resp == (42, )
        assert not cursor.closed

    assert cursor.closed


@asyncio.coroutine
async def test_cursor_with_context_manager(make_connection):
    conn = await make_connection()
    cursor = await conn.cursor()
    await cursor.execute('SELECT 42;')

    assert not cursor.closed
    async with cursor:
        resp = await cursor.fetchone()
        assert resp == (42, )
    assert cursor.closed


@asyncio.coroutine
async def test_cursor_lightweight(make_connection):
    conn = await make_connection()
    cursor = await conn.cursor()
    await cursor.execute('SELECT 42;')

    assert not cursor.closed
    async with cursor:
        pass
    assert cursor.closed


@asyncio.coroutine
async def test_pool_context_manager(pg_params, loop):
    pool = await aiopg.create_pool(loop=loop, **pg_params)

    async with pool:
        conn = await pool.acquire()
        async with conn.cursor() as cursor:
            await cursor.execute('SELECT 42;')
            resp = await cursor.fetchone()
            assert resp == (42, )
        pool.release(conn)
    assert cursor.closed
    assert pool.closed


@asyncio.coroutine
async def test_create_pool_context_manager(pg_params, loop):
    async with aiopg.create_pool(loop=loop, **pg_params) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT 42;')
                resp = await cursor.fetchone()
                assert resp == (42, )

    assert cursor.closed
    assert conn.closed
    assert pool.closed


@asyncio.coroutine
async def test_cursor_aiter(make_connection):
    result = []
    conn = await make_connection()
    assert not conn.closed
    async with conn:
        cursor = await conn.cursor()
        await cursor.execute('SELECT generate_series(1, 5);')
        async for v in cursor:
            result.append(v)
        assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
        cursor.close()
    assert conn.closed


@asyncio.coroutine
async def test_engine_context_manager(pg_params, loop):
    engine = await aiopg.sa.create_engine(loop=loop, **pg_params)
    async with engine:
        conn = await engine.acquire()
        assert isinstance(conn, SAConnection)
        engine.release(conn)
    assert engine.closed


@asyncio.coroutine
async def test_create_engine_context_manager(pg_params, loop):
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            assert isinstance(conn, SAConnection)
    assert engine.closed


@asyncio.coroutine
async def test_result_proxy_aiter(pg_params, loop):
    sql = 'SELECT generate_series(1, 5);'
    result = []
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.execute(sql) as cursor:
                async for v in cursor:
                    result.append(v)
                assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
            assert cursor.closed
    assert conn.closed


@asyncio.coroutine
async def test_transaction_context_manager(pg_params, loop):
    sql = 'SELECT generate_series(1, 5);'
    result = []
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin() as tr:
                async with conn.execute(sql) as cursor:
                    async for v in cursor:
                        result.append(v)
                    assert tr.is_active
                assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
                assert cursor.closed
            assert not tr.is_active

            tr2 = await conn.begin()
            async with tr2:
                assert tr2.is_active
                async with conn.execute('SELECT 1;') as cursor:
                    rec = await cursor.scalar()
                    assert rec == 1
                    cursor.close()
            assert not tr2.is_active

    assert conn.closed


@asyncio.coroutine
async def test_transaction_context_manager_error(pg_params, loop):
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            with pytest.raises(RuntimeError) as ctx:
                async with conn.begin() as tr:
                    assert tr.is_active
                    raise RuntimeError('boom')
            assert str(ctx.value) == 'boom'
            assert not tr.is_active
    assert conn.closed


@asyncio.coroutine
async def test_transaction_context_manager_commit_once(pg_params, loop):
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin() as tr:
                # check that in context manager we do not execute
                # commit for second time. Two commits in row causes
                # InvalidRequestError exception
                await tr.commit()
            assert not tr.is_active

            tr2 = await conn.begin()
            async with tr2:
                assert tr2.is_active
                # check for double commit one more time
                await tr2.commit()
            assert not tr2.is_active
    assert conn.closed


@asyncio.coroutine
async def test_transaction_context_manager_nested_commit(pg_params, loop):
    sql = 'SELECT generate_series(1, 5);'
    result = []
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin_nested() as tr1:
                async with conn.begin_nested() as tr2: 
                    async with conn.execute(sql) as cursor:
                        async for v in cursor:
                            result.append(v)
                        assert tr1.is_active
                        assert tr2.is_active
                    assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
                    assert cursor.closed
                assert not tr2.is_active

                tr2 = await conn.begin_nested()
                async with tr2:
                    assert tr2.is_active
                    async with conn.execute('SELECT 1;') as cursor:
                        rec = await cursor.scalar()
                        assert rec == 1
                        cursor.close()
                assert not tr2.is_active
            assert not tr1.is_active

    assert conn.closed


@asyncio.coroutine
async def test_sa_connection_execute(pg_params, loop):
    sql = 'SELECT generate_series(1, 5);'
    result = []
    async with aiopg.sa.create_engine(loop=loop, **pg_params) as engine:
        async with engine.acquire() as conn:
            async for value in conn.execute(sql):
                result.append(value)
            assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
    assert conn.closed
