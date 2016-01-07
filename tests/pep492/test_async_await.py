import unittest
import asyncio
import pytest

import aiopg
from aiopg.sa import create_engine, SAConnection


class TestAsyncWith(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.database = 'aiopg'
        self.user = 'aiopg'
        self.host = '127.0.0.1'
        self.password = 'passwd'

    def tearDown(self):
        self.loop.close()
        self.loop = None

    async def connect(self, no_loop=False, **kwargs):
        loop = None if no_loop else self.loop
        conn = await aiopg.connect(database=self.database,
                                   user=self.user,
                                   password=self.password,
                                   host=self.host,
                                   loop=loop,
                                   **kwargs)
        self.addCleanup(conn.close)
        return conn

    def test_cursor_await(self):
        async def go():
            conn = await self.connect()

            cursor = await conn.cursor()
            await cursor.execute('SELECT 42;')
            resp = await cursor.fetchone()
            assert resp == (42, )
            cursor.close()

        self.loop.run_until_complete(go())

    def test_connect_context_manager(self):
        async def go():
            kw = dict(database='aiopg', user='aiopg', password='passwd',
                      host='127.0.0.1', loop=self.loop)
            async with aiopg.connect(**kw) as conn:
                cursor = await conn.cursor()
                await cursor.execute('SELECT 42')
                resp = await cursor.fetchone()
                assert resp == (42, )
                cursor.close()
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_connection_context_manager(self):
        async def go():
            conn = await self.connect()
            assert not conn.closed
            async with conn:
                cursor = await conn.cursor()
                await cursor.execute('SELECT 42;')
                resp = await cursor.fetchone()
                assert resp == (42, )
                cursor.close()
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_cursor_create_with_context_manager(self):
        async def go():
            conn = await self.connect()

            async with conn.cursor() as cursor:
                await cursor.execute('SELECT 42;')
                resp = await cursor.fetchone()
                assert resp == (42, )
                assert not cursor.closed

            assert cursor.closed
        self.loop.run_until_complete(go())

    def test_cursor_with_context_manager(self):
        async def go():
            conn = await self.connect()
            cursor = await conn.cursor()
            await cursor.execute('SELECT 42;')

            assert not cursor.closed
            async with cursor:
                resp = await cursor.fetchone()
                assert resp == (42, )
            assert cursor.closed

        self.loop.run_until_complete(go())

    def test_cursor_lightweight(self):
        async def go():
            conn = await self.connect()
            cursor = await conn.cursor()
            await cursor.execute('SELECT 42;')

            assert not cursor.closed
            async with cursor:
                pass
            assert cursor.closed

        self.loop.run_until_complete(go())

    def test_pool_context_manager(self):
        async def go():
            pool = await aiopg.create_pool(host=self.host, user=self.user,
                                           database=self.database,
                                           password=self.password,
                                           loop=self.loop)
            async with pool:
                conn = await pool.acquire()
                async with conn.cursor() as cursor:
                    await cursor.execute('SELECT 42;')
                    resp = await cursor.fetchone()
                    assert resp == (42, )
                pool.release(conn)
            assert cursor.closed
            assert pool.closed
        self.loop.run_until_complete(go())

    def test_create_pool_context_manager(self):
        async def go():
            async with aiopg.create_pool(host=self.host, user=self.user,
                                         database=self.database,
                                         password=self.password,
                                         loop=self.loop) as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute('SELECT 42;')
                        resp = await cursor.fetchone()
                        assert resp == (42, )

            assert cursor.closed
            assert conn.closed
            assert pool.closed

        self.loop.run_until_complete(go())

    def test_cursor_aiter(self):
        async def go():
            result = []
            conn = await self.connect()
            assert not conn.closed
            async with conn:
                cursor = await conn.cursor()
                await cursor.execute('SELECT generate_series(1, 5);')
                async for v in cursor:
                    result.append(v)
                assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
                cursor.close()
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_engine_context_manager(self):
        async def go():
            engine = await create_engine(host=self.host, user=self.user,
                                         database=self.database,
                                         password=self.password,
                                         loop=self.loop)
            async with engine:
                conn = await engine.acquire()
                assert isinstance(conn, SAConnection)
                engine.release(conn)
            assert engine.closed
        self.loop.run_until_complete(go())

    def test_create_engine_context_manager(self):
        async def go():
            async with create_engine(host=self.host, user=self.user,
                                     database=self.database,
                                     password=self.password,
                                     loop=self.loop) as engine:
                async with engine.acquire() as conn:
                    assert isinstance(conn, SAConnection)
            assert engine.closed

        self.loop.run_until_complete(go())

    def test_result_proxy_aiter(self):
        async def go():
            sql = 'SELECT generate_series(1, 5);'
            result = []
            async with create_engine(host=self.host, user=self.user,
                                     database=self.database,
                                     password=self.password,
                                     loop=self.loop) as engine:
                async with engine.acquire() as conn:
                    async with conn.execute(sql) as cursor:
                        async for v in cursor:
                            result.append(v)
                        assert result == [(1,), (2, ), (3, ), (4, ), (5, )]
                    assert cursor.closed
            assert conn.closed

        self.loop.run_until_complete(go())

    def test_transaction_context_manager(self):
        async def go():
            sql = 'SELECT generate_series(1, 5);'
            result = []
            async with create_engine(host=self.host, user=self.user,
                                     database=self.database,
                                     password=self.password,
                                     loop=self.loop) as engine:
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
        self.loop.run_until_complete(go())

    def test_transaction_context_manager_error(self):
        async def go():
            async with create_engine(host=self.host, user=self.user,
                                     database=self.database,
                                     password=self.password,
                                     loop=self.loop) as engine:
                async with engine.acquire() as conn:
                    with pytest.raises(RuntimeError) as ctx:
                        async with conn.begin() as tr:
                            assert tr.is_active
                            raise RuntimeError('boom')
                    assert str(ctx.value) == 'boom'
                    assert not tr.is_active
            assert conn.closed
        self.loop.run_until_complete(go())

    def test_transaction_context_manager_commit_once(self):
        async def go():
            async with create_engine(host=self.host, user=self.user,
                                     database=self.database,
                                     password=self.password,
                                     loop=self.loop) as engine:
                async with engine.acquire() as conn:
                    async with conn.begin() as tr:
                        # check that in context manager we do not execute
                        # commit for second time. Two commits in row causes
                        # InvalidRequestError exception
                        await tr.commit()
                    assert not tr.is_active
            assert conn.closed
        self.loop.run_until_complete(go())
