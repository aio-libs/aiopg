import asyncio

import sqlalchemy as sa
from sqlalchemy.schema import CreateTable

from aiopg.sa import create_engine

metadata = sa.MetaData()

users = sa.Table(
    'users_sa_transaction', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('name', sa.String(255))
)


async def create_sa_transaction_tables(conn):
    await conn.execute(CreateTable(users))


async def check_count_users(conn, *, count):
    s_query = sa.select(users).select_from(users)
    assert count == len(list(await (await conn.execute(s_query)).fetchall()))


async def success_transaction(conn):
    await check_count_users(conn, count=0)

    async with conn.begin():
        await conn.execute(sa.insert(users).values(id=1, name='test1'))
        await conn.execute(sa.insert(users).values(id=2, name='test2'))

    await check_count_users(conn, count=2)

    async with conn.begin():
        await conn.execute(sa.delete(users).where(users.c.id == 1))
        await conn.execute(sa.delete(users).where(users.c.id == 2))

    await check_count_users(conn, count=0)


async def fail_transaction(conn):
    await check_count_users(conn, count=0)

    trans = await conn.begin()

    try:
        await conn.execute(sa.insert(users).values(id=1, name='test1'))
        raise RuntimeError()

    except RuntimeError:
        await trans.rollback()
    else:
        await trans.commit()

    await check_count_users(conn, count=0)


async def success_nested_transaction(conn):
    await check_count_users(conn, count=0)

    async with conn.begin_nested():
        await conn.execute(sa.insert(users).values(id=1, name='test1'))

        async with conn.begin_nested():
            await conn.execute(sa.insert(users).values(id=2, name='test2'))

    await check_count_users(conn, count=2)

    async with conn.begin():
        await conn.execute(sa.delete(users).where(users.c.id == 1))
        await conn.execute(sa.delete(users).where(users.c.id == 2))

    await check_count_users(conn, count=0)


async def fail_nested_transaction(conn):
    await check_count_users(conn, count=0)

    async with conn.begin_nested():
        await conn.execute(sa.insert(users).values(id=1, name='test1'))

        tr_f = await conn.begin_nested()
        try:
            await conn.execute(sa.insert(users).values(id=2, name='test2'))
            raise RuntimeError()

        except RuntimeError:
            await tr_f.rollback()
        else:
            await tr_f.commit()

        async with conn.begin_nested():
            await conn.execute(sa.insert(users).values(id=2, name='test2'))

    await check_count_users(conn, count=2)

    async with conn.begin():
        await conn.execute(sa.delete(users).where(users.c.id == 1))
        await conn.execute(sa.delete(users).where(users.c.id == 2))

    await check_count_users(conn, count=0)


async def fail_first_nested_transaction(conn):
    trans = await conn.begin_nested()

    try:
        await conn.execute(sa.insert(users).values(id=1, name='test1'))

        async with conn.begin_nested():
            await conn.execute(sa.insert(users).values(id=2, name='test2'))

        async with conn.begin_nested():
            await conn.execute(sa.insert(users).values(id=3, name='test3'))

        raise RuntimeError()

    except RuntimeError:
        await trans.rollback()
    else:
        await trans.commit()

    await check_count_users(conn, count=0)


async def go():
    engine = await create_engine(user='aiopg',
                                 database='aiopg',
                                 host='127.0.0.1',
                                 password='passwd')
    async with engine:
        async with engine.acquire() as conn:
            await create_sa_transaction_tables(conn)

            await success_transaction(conn)
            await fail_transaction(conn)

            await success_nested_transaction(conn)
            await fail_nested_transaction(conn)
            await fail_first_nested_transaction(conn)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
