import asyncio

import sqlalchemy as sa
from psycopg2 import InternalError
from psycopg2.extensions import TransactionRollbackError
from sqlalchemy.sql.ddl import CreateTable

from aiopg.sa import create_engine

metadata = sa.MetaData()

users = sa.Table(
    'users_sa_isolation_transaction', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('name', sa.String(255))
)


async def create_sa_transaction_tables(conn):
    await conn.execute(CreateTable(users))


async def repea_sa_transaction(conn, conn2):
    isolation_level = 'REPEATABLE READ'
    await conn.execute(sa.insert(users).values(id=1, name='test1'))
    t1 = await conn.begin(isolation_level=isolation_level)

    where = users.c.id == 1
    q_user = users.select().where(where)
    user = await (await conn.execute(q_user)).fetchone()

    assert await (await conn2.execute(q_user)).fetchone() == user

    await conn.execute(sa.update(users).values({'name': 'name2'}).where(where))

    t2 = await conn2.begin(isolation_level=isolation_level)
    assert await (await conn2.execute(q_user)).fetchone() == user

    await t1.commit()

    await conn2.execute(users.insert().values({'id': 2, 'name': 'test'}))

    try:
        await conn2.execute(
            sa.update(users).values({'name': 't'}).where(where))
    except TransactionRollbackError as e:
        assert e.pgcode == '40001'

    await t2.commit()

    assert len(await (await conn2.execute(q_user)).fetchall()) == 1
    await conn.execute(sa.delete(users))
    assert len(await (await conn.execute(users.select())).fetchall()) == 0


async def serializable_sa_transaction(conn, conn2):
    isolation_level = 'SERIALIZABLE'
    await conn.execute(sa.insert(users).values(id=1, name='test1'))
    t1 = await conn.begin(isolation_level=isolation_level)

    where = users.c.id == 1
    q_user = users.select().where(where)
    user = await (await conn.execute(q_user)).fetchone()

    assert await (await conn2.execute(q_user)).fetchone() == user

    await conn.execute(sa.update(users).values({'name': 'name2'}).where(where))

    t2 = await conn2.begin(isolation_level=isolation_level)
    assert await (await conn2.execute(q_user)).fetchone() == user

    await t1.commit()

    try:
        await conn2.execute(users.insert().values({'id': 2, 'name': 'test'}))
    except TransactionRollbackError as e:
        assert e.pgcode == '40001'

    try:
        await conn2.execute(users.update().values({'name': 't'}).where(where))
    except InternalError as e:
        assert e.pgcode == '25P02'

    await t2.commit()

    user = dict(await (await conn2.execute(q_user)).fetchone())
    assert user == {'name': 'name2', 'id': 1}

    await conn.execute(sa.delete(users))
    assert len(await (await conn.execute(users.select())).fetchall()) == 0


async def read_only_read_sa_transaction(conn, deferrable):
    await conn.execute(sa.insert(users).values(id=1, name='test1'))
    t1 = await conn.begin(
        isolation_level='SERIALIZABLE',
        readonly=True,
        deferrable=deferrable
    )

    where = users.c.id == 1

    try:
        await conn.execute(sa.update(users).values({'name': 't'}).where(where))
    except InternalError as e:
        assert e.pgcode == '25006'

    await t1.commit()

    await conn.execute(sa.delete(users))
    assert len(await (await conn.execute(users.select())).fetchall()) == 0


async def isolation_read_sa_transaction(conn, conn2):
    await conn.execute(sa.insert(users).values(id=1, name='test1'))
    t1 = await conn.begin()

    where = users.c.id == 1
    q_user = users.select().where(where)
    user = await (await conn.execute(q_user)).fetchone()

    assert await (await conn2.execute(q_user)).fetchone() == user

    await conn.execute(sa.update(users).values({'name': 'name2'}).where(where))

    t2 = await conn2.begin()
    assert await (await conn2.execute(q_user)).fetchone() == user

    await t1.commit()

    await conn2.execute(sa.update(users).values(user).where(where))
    await t2.commit()

    assert await (await conn2.execute(q_user)).fetchone() == user

    await conn.execute(sa.delete(users))
    assert len(await (await conn.execute(users.select())).fetchall()) == 0


async def go():
    engine = await create_engine(user='aiopg',
                                 database='aiopg',
                                 host='127.0.0.1',
                                 password='passwd')
    async with engine:
        async with engine.acquire() as conn:
            await create_sa_transaction_tables(conn)

        async with engine.acquire() as conn:
            await read_only_read_sa_transaction(conn, True)
            await read_only_read_sa_transaction(conn, False)

            async with engine.acquire() as conn2:
                await repea_sa_transaction(conn, conn2)
                await serializable_sa_transaction(conn, conn2)
                await isolation_read_sa_transaction(conn, conn2)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
