import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa


metadata = sa.MetaData()

tbl = sa.Table('tbl', metadata,
               sa.Column('id', sa.Integer, primary_key=True),
               sa.Column('val', sa.String(255)))


async def create_table(engine):
    with (await engine) as conn:
        await conn.execute('DROP TABLE IF EXISTS tbl')
        await conn.execute('''CREATE TABLE tbl (
                                            id serial PRIMARY KEY,
                                            val varchar(255))''')


async def go():
    engine = await create_engine(user='aiopg',
                                      database='aiopg',
                                      host='127.0.0.1',
                                      password='passwd')

    await create_table(engine)
    with (await engine) as conn:
        await conn.execute(tbl.insert().values(val='abc'))

        res = await conn.execute(tbl.select())
        for row in res:
            print(row.id, row.val)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
