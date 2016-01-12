import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa


metadata = sa.MetaData()

tbl = sa.Table('tbl', metadata,
               sa.Column('id', sa.Integer, primary_key=True),
               sa.Column('val', sa.String(255)))


@asyncio.coroutine
def create_table(engine):
    with (yield from engine) as conn:
        yield from conn.execute('DROP TABLE IF EXISTS tbl')
        yield from conn.execute('''CREATE TABLE tbl (
                                            id serial PRIMARY KEY,
                                            val varchar(255))''')


@asyncio.coroutine
def go():
    engine = yield from create_engine(user='aiopg',
                                      database='aiopg',
                                      host='127.0.0.1',
                                      password='passwd')

    yield from create_table(engine)
    with (yield from engine) as conn:
        yield from conn.execute(tbl.insert().values(val='abc'))

        res = yield from conn.execute(tbl.select())
        for row in res:
            print(row.id, row.val)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
