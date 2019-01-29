import sqlalchemy as sa

metadata = sa.MetaData()

tbl = sa.Table(
    'tbl', metadata,
    sa.Column('MyIDField', sa.Integer, key='id', primary_key=True),
    sa.Column('NaMe', sa.String(255), key='name', default='default name'),
)


async def inser_tbl(conn, **kwargs):
    await conn.execute(tbl.insert().values(**kwargs))
    row = await (await conn.execute(tbl.select())).first()

    for name, val in kwargs.items():
        assert row[name] == val

    await conn.execute(sa.delete(tbl))


async def create_table(conn):
    await conn.execute('DROP TABLE IF EXISTS tbl')
    await conn.execute(
        'CREATE TABLE tbl ('
        '"MyIDField" INTEGER NOT NULL, '
        '"NaMe" VARCHAR(255), '
        'PRIMARY KEY ("MyIDField"))'
    )


async def test_mmm(make_sa_connection):
    conn = await make_sa_connection()
    await create_table(conn)
    await inser_tbl(conn, id=1)
    await inser_tbl(conn, id=2, name='test')
