import asyncio
import datetime
import random

import sqlalchemy as sa

from aiopg.sa import create_engine

metadata = sa.MetaData()

users = sa.Table('users', metadata,
                 sa.Column('id', sa.Integer, primary_key=True),
                 sa.Column('name', sa.String(255)),
                 sa.Column('birthday', sa.DateTime))

emails = sa.Table('emails', metadata,
                  sa.Column('id', sa.Integer, primary_key=True),
                  sa.Column('user_id', None, sa.ForeignKey('users.id')),
                  sa.Column('email', sa.String(255), nullable=False),
                  sa.Column('private', sa.Boolean, nullable=False))


async def create_tables(conn):
    await conn.execute('DROP TABLE IF EXISTS emails')
    await conn.execute('DROP TABLE IF EXISTS users')
    await conn.execute('''CREATE TABLE users (
                                        id serial PRIMARY KEY,
                                        name varchar(255),
                                        birthday timestamp)''')
    await conn.execute('''CREATE TABLE emails (
                                id serial,
                                user_id int references users(id),
                                email varchar(253),
                                private bool)''')


names = {'Andrew', 'Bob', 'John', 'Vitaly', 'Alex', 'Lina', 'Olga',
         'Doug', 'Julia', 'Matt', 'Jessica', 'Nick', 'Dave', 'Martin',
         'Abbi', 'Eva', 'Lori', 'Rita', 'Rosa', 'Ivy', 'Clare', 'Maria',
         'Jenni', 'Margo', 'Anna'}


def gen_birthday():
    now = datetime.datetime.now()
    year = random.randint(now.year - 30, now.year - 20)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return datetime.datetime(year, month, day)


async def fill_data(conn):
    async with conn.begin():
        for name in random.sample(names, len(names)):
            uid = await conn.scalar(
                users.insert().values(name=name, birthday=gen_birthday()))
            emails_count = int(random.paretovariate(2))
            for num in random.sample(range(10000), emails_count):
                is_private = random.uniform(0, 1) < 0.8
                await conn.execute(emails.insert().values(
                    user_id=uid,
                    email='{}+{}@gmail.com'.format(name, num),
                    private=is_private))


async def count(conn):
    c1 = (await conn.scalar(users.count()))
    c2 = (await conn.scalar(emails.count()))
    print("Population consists of", c1, "people with",
          c2, "emails in total")
    join = sa.join(emails, users, users.c.id == emails.c.user_id)
    query = (sa.select([users.c.name])
             .select_from(join)
             .where(emails.c.private == False)  # noqa
             .group_by(users.c.name)
             .having(sa.func.count(emails.c.private) > 0))

    print("Users with public emails:")
    async for row in conn.execute(query):
        print(row.name)

    print()


async def show_julia(conn):
    print("Lookup for Julia:")
    join = sa.join(emails, users, users.c.id == emails.c.user_id)
    query = (sa.select([users, emails], use_labels=True)
             .select_from(join).where(users.c.name == 'Julia'))
    async for row in conn.execute(query):
        print(row.users_name, row.users_birthday,
              row.emails_email, row.emails_private)
    print()


async def ave_age(conn):
    query = (sa.select([sa.func.avg(sa.func.age(users.c.birthday))])
             .select_from(users))
    ave = (await conn.scalar(query))
    print("Average age of population is", ave,
          "or ~", int(ave.days / 365), "years")
    print()


async def go():
    engine = await create_engine(user='aiopg',
                                 database='aiopg',
                                 host='127.0.0.1',
                                 password='passwd')
    async with engine:
        async with engine.acquire() as conn:
            await create_tables(conn)
            await fill_data(conn)
            await count(conn)
            await show_julia(conn)
            await ave_age(conn)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
