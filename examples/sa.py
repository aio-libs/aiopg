import asyncio
from aiopg.sa import create_engine
import sqlalchemy as sa
import random
import datetime


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


@asyncio.coroutine
def create_tables(engine):
    with (yield from engine) as conn:
        yield from conn.execute('DROP TABLE IF EXISTS emails')
        yield from conn.execute('DROP TABLE IF EXISTS users')
        yield from conn.execute('''CREATE TABLE users (
                                            id serial PRIMARY KEY,
                                            name varchar(255),
                                            birthday timestamp)''')
        yield from conn.execute('''CREATE TABLE emails (
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


@asyncio.coroutine
def fill_data(engine):
    with (yield from engine) as conn:
        tr = yield from conn.begin()

        for name in random.sample(names, len(names)):
            uid = yield from conn.scalar(
                users.insert().values(name=name, birthday=gen_birthday()))
            emails_count = int(random.paretovariate(2))
            for num in random.sample(range(10000), emails_count):
                is_private = random.uniform(0, 1) < 0.8
                yield from conn.execute(emails.insert().values(
                    user_id=uid,
                    email='{}+{}@gmail.com'.format(name, num),
                    private=is_private))
        yield from tr.commit()


@asyncio.coroutine
def count(engine):
    with (yield from engine) as conn:
        c1 = (yield from conn.scalar(users.count()))
        c2 = (yield from conn.scalar(emails.count()))
        print("Population consists of", c1, "people with",
              c2, "emails in total")
        join = sa.join(emails, users, users.c.id == emails.c.user_id)
        query = (sa.select([users.c.name])
                 .select_from(join)
                 .where(emails.c.private == 0)
                 .group_by(users.c.name)
                 .having(sa.func.count(emails.c.private) > 0))

        print("Users with public emails:")
        ret = yield from conn.execute(query)
        for row in ret:
            print(row.name)

        print()


@asyncio.coroutine
def show_julia(engine):
    with (yield from engine) as conn:
        print("Lookup for Julia:")
        join = sa.join(emails, users, users.c.id == emails.c.user_id)
        query = (sa.select([users, emails], use_labels=True)
                 .select_from(join).where(users.c.name == 'Julia'))
        res = yield from conn.execute(query)
        for row in res:
            print(row.users_name, row.users_birthday,
                  row.emails_email, row.emails_private)
        print()


@asyncio.coroutine
def ave_age(engine):
    with (yield from engine) as conn:
        query = (sa.select([sa.func.avg(sa.func.age(users.c.birthday))])
                 .select_from(users))
        ave = (yield from conn.scalar(query))
        print("Average age of population is", ave,
              "or ~", int(ave.days / 365), "years")
        print()


@asyncio.coroutine
def go():
    engine = yield from create_engine(user='aiopg',
                                      database='aiopg',
                                      host='localhost',
                                      password='passwd')

    yield from create_tables(engine)
    yield from fill_data(engine)
    yield from count(engine)
    yield from show_julia(engine)
    yield from ave_age(engine)
    # print("Average age of our population", ave_age)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
