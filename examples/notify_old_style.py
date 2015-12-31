import asyncio
import aiopg

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


@asyncio.coroutine
def notify(conn):
    cur = yield from conn.cursor()
    try:
        for i in range(5):
            msg = "message {}".format(i)
            print('Send ->', msg)
            yield from cur.execute("NOTIFY channel, '{}'".format(msg))

        yield from cur.execute("NOTIFY channel, 'finish'")
    finally:
        cur.close()


@asyncio.coroutine
def listen(conn):
    cur = yield from conn.cursor()
    try:
        yield from cur.execute("LISTEN channel")
        while True:
            msg = yield from conn.notifies.get()
            if msg.payload == 'finish':
                return
            else:
                print('Receive <-', msg.payload)
    finally:
        cur.close()


@asyncio.coroutine
def main():
    pool = yield from aiopg.create_pool(dsn)
    with (yield from pool) as conn1:
        listener = listen(conn1)
        with (yield from pool) as conn2:
            notifier = notify(conn2)
            yield from asyncio.gather(listener, notifier)
    print("ALL DONE")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
