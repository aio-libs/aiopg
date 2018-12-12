import asyncio
import aiopg

dsn = 'dbname=aiopg user=aiopg password=passwd host=127.0.0.1'


async def notify(conn):
    cur = await conn.cursor()
    try:
        for i in range(5):
            msg = "message {}".format(i)
            print('Send ->', msg)
            await cur.execute("NOTIFY channel, %s", (msg,))

        await cur.execute("NOTIFY channel, 'finish'")
    finally:
        cur.close()


async def listen(conn):
    cur = await conn.cursor()
    try:
        await cur.execute("LISTEN channel")
        while True:
            msg = await conn.notifies.get()
            if msg.payload == 'finish':
                return
            else:
                print('Receive <-', msg.payload)
    finally:
        cur.close()


async def main():
    pool = await aiopg.create_pool(dsn)
    with (await pool) as conn1:
        listener = listen(conn1)
        with (await pool) as conn2:
            notifier = notify(conn2)
            await asyncio.gather(listener, notifier)
    print("ALL DONE")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
