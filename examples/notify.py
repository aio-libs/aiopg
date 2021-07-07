import asyncio

import psycopg2

import aiopg

dsn = "dbname=aiopg user=aiopg password=passwd host=127.0.0.1"


async def notify(conn):
    async with conn.cursor() as cur:
        for i in range(5):
            msg = f"message {i}"
            print("Send ->", msg)
            await cur.execute("NOTIFY channel, %s", (msg,))

        await cur.execute("NOTIFY channel, 'finish'")


async def listen(conn):
    async with conn.cursor() as cur:
        await cur.execute("LISTEN channel")
        while True:
            try:
                msg = await conn.notifies.get()
            except psycopg2.Error as ex:
                print("ERROR: ", ex)
                return
            if msg.payload == "finish":
                return
            else:
                print("Receive <-", msg.payload)


async def main():
    async with aiopg.connect(dsn) as listenConn:
        async with aiopg.create_pool(dsn) as notifyPool:
            async with notifyPool.acquire() as notifyConn:
                listener = listen(listenConn)
                notifier = notify(notifyConn)
                await asyncio.gather(listener, notifier)
    print("ALL DONE")


asyncio.run(main())
