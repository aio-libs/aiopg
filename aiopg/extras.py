def _paginate(seq, page_size):
    """Consume an iterable and return it in chunks.

    Every chunk is at most `page_size`. Never return an empty chunk.
    """
    page = []
    count = len(seq)
    it = iter(seq)
    for s in range(count + 1):
        try:
            for i in range(page_size):
                page.append(next(it))
            yield page
            page = []
        except StopIteration:
            if page:
                yield page
            return


async def execute_batch(cur, sql, argslist, page_size=100):
    r"""Execute groups of statements in fewer server roundtrips.

    Execute *sql* several times, against all parameters set (sequences or
    mappings) found in *argslist*.

    The function is semantically similar to

    .. parsed-literal::

        *cur*\.\ `~cursor.executemany`\ (\ *sql*\ , *argslist*\ )

    but has a different implementation: Psycopg will join the statements into
    fewer multi-statement commands, each one containing at most *page_size*
    statements, resulting in a reduced number of server roundtrips.

    After the execution of the function the `cursor.rowcount` property will
    **not** contain a total result.

    """
    for page in _paginate(argslist, page_size=page_size):
        sqls = [cur.mogrify(sql, args) for args in page]
        await cur.execute(b";".join(sqls))
