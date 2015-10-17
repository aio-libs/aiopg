import asyncio
try:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
    from sqlalchemy import sql
    from sqlalchemy import util
    from sqlalchemy.sql import sqltypes
except ImportError:  # pragma: no cover
    raise ImportError('aiopg.sa requires sqlalchemy')


class PGDialect_psycopg2_aio(PGDialect_psycopg2):
    """
    PGDialect that redefines some methods in asynchronous style
    """

    @asyncio.coroutine
    def has_schema(self, connection, schema):
        query = ("select nspname from pg_namespace "
                 "where lower(nspname)=:schema")
        cursor = yield from connection.execute(
            sql.text(
                query,
                bindparams=[
                    sql.bindparam(
                        'schema', util.text_type(schema.lower()),
                        type_=sqltypes.Unicode)]
            )
        )

        response = yield from cursor.first()
        return bool(response)

    @asyncio.coroutine
    def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in pg_class...
        if schema is None:
            cursor = yield from connection.execute(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where "
                    "pg_catalog.pg_table_is_visible(c.oid) "
                    "and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(table_name),
                                      type_=sqltypes.Unicode)]
                )
            )
        else:
            cursor = yield from connection.execute(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where n.nspname=:schema and "
                    "relname=:name",
                    bindparams=[
                        sql.bindparam('name',
                                      util.text_type(table_name),
                                      type_=sqltypes.Unicode),
                        sql.bindparam('schema',
                                      util.text_type(schema),
                                      type_=sqltypes.Unicode)]
                )
            )
        response = yield from cursor.first()
        return bool(response)

    @asyncio.coroutine
    def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            cursor = yield from connection.execute(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=current_schema() "
                    "and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(sequence_name),
                                      type_=sqltypes.Unicode)
                    ]
                )
            )
        else:
            cursor = yield from connection.execute(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=:schema and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(sequence_name),
                                      type_=sqltypes.Unicode),
                        sql.bindparam('schema',
                                      util.text_type(schema),
                                      type_=sqltypes.Unicode)
                    ]
                )
            )
        response = yield from cursor.first()
        return bool(response)

    @asyncio.coroutine
    def has_type(self, connection, type_name, schema=None):
        if schema is not None:
            query = """
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                AND t.typname = :typname
                AND n.nspname = :nspname
                )
                """
            query = sql.text(query)
        else:
            query = """
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t
                WHERE t.typname = :typname
                AND pg_type_is_visible(t.oid)
                )
                """
            query = sql.text(query)
        query = query.bindparams(
            sql.bindparam('typname',
                          util.text_type(type_name), type_=sqltypes.Unicode),
        )
        if schema is not None:
            query = query.bindparams(
                sql.bindparam('nspname',
                              util.text_type(schema), type_=sqltypes.Unicode),
            )
        cursor = yield from connection.execute(query)
        response = yield from cursor.scalar()
        return bool(response)
