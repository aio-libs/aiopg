import asyncio
from functools import wraps
from io import StringIO

import sqlalchemy


def dump_sql(func, bind=False, dialect=None):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        out = StringIO()

        def dump(sql, *multiparams, **params):
            out.write('{};\n'.format(str(
                sql.compile(dialect=dump.dialect)).strip()))

        # There's no way to use given dialect here
        engine = sqlalchemy.create_engine('postgres://',
                                          strategy='mock',
                                          executor=dump)
        if dialect is None:
            dump.dialect = engine.dialect
        else:
            dump.dialect = dialect

        if bind:
            func(*args, bind=engine, **kwargs)
        else:
            func(engine, *args, **kwargs)

        return out.getvalue()
    return func_wrapper


def create_all_sql(metadata, dialect=None):
    return dump_sql(metadata.create_all, bind=True, dialect=dialect)()


def drop_all_sql(metadata, dialect=None):
    return dump_sql(metadata.create_all, bind=True, dialect=dialect)()


@asyncio.coroutine
def get_default_value(conn, dialect, default):
    ''' Return value for column default.

        Code has taken from SQLAlchemy.DefaultExecutionContext.
    '''

    if default is None:
        return None
    elif default.is_sequence:
        sql = "select nextval('%s')" % \
            dialect.identifier_preparer.format_sequence(default)
        return (yield from conn.scalar(sql))
    elif default.is_clause_element:
        return (yield from conn.scalar(default.arg))
    elif default.is_callable:
        return default.arg(dialect)
    else:
        return default.arg


@asyncio.coroutine
def get_table_defaults(conn, dialect, columns, excluded=None, method='insert'):
    ''' Return table defaults for method 'insert' or 'update'.

        exclude : these colums should be excluded from defaults
    '''
    if method == 'insert':
        attr = 'default'
    if method == 'update':
        attr = 'onupdate'

    result = {}

    for column in columns:
        if column.name in excluded:
            continue

        default = getattr(column, attr)
        if default is not None:
            val = yield from get_default_value(conn, dialect, default)
            result[column.name] = val

    return result


@asyncio.coroutine
def insert(conn, dialect, table, returning_id=False, **values):
    ''' Create Insert object for table with preset defaults.
    '''
    values = dict(values)
    defaults = yield from get_table_defaults(conn, dialect, table.c,
                                             excluded=values.keys(),
                                             method='insert',
                                             )
    values.update(defaults)

    if returning_id:
        insertion = table.insert(returning=[table.id])
    else:
        insertion = table.insert()

    return insertion.values(**values)


@asyncio.coroutine
def update(conn, dialect, table, **values):
    ''' Create Update object for table with preset defaults.
    '''
    defaults = yield from get_table_defaults(conn, dialect, table.c,
                                             excluded=values.keys(),
                                             method='update',
                                             )
    values.update(defaults)
    return table.update().values(**values)
