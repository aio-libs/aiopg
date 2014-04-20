"""Optional support for sqlalchemy.sql dynamic query generation."""

import asyncio

try:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
except ImportError:  # pragma: no cover
    raise ImportError('aiopg.sa requires sqlalchemy')


from .connection import SAConnection
from .exc import (Error, ArgumentError, InvalidRequestError,
                  NoSuchColumnError, ResourceClosedError)


dialect = PGDialect_psycopg2()
dialect.implicit_returning = True
dialect.supports_native_enum = True
dialect.supports_smallserial = True  # 9.2+
dialect._backslash_escapes = False
dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
