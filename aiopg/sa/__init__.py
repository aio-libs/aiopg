"""Optional support for sqlalchemy.sql dynamic query generation."""

import asyncio

try:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
except ImportError:  # pragma: no cover
    raise ImportError('aiopg.sa requires sqlalchemy')


from .connection import SAConnection


dialect = PGDialect_psycopg2()
