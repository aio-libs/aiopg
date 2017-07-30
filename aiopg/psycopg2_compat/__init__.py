from ..utils import IS_PYPY


if IS_PYPY:
    from psycopg2cffi import *  # NOQA
else:
    from psycopg2 import *  # NOQA
