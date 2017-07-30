from ..utils import IS_PYPY


if IS_PYPY:
    from psycopg2cffi.extras import *  # NOQA
else:
    from psycopg2.extras import *  # NOQA