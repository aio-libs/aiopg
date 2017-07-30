from ..utils import IS_PYPY


if IS_PYPY:
    from psycopg2cffi.extensions import *  # NOQA
else:
    from psycopg2.extensions import *  # NOQA
