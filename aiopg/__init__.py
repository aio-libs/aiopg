import platform
import re
import sys
from collections import namedtuple


PY_IMPL = platform.python_implementation()

# map psycopg2cffi to psycopg2, so any code
# that imports psycopg2 will use psycopg2cffi
if PY_IMPL == 'PyPy':
    from psycopg2cffi import compat
    compat.register()


from .connection import connect, Connection, TIMEOUT as DEFAULT_TIMEOUT
from .cursor import Cursor
from .pool import create_pool, Pool


__all__ = ('connect', 'create_pool', 'Connection', 'Cursor', 'Pool',
           'version', 'version_info', 'DEFAULT_TIMEOUT')

__version__ = '0.13.0'

version = __version__ + ' , Python ' + sys.version


VersionInfo = namedtuple('VersionInfo',
                         'major minor micro releaselevel serial')


def _parse_version(ver):
    RE = (r'^(?P<major>\d+)\.(?P<minor>\d+)\.'
          '(?P<micro>\d+)((?P<releaselevel>[a-z]+)(?P<serial>\d+)?)?$')
    match = re.match(RE, ver)
    try:
        major = int(match.group('major'))
        minor = int(match.group('minor'))
        micro = int(match.group('micro'))
        levels = {'c': 'candidate',
                  'a': 'alpha',
                  'b': 'beta',
                  None: 'final'}
        releaselevel = levels[match.group('releaselevel')]
        serial = int(match.group('serial')) if match.group('serial') else 0
        return VersionInfo(major, minor, micro, releaselevel, serial)
    except Exception:
        raise ImportError("Invalid package version {}".format(ver))


version_info = _parse_version(__version__)


# make pyflakes happy
(connect, create_pool, Connection, Cursor, Pool, DEFAULT_TIMEOUT)
