import re
import sys
from collections import namedtuple

from .connection import connect, Connection, TIMEOUT as DEFAULT_TIMEOUT
from .cursor import Cursor
from .pool import create_pool, Pool
from .transaction import IsolationLevel, Transaction

__all__ = ('connect', 'create_pool', 'Connection', 'Cursor', 'Pool',
           'version', 'version_info', 'DEFAULT_TIMEOUT', 'IsolationLevel',
           'Transaction')

__version__ = '0.15.0'

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
(connect, create_pool, Connection, Cursor, Pool, DEFAULT_TIMEOUT,
 IsolationLevel, Transaction)
