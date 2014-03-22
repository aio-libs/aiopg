
class PgTulipExeption(Exception):
    """Base class for all pgtulip exception"""


class BusyConnection(PgTulipExeption):
    """Busy connection is attempted to be returned to the
    connection pool."""


class PsycoPollErrorException(PgTulipExeption):
    """There was a problem during connection polling. This value should
    actually never be returned: in case of poll error usually an exception
    containing the relevant details is raised.

    http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.POLL_ERROR
    """


class UnknownPollException(PgTulipExeption):
    """There was a problem during connection polling. This value should
    actually never be returned: in case of poll error usually an exception
    containing the relevant details is raised.

    http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.POLL_ERROR
    """