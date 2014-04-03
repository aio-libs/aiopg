class Error(Exception):
    """Base class for all aiopg exception."""


class UnknownPollError(Error):
    """There was a problem during connection polling. This value should
    actually never be returned: in case of poll error usually an exception
    containing the relevant details is raised.

    See also psycopg2.extensions.POLL_ERROR
    """


class ConnectionClosedError(Error):
    """Connection has been closed."""
