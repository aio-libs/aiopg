import abc
import asyncio
import contextlib
import datetime
import enum
import errno
import functools
import platform
import select
import selectors
import sys
import traceback
import uuid
import warnings
import weakref
from collections.abc import Mapping
from types import MappingProxyType, TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Hashable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from .log import logger
from .utils import (
    ClosableQueue,
    _ContextManager,
    create_completed_future,
    execute_or_await,
    get_running_loop,
)

ReplicationSlotType = Union[Literal[87654321], Literal[12345678]]

TIMEOUT = 60.0

# Windows specific error code, not in errno for some reason, and doesnt map
# to OSError.errno EBADF
WSAENOTSOCK = 10038


def connect(
    dsn: Optional[str] = None,
    *,
    timeout: float = TIMEOUT,
    enable_json: bool = True,
    enable_hstore: bool = True,
    enable_uuid: bool = True,
    echo: bool = False,
    **kwargs: Any,
) -> _ContextManager["Connection"]:
    """A factory for connecting to PostgreSQL.

    The coroutine accepts all parameters that psycopg2.connect() does
    plus optional keyword-only `timeout` parameters.

    Returns instantiated Connection object.

    """
    connection = Connection(
        dsn,
        timeout,
        bool(echo),
        enable_hstore=enable_hstore,
        enable_uuid=enable_uuid,
        enable_json=enable_json,
        **kwargs,
    )
    return _ContextManager[Connection](connection, disconnect)  # type: ignore


async def disconnect(c: "Connection") -> None:
    await c.close()


def _is_bad_descriptor_error(os_error: OSError) -> bool:
    if platform.system() == "Windows":  # pragma: no cover
        winerror = int(getattr(os_error, "winerror", 0))
        return winerror == WSAENOTSOCK
    return os_error.errno == errno.EBADF


class IsolationCompiler(abc.ABC):
    __slots__ = ("_isolation_level", "_readonly", "_deferrable")

    def __init__(
        self, isolation_level: Optional[str], readonly: bool, deferrable: bool
    ):
        self._isolation_level = isolation_level
        self._readonly = readonly
        self._deferrable = deferrable

    @property
    def name(self) -> str:
        return self._isolation_level or "Unknown"

    def savepoint(self, unique_id: str) -> str:
        return f"SAVEPOINT {unique_id}"

    def release_savepoint(self, unique_id: str) -> str:
        return f"RELEASE SAVEPOINT {unique_id}"

    def rollback_savepoint(self, unique_id: str) -> str:
        return f"ROLLBACK TO SAVEPOINT {unique_id}"

    def commit(self) -> str:
        return "COMMIT"

    def rollback(self) -> str:
        return "ROLLBACK"

    def begin(self) -> str:
        query = "BEGIN"
        if self._isolation_level is not None:
            query += f" ISOLATION LEVEL {self._isolation_level.upper()}"

        if self._readonly:
            query += " READ ONLY"

        if self._deferrable:
            query += " DEFERRABLE"

        return query

    def __repr__(self) -> str:
        return self.name


class ReadCommittedCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__("Read committed", readonly, deferrable)


class RepeatableReadCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__("Repeatable read", readonly, deferrable)


class SerializableCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__("Serializable", readonly, deferrable)


class DefaultCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__(None, readonly, deferrable)

    @property
    def name(self) -> str:
        return "Default"


class IsolationLevel(enum.Enum):
    serializable = SerializableCompiler
    repeatable_read = RepeatableReadCompiler
    read_committed = ReadCommittedCompiler
    default = DefaultCompiler

    def __call__(self, readonly: bool, deferrable: bool) -> IsolationCompiler:
        return self.value(readonly, deferrable)  # type: ignore


async def _release_savepoint(t: "Transaction") -> None:
    await t.release_savepoint()


async def _rollback_savepoint(t: "Transaction") -> None:
    await t.rollback_savepoint()


class Transaction:
    __slots__ = ("_cursor", "_is_begin", "_isolation", "_unique_id")

    def __init__(
        self,
        cursor: "Cursor",
        isolation_level: Callable[[bool, bool], IsolationCompiler],
        readonly: bool = False,
        deferrable: bool = False,
    ):
        self._cursor = cursor
        self._is_begin = False
        self._unique_id: Optional[str] = None
        self._isolation = isolation_level(readonly, deferrable)

    @property
    def is_begin(self) -> bool:
        return self._is_begin

    async def begin(self) -> "Transaction":
        if self._is_begin:
            raise psycopg2.ProgrammingError(
                "You are trying to open a new transaction, use the save point"
            )
        self._is_begin = True
        await self._cursor.execute(self._isolation.begin())
        return self

    async def commit(self) -> None:
        self._check_commit_rollback()
        await self._cursor.execute(self._isolation.commit())
        self._is_begin = False

    async def rollback(self) -> None:
        self._check_commit_rollback()
        if not self._cursor.closed:
            await self._cursor.execute(self._isolation.rollback())
        self._is_begin = False

    async def rollback_savepoint(self) -> None:
        self._check_release_rollback()
        if not self._cursor.closed:
            await self._cursor.execute(
                self._isolation.rollback_savepoint(
                    self._unique_id  # type: ignore
                )
            )
        self._unique_id = None

    async def release_savepoint(self) -> None:
        self._check_release_rollback()
        await self._cursor.execute(
            self._isolation.release_savepoint(self._unique_id)  # type: ignore
        )
        self._unique_id = None

    async def savepoint(self) -> "Transaction":
        self._check_commit_rollback()
        if self._unique_id is not None:
            raise psycopg2.ProgrammingError("You do not shut down savepoint")

        self._unique_id = f"s{uuid.uuid1().hex}"
        await self._cursor.execute(self._isolation.savepoint(self._unique_id))

        return self

    def point(self) -> _ContextManager["Transaction"]:
        return _ContextManager[Transaction](
            self.savepoint(),
            _release_savepoint,
            _rollback_savepoint,
        )

    def _check_commit_rollback(self) -> None:
        if not self._is_begin:
            raise psycopg2.ProgrammingError(
                "You are trying to commit " "the transaction does not open"
            )

    def _check_release_rollback(self) -> None:
        self._check_commit_rollback()
        if self._unique_id is None:
            raise psycopg2.ProgrammingError("You do not start savepoint")

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"transaction={self._isolation} id={id(self):#x}>"
        )

    def __del__(self) -> None:
        if self._is_begin:
            warnings.warn(
                f"You have not closed transaction {self!r}", ResourceWarning
            )

        if self._unique_id is not None:
            warnings.warn(
                f"You have not closed savepoint {self!r}", ResourceWarning
            )

    async def __aenter__(self) -> "Transaction":
        return await self.begin()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()


async def _commit_transaction(t: Transaction) -> None:
    await t.commit()


async def _rollback_transaction(t: Transaction) -> None:
    await t.rollback()


class Cursor:
    def __init__(
        self,
        conn: "Connection",
        impl: Any,
        timeout: float,
        echo: bool,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        self._conn = conn
        self._impl = impl
        self._timeout = timeout
        self._echo = echo
        self._transaction = Transaction(
            self, isolation_level or IsolationLevel.default
        )

    @property
    def echo(self) -> bool:
        """Return echo mode status."""
        return self._echo

    @property
    def description(self) -> Optional[Sequence[Any]]:
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences is a collections.namedtuple containing
        information describing one result column:

        0.  name: the name of the column returned.
        1.  type_code: the PostgreSQL OID of the column.
        2.  display_size: the actual length of the column in bytes.
        3.  internal_size: the size in bytes of the column associated to
            this column on the server.
        4.  precision: total number of significant digits in columns of
            type NUMERIC. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type NUMERIC. None for other types.
        6.  null_ok: always None as not easy to retrieve from the libpq.

        This attribute will be None for operations that do not
        return rows or if the cursor has not had an operation invoked
        via the execute() method yet.

        """
        return self._impl.description  # type: ignore

    def close(self) -> None:
        """Close the cursor now."""
        if not self.closed:
            self._impl.close()

    @property
    def closed(self) -> bool:
        """Read-only boolean attribute: specifies if the cursor is closed."""
        return self._impl.closed  # type: ignore

    @property
    def connection(self) -> "Connection":
        """Read-only attribute returning a reference to the `Connection`."""
        return self._conn

    @property
    def raw(self) -> Any:
        """Underlying psycopg cursor object, readonly"""
        return self._impl

    @property
    def name(self) -> str:
        # Not supported
        return self._impl.name  # type: ignore

    @property
    def scrollable(self) -> Optional[bool]:
        # Not supported
        return self._impl.scrollable  # type: ignore

    @scrollable.setter
    def scrollable(self, val: bool) -> None:
        # Not supported
        self._impl.scrollable = val

    @property
    def withhold(self) -> bool:
        # Not supported
        return self._impl.withhold  # type: ignore

    @withhold.setter
    def withhold(self, val: bool) -> None:
        # Not supported
        self._impl.withhold = val

    async def execute(
        self,
        operation: str,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be
        bound to variables in the operation.  Variables are specified
        either with positional %s or named %({name})s placeholders.

        """
        if timeout is None:
            timeout = self._timeout
        waiter = self._conn._create_waiter("cursor.execute")
        if self._echo:
            logger.info(operation)
            logger.info("%r", parameters)
        try:
            self._impl.execute(operation, parameters)
        except BaseException:
            self._conn._waiter = None
            raise
        try:
            await self._conn._poll(waiter, timeout)
        except asyncio.TimeoutError:
            self._impl.close()
            raise

    async def executemany(self, *args: Any, **kwargs: Any) -> None:
        # Not supported
        raise psycopg2.ProgrammingError(
            "executemany cannot be used in asynchronous mode"
        )

    async def callproc(
        self,
        procname: str,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each
        argument that the procedure expects. The result of the call is
        returned as modified copy of the input sequence. Input
        parameters are left untouched, output and input/output
        parameters replaced with possibly new values.

        """
        if timeout is None:
            timeout = self._timeout
        waiter = self._conn._create_waiter("cursor.callproc")
        if self._echo:
            logger.info("CALL %s", procname)
            logger.info("%r", parameters)
        try:
            self._impl.callproc(procname, parameters)
        except BaseException:
            self._conn._waiter = None
            raise
        else:
            await self._conn._poll(waiter, timeout)

    def begin(self) -> _ContextManager[Transaction]:
        return _ContextManager[Transaction](
            self._transaction.begin(),
            _commit_transaction,
            _rollback_transaction,
        )

    def begin_nested(self) -> _ContextManager[Transaction]:
        if self._transaction.is_begin:
            return self._transaction.point()

        return _ContextManager[Transaction](
            self._transaction.begin(),
            _commit_transaction,
            _rollback_transaction,
        )

    def mogrify(self, operation: str, parameters: Any = None) -> bytes:
        """Return a query string after arguments binding.

        The byte string returned is exactly the one that would be sent to
        the database running the .execute() method or similar.

        """
        ret = self._impl.mogrify(operation, parameters)
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side mogrify"
        return ret  # type: ignore

    async def setinputsizes(self, sizes: int) -> None:
        """This method is exposed in compliance with the DBAPI.

        It currently does nothing but it is safe to call it.

        """
        self._impl.setinputsizes(sizes)

    async def fetchone(self) -> Any:
        """Fetch the next row of a query result set.

        Returns a single tuple, or None when no more data is
        available.

        """
        ret = self._impl.fetchone()
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"
        return ret

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        """Fetch the next set of rows of a query result.

        Returns a list of tuples. An empty list is returned when no
        more rows are available.

        The number of rows to fetch per call is specified by the
        parameter.  If it is not given, the cursor's .arraysize
        determines the number of rows to be fetched. The method should
        try to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number
        of rows not being available, fewer rows may be returned.

        """
        if size is None:
            size = self._impl.arraysize
        ret = self._impl.fetchmany(size)
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"
        return ret  # type: ignore

    async def fetchall(self) -> List[Any]:
        """Fetch all (remaining) rows of a query result.

        Returns them as a list of tuples.  An empty list is returned
        if there is no more record to fetch.

        """
        ret = self._impl.fetchall()
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"
        return ret  # type: ignore

    async def scroll(self, value: int, mode: str = "relative") -> None:
        """Scroll to a new position according to mode.

        If mode is relative (default), value is taken as offset
        to the current position in the result set, if set to
        absolute, value states an absolute target position.

        """
        self._impl.scroll(value, mode)
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"

    @property
    def arraysize(self) -> int:
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        return self._impl.arraysize  # type: ignore

    @arraysize.setter
    def arraysize(self, val: int) -> None:
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        self._impl.arraysize = val

    @property
    def itersize(self) -> int:
        # Not supported
        return self._impl.itersize  # type: ignore

    @itersize.setter
    def itersize(self, val: int) -> None:
        # Not supported
        self._impl.itersize = val

    @property
    def rowcount(self) -> int:
        """Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.

        """
        return self._impl.rowcount  # type: ignore

    @property
    def rownumber(self) -> int:
        """Row index.

        This read-only attribute provides the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined."""

        return self._impl.rownumber  # type: ignore

    @property
    def lastrowid(self) -> int:
        """OID of the last inserted row.

        This read-only attribute provides the OID of the last row
        inserted by the cursor. If the table wasn't created with OID
        support or the last operation is not a single record insert,
        the attribute is set to None.

        """
        return self._impl.lastrowid  # type: ignore

    @property
    def query(self) -> Optional[str]:
        """The last executed query string.

        Read-only attribute containing the body of the last query sent
        to the backend (including bound arguments) as bytes
        string. None if no query has been executed yet.

        """
        return self._impl.query  # type: ignore

    @property
    def statusmessage(self) -> str:
        """the message returned by the last command."""
        return self._impl.statusmessage  # type: ignore

    @property
    def tzinfo_factory(self) -> datetime.tzinfo:
        """The time zone factory used to handle data types such as
        `TIMESTAMP WITH TIME ZONE`.
        """
        return self._impl.tzinfo_factory  # type: ignore

    @tzinfo_factory.setter
    def tzinfo_factory(self, val: datetime.tzinfo) -> None:
        """The time zone factory used to handle data types such as
        `TIMESTAMP WITH TIME ZONE`.
        """
        self._impl.tzinfo_factory = val

    async def nextset(self) -> None:
        # Not supported
        self._impl.nextset()  # raises psycopg2.NotSupportedError

    async def setoutputsize(
        self, size: int, column: Optional[int] = None
    ) -> None:
        # Does nothing
        self._impl.setoutputsize(size, column)

    async def copy_from(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "copy_from cannot be used in asynchronous mode"
        )

    async def copy_to(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "copy_to cannot be used in asynchronous mode"
        )

    async def copy_expert(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "copy_expert cannot be used in asynchronous mode"
        )

    async def create_replication_slot(
        self,
        slot_name: str,
        slot_type: Optional[ReplicationSlotType] = None,
        output_plugin: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Create a streaming replication slot.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "create_replication_slot cannot be used "
            "with a non-replication cursor"
        )

    async def drop_replication_slot(
        self,
        slot_name: str,
        timeout: Optional[float] = None,
    ) -> None:
        """Drop a streaming replication slot.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "drop_replication_slot cannot be used "
            "with a non-replication cursor"
        )

    async def start_replication(
        self,
        slot_name: Optional[str] = None,
        slot_type: Optional[ReplicationSlotType] = None,
        start_lsn: Union[int, str] = 0,
        timeline: int = 0,
        options: Optional[Dict[Hashable, Any]] = None,
        decode: bool = False,
        status_interval: float = 10,
        timeout: Optional[float] = None,
    ) -> None:
        """Start replication on the replication connection.
        After this, no other commands can be performed on the cursor except
        receiving replication messages and sending feedback packets.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "start_replication cannot be used with a non-replication cursor"
        )

    async def start_replication_expert(
        self,
        command: str,
        decode: bool = False,
        status_interval: float = 10,
        timeout: Optional[float] = None,
    ) -> None:
        """Start replication on the replication connection using the
        raw START_REPLICATION command provided by PostgreSQL.
        After this, no other commands can be performed on the cursor except
        receiving replication messages and sending feedback packets.

        For advanced usage only, in most cases, `start_replication()`
        will be sufficient.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "start_replication_expert cannot be used with "
            "a non-replication cursor"
        )

    async def send_feedback(
        self,
        write_lsn: int = 0,
        flush_lsn: int = 0,
        apply_lsn: int = 0,
        reply: bool = False,
        force: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """Report to the server that all messages up to a certain LSN
        position have been processed on the client and may be discarded on
        the server.

        If the `reply` or `force` parameters are not set, this method
        will only update internal psycopg2 structures without sending
        the feedback message to the server.

        The feedback message will be automatically sent the next time
        `read_message()` is called if `status_interval` timeout is reached
        by then. Methods such as `message_stream()` and `consume_stream()`
        handle this transparently under the hood without requiring any client
        interference.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "send_feedback cannot be used with a non-replication cursor"
        )

    async def message_stream(
        self,
    ) -> AsyncGenerator["ReplicationMessage", None]:
        """Keep yielding replication messages received from the server
        and also send feedback packets in a timely manner when
        the `status_interval` timeout is reached.

        The client must also confirm every yielded and processed message
        by calling the `send_feedback()` method on the corresponding
        replication cursor (every `ReplicationMessage` instance has a
        reference to it).

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "message_stream cannot be used with a non-replication cursor"
        )
        # noinspection PyUnreachableCode
        yield

    async def consume_stream(
        self,
        consumer: Union[
            Callable[["ReplicationMessage"], Coroutine[Any, Any, Any]],
            Callable[["ReplicationMessage"], Any],
        ],
    ) -> None:
        """Enter an endless loop reading messages from the server and
        passing them to a client-provided `consumer` callback one at a time.
        The `consumer` callback can be either a coroutine function
        or a callable receiving only one parameter -
        a `ReplicationMessage` instance.
        Also, send feedback packets in a timely manner when
        the `status_interval` timeout is reached.

        The client must also confirm every received and processed message
        withinthe callback by calling the `send_feedback()` method
        on the corresponding replication cursor (every `ReplicationMessage`
        instance has a reference to it).

        In order to make this method break out of the loop and return,
        the provided `consumer` callback can throw a
        psycopg2.extras.StopReplication exception.
        Any unhandled exception will make it break out of the loop as well.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "consume_stream cannot be used with a non-replication cursor"
        )

    async def read_message(
        self,
        timeout: Optional[float] = None,
    ) -> Optional["ReplicationMessage"]:
        """Try to read the next replication message from the server.
        When called, also automatically send feedback packets to the server if
        `status_interval` timeout is reached.

        Block until either a message from the server is received,
        the feedback status interval is reached, or an optionally provided
        timeout has elapsed. In the latter case, return None, otherwise,
        return the received message wrapped in a `ReplicationMessage` instance.

        The client must confirm every processed message by calling the
        `send_feedback()` method on the corresponding replication cursor
        (every `ReplicationMessage` instance has a reference to it).

        Basically, this method has to be called in a loop to keep up with the
        flow of messages coming from the server and also to periodically
        send feedback packets, especially if there are no messages to be
        received.

        This is why methods such as `message_stream()` or `consume_stream()`
        are preferred over this one because they will keep up with the flow
        of messages without explicitly requiring the client to call them over
        and over again. They will also automatically handle sending feedback
        packets in a timely manner each time the `status_interval` timeout
        is reached.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "read_message cannot be used with a non-replication cursor"
        )

    @property
    def timeout(self) -> float:
        """Return default timeout for cursor operations."""
        return self._timeout

    @property
    def wal_end(self) -> int:
        """Return the LSN position of the current end of WAL on the server
        at the moment when last data or keepalive message was received
        from the server.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "wal_end is not available on non-replication cursors"
        )

    @property
    def feedback_timestamp(self) -> datetime.datetime:
        """Return a datetime object representing the timestamp at the moment
        when the last feedback message was sent to the server.

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "feedback_timestamp is not available on non-replication cursors"
        )

    @property
    def io_timestamp(self) -> datetime.datetime:
        """Return a datetime object representing the timestamp at the moment
        of last communication with the server
        (a data or keepalive message in either direction).

        NOTE: only available with psycopg2's logical or
        physical replication connection factories.

        """
        raise psycopg2.ProgrammingError(
            "io_timestamp is not available on non-replication cursors"
        )

    def __aiter__(self) -> "Cursor":
        return self

    async def __anext__(self) -> Any:
        ret = await self.fetchone()
        if ret is not None:
            return ret
        raise StopAsyncIteration

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"<"
            f"{type(self).__module__}::{type(self).__name__} "
            f"name={self.name}, "
            f"closed={self.closed}"
            f">"
        )


class ReplicationMessage:
    """A streaming replication protocol message.

    This is just a wrapper around psycopg2's `ReplicationMessage` class
    modified to expose the asynchronous `_ReplicationCursor` implementation
    through the `cursor` property.

    As the attributes of the original psycopg2 message class are defined
    as read-only at the C level API, we keep them as such by proxying
    them through properties and not instance attributes.

    """

    __slots__ = ("_impl", "_cursor")

    def __init__(
        self,
        impl: psycopg2.extras.ReplicationMessage,
        cursor: "_ReplicationCursor",
    ) -> None:
        self._impl = impl
        self._cursor = cursor

    @property
    def payload(self) -> Any:
        """Return the actual data received from the server.

        The data type depends on the replication mode used,
        the `decode` option passed in to `start_replication()` or the logical
        decoding output plugin used in case of logical replication.
        """
        return self._impl.payload

    @property
    def data_size(self) -> int:
        """Return the raw size of the message payload.

        When physical replication is used, this number corresponds
        to the actual WAL segment size in bytes stored within
        the server's write-ahead log.

        When using logical replication, this number represents the WAL entry
        size in bytes after it has been transformed by any sort of logical
        decoding plugin (before possible unicode conversion) and usually
        differs from the raw WAL entry size stored on disk.

        """
        return cast(int, self._impl.data_size)

    @property
    def data_start(self) -> int:
        """Return the LSN position of the start of the message."""
        return cast(int, self._impl.data_start)

    @property
    def wal_end(self) -> int:
        """Return the LSN position of the current end of WAL on the server."""
        return cast(int, self._impl.wal_end)

    @property
    def send_time(self) -> datetime.datetime:
        """Return a datetime object representing the server timestamp
        at the moment when the message was sent.
        """
        return cast(datetime.datetime, self._impl.send_time)

    @property
    def cursor(self) -> "_ReplicationCursor":
        """Return a reference to the corresponding asynchronous
        `_ReplicationCursor` object.
        """
        return self._cursor

    def _xlog_fmt(self, xlog: int) -> str:
        return "{:x}/{:x}".format(xlog >> 32, xlog & 0xFFFFFFFF)

    def __repr__(self) -> str:
        return (
            f"<"
            f"{type(self).__module__}::{type(self).__name__} "
            f"data_size={self.data_size}, "
            f"data_start={self._xlog_fmt(self.data_start)}, "
            f"wal_end={self._xlog_fmt(self.wal_end)}, "
            f"send_time={self._impl.__repr__().split(':')[-1][1:-1]}"
            f">"
        )


class _ReplicationCursor(Cursor):
    """A cursor used for communication on replication connections.

    To use it, you have to provide a `connection_factory` option when
    establishing a new connection to the server and optionally a
    psycopg2.extras.ReplicationCursor `cursor_factory` when creating a cursor
    on this connection.
    There are two connection factories available provided by psycopg2, each one
    corresponding to a specific replication mode:
        - psycopg2.extras.LogicalReplicationConnection to open a special type
          of connection that is used for logical replication;
        - psycopg2.extras.PhysicalReplicationConnection to open a special type
          of connection that is used for physical replication.

    """

    # a mapping of streaming replication protocol-specific commands
    # to methods that are used to log these commands when 'echo' mode
    # is enabled on the cursor
    _repl_command_to_repr_method = MappingProxyType(
        {
            "create_replication_slot": "_create_replication_slot_repr",
            "drop_replication_slot": "_drop_replication_slot_repr",
            "start_replication": "_start_replication_repr",
            "start_replication_expert": "_start_replication_expert_repr",
        }
    )

    def __init__(
        self,
        conn: "Connection",
        impl: psycopg2.extras.ReplicationCursor,
        timeout: float,
        echo: bool,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> None:
        super().__init__(conn, impl, timeout, echo, isolation_level)
        # time between feedback packets sent to the server
        # set after either `start_replication()`
        # or `start_replication_expert()` methods get called
        self._status_interval: Optional[float] = None

    async def create_replication_slot(
        self,
        slot_name: str,
        slot_type: Optional[ReplicationSlotType] = None,
        output_plugin: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Create a streaming replication slot."""
        await self._execute_replication_command(
            "create_replication_slot",
            timeout,
            slot_name=slot_name,
            slot_type=slot_type,
            output_plugin=output_plugin,
        )

    async def drop_replication_slot(
        self,
        slot_name: str,
        timeout: Optional[float] = None,
    ) -> None:
        """Drop a streaming replication slot."""
        await self._execute_replication_command(
            "drop_replication_slot",
            timeout,
            slot_name=slot_name,
        )

    async def start_replication(
        self,
        slot_name: Optional[str] = None,
        slot_type: Optional[ReplicationSlotType] = None,
        start_lsn: Union[int, str] = 0,
        timeline: int = 0,
        options: Optional[Dict[Hashable, Any]] = None,
        decode: bool = False,
        status_interval: float = 10,
        timeout: Optional[float] = None,
    ) -> None:
        """Start replication on the replication connection.
        After this, no other commands can be performed on the cursor except
        receiving replication messages and sending feedback packets.
        """
        if status_interval < 1:
            raise psycopg2.ProgrammingError(
                "status_interval must be >= 1 (sec)"
            )

        self._status_interval = status_interval
        await self._execute_replication_command(
            "start_replication",
            timeout,
            slot_name=slot_name,
            slot_type=slot_type,
            start_lsn=start_lsn,
            timeline=timeline,
            options=options,
            decode=decode,
            status_interval=status_interval,
        )

    async def start_replication_expert(
        self,
        command: str,
        decode: bool = False,
        status_interval: float = 10,
        timeout: Optional[float] = None,
    ) -> None:
        """Start replication on the replication connection using the
        START_REPLICATION command provided by postgres.
        After this, no other commands can be performed on the cursor except
        receiving replication messages and sending feedback packets.

        For advanced usage only, in most cases, `start_replication()`
        will be sufficient.

        """
        if status_interval < 1:
            raise psycopg2.ProgrammingError(
                "status_interval must be >= 1 (sec)"
            )

        self._status_interval = status_interval
        await self._execute_replication_command(
            "start_replication_expert",
            timeout,
            command=command,
            decode=decode,
            status_interval=status_interval,
        )

    async def send_feedback(
        self,
        write_lsn: int = 0,
        flush_lsn: int = 0,
        apply_lsn: int = 0,
        reply: bool = False,
        force: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """Report to the server that all messages up to a certain LSN
        position have been processed on the client and may be discarded
        on the server.

        If the `reply` or `force` parameters are not set, this method
        will only update internal psycopg2 structures without sending
        the feedback message to the server.

        The feedback message will be sent automatically the next time
        `read_message()` is called if `status_interval` timeout is reached
        by then. Methods such as `message_stream()` and `consume_stream()`
        handle this transparently under the hood without requiring any client
        interference.

        """
        await self._execute_replication_command(
            "send_feedback",
            timeout,
            write_lsn=write_lsn,
            flush_lsn=flush_lsn,
            apply_lsn=apply_lsn,
            reply=reply,
            force=force,
        )

    async def message_stream(self) -> AsyncGenerator[ReplicationMessage, None]:
        """Keep yielding replication messages received from the server
        and also send feedback packets in a timely manner when
        the `status_interval` timeout is reached.

        The client must also confirm every yielded and processed message
        by calling the `send_feedback()` method on the corresponding
        replication cursor (every `ReplicationMessage` instance has a
        reference to it).

        """
        while True:
            msg = await self.read_message()
            if msg:
                yield msg

    async def consume_stream(
        self,
        consumer: Union[
            Callable[[ReplicationMessage], Coroutine[Any, Any, Any]],
            Callable[[ReplicationMessage], Any],
        ],
    ) -> None:
        """Enter an endless loop reading messages from the server and
        passing them to a client-provided `consumer` callback one at a time.
        The `consumer` callback can be either a coroutine function
        or a callable receiving only one parameter -
        a `ReplicationMessage` instance.
        Also, send feedback packets in a timely manner when
        the `status_interval` timeout is reached.

        The client must also confirm every received and processed message
        within the callback by calling the `send_feedback()` method
        on the corresponding replication cursor (every `ReplicationMessage`
        instance has a reference to it).

        In order to make this method break out of the loop and return,
        the provided `consumer` callback can throw a
        psycopg2.extras.StopReplication exception.
        Any unhandled exception will make it break out of the loop as well.

        """
        while True:
            msg = await self.read_message()
            if msg is not None:
                try:
                    await execute_or_await(consumer, msg)
                except psycopg2.extras.StopReplication:
                    return

    async def read_message(
        self,
        timeout: Optional[float] = None,
    ) -> Optional[ReplicationMessage]:
        """Try to read the next replication message from the server.
        When called, also automatically send feedback packets to the server if
        `status_interval` timeout is reached.

        Block until either a message from the server is received,
        the feedback status interval is reached, or an optionally provided
        timeout has elapsed. In the latter case, return None, otherwise,
        return the received message wrapped in a `ReplicationMessage` instance.

        The client must confirm every processed message by calling the
        `send_feedback()` method on the corresponding replication cursor
        (every `ReplicationMessage` instance has a reference to it).

        Basically, this method has to be called in a loop to keep up with the
        flow of messages coming from the server and also to periodically
        send feedback packets, especially if there are no messages to be
        received.

        This is why methods such as `message_stream()` or `consume_stream()`
        are preferred over this one because they will keep up with the flow
        of messages without explicitly requiring the client to call them over
        and over again. They will also automatically handle sending feedback
        packets in a timely manner.

        """
        # try to receive a replication message immediately
        # and also send a feedback packet to the server if
        # `status_interval` timeout is reached
        msg = self._impl.read_message()
        if msg is not None:
            return ReplicationMessage(msg, self)

        # otherwise, let the event loop poll the connection for incoming
        # replication messages
        fut = self._conn._loop.create_future()
        fd = cast(int, self._impl.fileno())

        self._conn._loop.add_reader(fd, self._read_message, fut)
        fut.add_done_callback(functools.partial(self._read_message_done, fd))

        try:
            return await asyncio.wait_for(
                fut,
                timeout or self._status_interval,
            )
        except asyncio.TimeoutError:
            return None

    def _read_message(self, fut: "asyncio.Future[ReplicationMessage]") -> None:
        # is added as an I/O callback if `read_message()` cannot return a
        # replication message immediately
        # replaces the connection's original `_ready()` callback
        # while the client is waiting for an incoming message
        # the connection's original `_ready()` callback will be restored after
        # this I/O operation either completes or times out
        if fut.done():
            return
        try:
            msg = self._impl.read_message()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            # read-event might have been triggered by a server keepalive
            # message
            # if no replication message was received,
            # try again next time
            if msg is not None:
                fut.set_result(ReplicationMessage(msg, self))

    def _read_message_done(
        self,
        fd: int,
        fut: "asyncio.Future[ReplicationMessage]",
    ) -> None:
        # is called after `read_message()` either completes successfully by
        # returning a replication message or times out,
        # restores the connection's original `_ready()` I/O callback instead of
        # instructing the event loop to completely stop polling the fd
        # for read-events. This is done not only for the sake of aesthetics
        # and completeness (queries and such are not supported on the cursor
        # after replication streaming has been started) but also to check that
        # the connection's fd is still valid by modifying its I/O callback
        # which requires a syscall on behalf of the poll/epoll/kqueue
        # OS resource under the hood.
        loop = fut.get_loop()
        # safety measure - uvloop has very different low-level APIs
        # and doesn't use the selectors module
        selector = getattr(loop, "_selector", None)
        try:
            # special case for systems that do not support modern I/O polling
            # mechanisms and only have the 'select' system call at
            # their disposal, in which case, no syscall will be performed
            # by the selectors module when modifying its key (mainly keeping
            # asyncio's `_WindowsSelectorEventLoop` in mind because IOCP
            # low-level event loop APIs are not supported by this library).
            if selector and isinstance(selector, selectors.SelectSelector):
                select.select([fd], [], [], 0)
            loop.add_reader(fd, self._conn._ready, self._conn._weakref)
        except OSError as os_exc:
            if _is_bad_descriptor_error(os_exc):
                with contextlib.suppress(OSError):
                    loop.remove_reader(fd)
                    # forget a bad file descriptor, don't try to
                    # touch it
                    self._conn._fileno = None

    @property
    def wal_end(self) -> int:
        """Return the LSN position of the current end of WAL on the server
        at the moment when last data or keepalive message was received
        from the server.
        """
        return cast(int, self._impl.wal_end)

    @property
    def feedback_timestamp(self) -> datetime.datetime:
        """Return a datetime object representing the timestamp at the moment
        when the last feedback message was sent to the server.
        """
        return cast(datetime.datetime, self._impl.feedback_timestamp)

    @property
    def io_timestamp(self) -> datetime.datetime:
        """Return a datetime object representing the timestamp at the moment
        of last communication with the server
        (a data or keepalive message in either direction).
        """
        return cast(datetime.datetime, self._impl.io_timestamp)

    async def _execute_replication_command(
        self,
        command_name: str,
        timeout: Optional[float],
        **kwargs: Any,
    ) -> None:
        if timeout is None:
            timeout = self._timeout

        waiter = self._conn._create_waiter(f"cursor.{command_name}")

        if self._echo:
            # get the appropriate logging method for
            # a specific replication command
            repr_method = self._repl_command_to_repr_method.get(command_name)
            if repr_method:
                logger.info(getattr(self, repr_method)(**kwargs))

        try:
            # execute psycopg2's original method
            # for the specified replication command
            getattr(self._impl, command_name)(**kwargs)
        except BaseException:
            self._conn._waiter = None
            raise

        try:
            await self._conn._poll(waiter, timeout)
        except asyncio.TimeoutError:
            self._impl.close()
            raise

    def _create_replication_slot_repr(
        self,
        slot_name: str,
        slot_type: Optional[ReplicationSlotType],
        output_plugin: Optional[str],
    ) -> str:
        command = f"CREATE_REPLICATION_SLOT {slot_name} "

        # determine the replication slot type
        if slot_type is None:
            slot_type = self._conn.raw.replication_type

        if slot_type == psycopg2.extras.REPLICATION_LOGICAL:
            command += f"LOGICAL {output_plugin or ''}"
        elif slot_type == psycopg2.extras.REPLICATION_PHYSICAL:
            command += "PHYSICAL"

        return command

    def _drop_replication_slot_repr(self, slot_name: str) -> str:
        return f"DROP_REPLICATION_SLOT {slot_name}"

    def _start_replication_repr(
        self,
        slot_name: Optional[str],
        slot_type: Optional[ReplicationSlotType],
        start_lsn: Union[int, str],
        timeline: int,
        options: Optional[Dict[Hashable, Any]],
        decode: bool,
        status_interval: float,
    ) -> str:
        command = "START_REPLICATION "

        # determine the replication slot type and name
        if slot_type is None:
            slot_type = self._conn.raw.replication_type

        if slot_type == psycopg2.extras.REPLICATION_LOGICAL:
            command += f"SLOT {slot_name or ''} LOGICAL "
        elif slot_type == psycopg2.extras.REPLICATION_PHYSICAL:
            if slot_name:
                command += f"SLOT {slot_name or ''} "

        # tweak the LSN format so it shows up in the correct
        # form within the logs
        if type(start_lsn) is str:
            lsn_lst = start_lsn.split("/")
            lsn = f"{int(lsn_lst[0], 16):X}/{int(lsn_lst[1], 16):08X}"
        else:
            lsn = (
                f"{cast(int, start_lsn) >> 32 & 4294967295:X}/"
                f"{cast(int, start_lsn) & 4294967295:08X}"
            )

        command += lsn

        if timeline != 0:
            command += f" TIMELINE {timeline}"

        # extract logical decoding plugin options from a dict and format them
        # according to the format in which they would appear in a raw
        # START_REPLICATION command
        if options:
            command += " ("
            for k, v in options.items():
                if not command.endswith("("):
                    command += ", "
                command += f"{k} {v}"
            command += ")"

        return command

    def _start_replication_expert_repr(
        self,
        command: str,
        decode: bool,
        status_interval: float,
    ) -> str:
        return command


async def _close_cursor(c: Cursor) -> None:
    c.close()


class Connection:
    """Low-level asynchronous interface for wrapped psycopg2 connection.

    The Connection instance encapsulates a database session.
    Provides support for creating asynchronous cursors.

    """

    _source_traceback = None

    # mapping of psycopg2 miscellaneous connection factories to library cursors
    # that provide some additional support for those connection types
    _conn_impl_to_library_cursor = MappingProxyType(
        {
            psycopg2.extras.PhysicalReplicationConnection: _ReplicationCursor,
            psycopg2.extras.LogicalReplicationConnection: _ReplicationCursor,
        }
    )
    # same as the above mapping but for psycopg2 cursor factories
    _cursor_impl_to_library_cursor = MappingProxyType(
        {
            psycopg2.extras.ReplicationCursor: _ReplicationCursor,
        }
    )

    def __init__(
        self,
        dsn: Optional[str],
        timeout: float,
        echo: bool = False,
        enable_json: bool = True,
        enable_hstore: bool = True,
        enable_uuid: bool = True,
        **kwargs: Any,
    ):
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore
        self._enable_uuid = enable_uuid
        self._loop = get_running_loop()
        self._waiter: Optional[
            "asyncio.Future[None]"
        ] = self._loop.create_future()

        kwargs["async_"] = kwargs.pop("async", True)
        kwargs.pop("loop", None)  # backward compatibility
        self._conn = psycopg2.connect(dsn, **kwargs)

        self._dsn = self._conn.dsn
        assert self._conn.isexecuting(), "Is conn an async at all???"
        self._fileno: Optional[int] = self._conn.fileno()
        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._writing = False
        self._echo = echo
        self._notifies = asyncio.Queue()  # type: ignore
        self._notifies_proxy = ClosableQueue(self._notifies, self._loop)
        self._weakref = weakref.ref(self)
        self._loop.add_reader(
            self._fileno, self._ready, self._weakref  # type: ignore
        )

        if self._loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    @staticmethod
    def _ready(weak_self: "weakref.ref[Any]") -> None:
        self = cast(Connection, weak_self())
        if self is None:
            return

        waiter = self._waiter

        try:
            state = self._conn.poll()
            while self._conn.notifies:
                notify = self._conn.notifies.pop(0)
                self._notifies.put_nowait(notify)
        except (psycopg2.Warning, psycopg2.Error) as exc:
            if self._fileno is not None:
                try:
                    select.select([self._fileno], [], [], 0)
                except OSError as os_exc:
                    if _is_bad_descriptor_error(os_exc):
                        with contextlib.suppress(OSError):
                            self._loop.remove_reader(self._fileno)
                            # forget a bad file descriptor, don't try to
                            # touch it
                            self._fileno = None

            try:
                if self._writing:
                    self._writing = False
                    if self._fileno is not None:
                        self._loop.remove_writer(self._fileno)
            except OSError as exc2:
                if exc2.errno != errno.EBADF:
                    # EBADF is ok for closed file descriptor
                    # chain exception otherwise
                    exc2.__cause__ = exc
                    exc = exc2
            self._notifies_proxy.close(exc)
            if waiter is not None and not waiter.done():
                waiter.set_exception(exc)
        else:
            if self._fileno is None:
                # connection closed
                if waiter is not None and not waiter.done():
                    waiter.set_exception(
                        psycopg2.OperationalError("Connection closed")
                    )
            if state == psycopg2.extensions.POLL_OK:
                if self._writing:
                    self._loop.remove_writer(self._fileno)  # type: ignore
                    self._writing = False
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)
            elif state == psycopg2.extensions.POLL_READ:
                if self._writing:
                    self._loop.remove_writer(self._fileno)  # type: ignore
                    self._writing = False
            elif state == psycopg2.extensions.POLL_WRITE:
                if not self._writing:
                    self._loop.add_writer(
                        self._fileno, self._ready, weak_self  # type: ignore
                    )
                    self._writing = True
            elif state == psycopg2.extensions.POLL_ERROR:
                self._fatal_error(
                    "Fatal error on aiopg connection: "
                    "POLL_ERROR from underlying .poll() call"
                )
            else:
                self._fatal_error(
                    f"Fatal error on aiopg connection: "
                    f"unknown answer {state} from underlying "
                    f".poll() call"
                )

    def _fatal_error(self, message: str) -> None:
        # Should be called from exception handler only.
        self._loop.call_exception_handler(
            {
                "message": message,
                "connection": self,
            }
        )
        self.close()
        if self._waiter and not self._waiter.done():
            self._waiter.set_exception(psycopg2.OperationalError(message))

    def _create_waiter(self, func_name: str) -> "asyncio.Future[None]":
        if self._waiter is not None:
            raise RuntimeError(
                f"{func_name}() called while another coroutine "
                f"is already waiting for incoming data"
            )
        self._waiter = self._loop.create_future()
        return self._waiter

    async def _poll(
        self, waiter: "asyncio.Future[None]", timeout: float
    ) -> None:
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready(self._weakref)

        try:
            await asyncio.wait_for(self._waiter, timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
            await asyncio.shield(self.close())
            raise exc
        except psycopg2.extensions.QueryCanceledError as exc:
            self._loop.call_exception_handler(
                {
                    "message": exc.pgerror,
                    "exception": exc,
                    "future": self._waiter,
                }
            )
            raise asyncio.CancelledError
        finally:
            self._waiter = None

    def isexecuting(self) -> bool:
        return self._conn.isexecuting()  # type: ignore

    def cursor(
        self,
        name: Optional[str] = None,
        cursor_factory: Any = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        timeout: Optional[float] = None,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> _ContextManager[Cursor]:
        """A coroutine that returns a new cursor object using the connection.

        *cursor_factory* argument can be used to create non-standard
         cursors. The argument must be subclass of
         `psycopg2.extensions.cursor`.

        *name*, *scrollable* and *withhold* parameters are not supported by
        psycopg in asynchronous mode.

        """

        self._last_usage = self._loop.time()
        coro = self._cursor(
            name=name,
            cursor_factory=cursor_factory,
            scrollable=scrollable,
            withhold=withhold,
            timeout=timeout,
            isolation_level=isolation_level,
        )
        return _ContextManager[Cursor](coro, _close_cursor)

    async def _cursor(
        self,
        name: Optional[str] = None,
        cursor_factory: Any = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        timeout: Optional[float] = None,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> Cursor:
        if timeout is None:
            timeout = self._timeout

        impl = await self._cursor_impl(
            name=name,
            cursor_factory=cursor_factory,
            scrollable=scrollable,
            withhold=withhold,
        )

        # check if there are any library cursor types that provide
        # additional support for the underlying psycopg2 connection,
        # otherwise, use the default `Cursor` implementation
        lib_cursor_type = self._conn_impl_to_library_cursor.get(
            type(self._conn),
            Cursor,
        )
        # same as the previous check but for psycopg2 cursor factories,
        # the result of this statement may also override the above step to
        # prevent any undefined behavior when combining conflicting psycopg2
        # connection and cursor types
        if cursor_factory is not None:
            lib_cursor_type = self._cursor_impl_to_library_cursor.get(
                type(impl),
                Cursor,
            )

        return lib_cursor_type(
            self,
            impl,
            timeout,
            self._echo,
            isolation_level,
        )

    async def _cursor_impl(
        self,
        name: Optional[str] = None,
        cursor_factory: Any = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> Any:
        if cursor_factory is None:
            impl = self._conn.cursor(
                name=name, scrollable=scrollable, withhold=withhold
            )
        else:
            impl = self._conn.cursor(
                name=name,
                cursor_factory=cursor_factory,
                scrollable=scrollable,
                withhold=withhold,
            )
        return impl

    def _close(self) -> None:
        """Remove the connection from the event_loop and close it."""
        # N.B. If connection contains uncommitted transaction the
        # transaction will be discarded
        if self._fileno is not None:
            self._loop.remove_reader(self._fileno)
            if self._writing:
                self._writing = False
                self._loop.remove_writer(self._fileno)

        self._conn.close()

        if not self._loop.is_closed():
            if self._waiter is not None and not self._waiter.done():
                self._waiter.set_exception(
                    psycopg2.OperationalError("Connection closed")
                )

            self._notifies_proxy.close(
                psycopg2.OperationalError("Connection closed")
            )

    def close(self) -> "asyncio.Future[None]":
        self._close()
        return create_completed_future(self._loop)

    @property
    def closed(self) -> bool:
        """Connection status.

        Read-only attribute reporting whether the database connection is
        open (False) or closed (True).

        """
        return self._conn.closed  # type: ignore

    @property
    def raw(self) -> Any:
        """Underlying psycopg connection object, readonly"""
        return self._conn

    async def commit(self) -> None:
        raise psycopg2.ProgrammingError(
            "commit cannot be used in asynchronous mode"
        )

    async def rollback(self) -> None:
        raise psycopg2.ProgrammingError(
            "rollback cannot be used in asynchronous mode"
        )

    # TPC

    async def xid(
        self, format_id: int, gtrid: str, bqual: str
    ) -> Tuple[int, str, str]:
        return self._conn.xid(format_id, gtrid, bqual)  # type: ignore

    async def tpc_begin(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_begin cannot be used in asynchronous mode"
        )

    async def tpc_prepare(self) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_prepare cannot be used in asynchronous mode"
        )

    async def tpc_commit(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_commit cannot be used in asynchronous mode"
        )

    async def tpc_rollback(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_rollback cannot be used in asynchronous mode"
        )

    async def tpc_recover(self) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_recover cannot be used in asynchronous mode"
        )

    async def cancel(self) -> None:
        raise psycopg2.ProgrammingError(
            "cancel cannot be used in asynchronous mode"
        )

    async def reset(self) -> None:
        raise psycopg2.ProgrammingError(
            "reset cannot be used in asynchronous mode"
        )

    @property
    def dsn(self) -> Optional[str]:
        """DSN connection string.

        Read-only attribute representing dsn connection string used
        for connectint to PostgreSQL server.

        """
        return self._dsn  # type: ignore

    async def set_session(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "set_session cannot be used in asynchronous mode"
        )

    @property
    def autocommit(self) -> bool:
        """Autocommit status"""
        return self._conn.autocommit  # type: ignore

    @autocommit.setter
    def autocommit(self, val: bool) -> None:
        """Autocommit status"""
        self._conn.autocommit = val

    @property
    def isolation_level(self) -> int:
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        return self._conn.isolation_level  # type: ignore

    async def set_isolation_level(self, val: int) -> None:
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        self._conn.set_isolation_level(val)

    @property
    def encoding(self) -> str:
        """Client encoding for SQL operations."""
        return self._conn.encoding  # type: ignore

    async def set_client_encoding(self, val: str) -> None:
        self._conn.set_client_encoding(val)

    @property
    def notices(self) -> List[str]:
        """A list of all db messages sent to the client during the session."""
        return self._conn.notices  # type: ignore

    @property
    def cursor_factory(self) -> Any:
        """The default cursor factory used by .cursor()."""
        return self._conn.cursor_factory

    async def get_backend_pid(self) -> int:
        """Returns the PID of the backend server process."""
        return self._conn.get_backend_pid()  # type: ignore

    async def get_parameter_status(self, parameter: str) -> Optional[str]:
        """Look up a current parameter setting of the server."""
        return self._conn.get_parameter_status(parameter)  # type: ignore

    async def get_transaction_status(self) -> int:
        """Return the current session transaction status as an integer."""
        return self._conn.get_transaction_status()  # type: ignore

    @property
    def protocol_version(self) -> int:
        """A read-only integer representing protocol being used."""
        return self._conn.protocol_version  # type: ignore

    @property
    def server_version(self) -> int:
        """A read-only integer representing the backend version."""
        return self._conn.server_version  # type: ignore

    @property
    def status(self) -> int:
        """A read-only integer representing the status of the connection."""
        return self._conn.status  # type: ignore

    async def lobject(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "lobject cannot be used in asynchronous mode"
        )

    @property
    def timeout(self) -> float:
        """Return default timeout for connection operations."""
        return self._timeout

    @property
    def last_usage(self) -> float:
        """Return time() when connection was used."""
        return self._last_usage

    @property
    def echo(self) -> bool:
        """Return echo mode status."""
        return self._echo

    def __repr__(self) -> str:
        return (
            f"<"
            f"{type(self).__module__}::{type(self).__name__} "
            f"isexecuting={self.isexecuting()}, "
            f"closed={self.closed}, "
            f"echo={self.echo}, "
            f">"
        )

    def __del__(self) -> None:
        try:
            _conn = self._conn
        except AttributeError:
            return
        if _conn is not None and not _conn.closed:
            self.close()
            warnings.warn(f"Unclosed connection {self!r}", ResourceWarning)

            context = {"connection": self, "message": "Unclosed connection"}
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    @property
    def notifies(self) -> ClosableQueue:
        """Return notification queue (an asyncio.Queue -like object)."""
        return self._notifies_proxy

    async def _get_oids(self) -> Tuple[Any, Any]:
        cursor = await self.cursor()
        rv0, rv1 = [], []
        try:
            await cursor.execute(
                "SELECT t.oid, typarray "
                "FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid "
                "WHERE typname = 'hstore';"
            )

            async for oids in cursor:
                if isinstance(oids, Mapping):
                    rv0.append(oids["oid"])
                    rv1.append(oids["typarray"])
                else:
                    rv0.append(oids[0])
                    rv1.append(oids[1])
        finally:
            cursor.close()

        return tuple(rv0), tuple(rv1)

    async def _connect(self) -> "Connection":
        try:
            await self._poll(self._waiter, self._timeout)  # type: ignore
        except BaseException:
            await asyncio.shield(self.close())
            raise
        if self._enable_json:
            psycopg2.extras.register_default_json(self._conn)
        if self._enable_uuid:
            psycopg2.extras.register_uuid(conn_or_curs=self._conn)
        if self._enable_hstore:
            oid, array_oid = await self._get_oids()
            psycopg2.extras.register_hstore(
                self._conn, oid=oid, array_oid=array_oid
            )

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        return self._connect().__await__()

    async def __aenter__(self) -> "Connection":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()
