import enum
import uuid
import warnings
from abc import ABC

import psycopg2

from aiopg.utils import _TransactionPointContextManager

__all__ = ('IsolationLevel', 'Transaction')


class IsolationCompiler(ABC):
    __slots__ = ('_isolation_level', '_readonly', '_deferrable')

    def __init__(self, isolation_level, readonly, deferrable):
        self._isolation_level = isolation_level
        self._readonly = readonly
        self._deferrable = deferrable

    @property
    def name(self):
        return self._isolation_level

    def savepoint(self, unique_id):
        return f'SAVEPOINT {unique_id}'

    def release_savepoint(self, unique_id):
        return f'RELEASE SAVEPOINT {unique_id}'

    def rollback_savepoint(self, unique_id):
        return f'ROLLBACK TO SAVEPOINT {unique_id}'

    def commit(self):
        return 'COMMIT'

    def rollback(self):
        return 'ROLLBACK'

    def begin(self):
        query = 'BEGIN'
        if self._isolation_level is not None:
            query += (
                f' ISOLATION LEVEL {self._isolation_level.upper()}'
            )

        if self._readonly:
            query += ' READ ONLY'

        if self._deferrable:
            query += ' DEFERRABLE'

        return query

    def __repr__(self):
        return self.name


class ReadCommittedCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        super().__init__('Read committed', readonly, deferrable)


class RepeatableReadCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        super().__init__('Repeatable read', readonly, deferrable)


class SerializableCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        super().__init__('Serializable', readonly, deferrable)


class DefaultCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        super().__init__(None, readonly, deferrable)

    @property
    def name(self):
        return 'Default'


class IsolationLevel(enum.Enum):
    serializable = SerializableCompiler
    repeatable_read = RepeatableReadCompiler
    read_committed = ReadCommittedCompiler
    default = DefaultCompiler

    def __call__(self, readonly, deferrable):
        return self.value(readonly, deferrable)


class Transaction:
    __slots__ = ('_cursor', '_is_begin', '_isolation', '_unique_id')

    def __init__(self, cursor, isolation_level,
                 readonly=False, deferrable=False):
        self._cursor = cursor
        self._is_begin = False
        self._unique_id = None
        self._isolation = isolation_level(readonly, deferrable)

    @property
    def is_begin(self):
        return self._is_begin

    async def begin(self):
        if self._is_begin:
            raise psycopg2.ProgrammingError(
                'You are trying to open a new transaction, use the save point')
        self._is_begin = True
        await self._cursor.execute(self._isolation.begin())
        return self

    async def commit(self):
        self._check_commit_rollback()
        await self._cursor.execute(self._isolation.commit())
        self._is_begin = False

    async def rollback(self):
        self._check_commit_rollback()
        if not self._cursor.closed:
            await self._cursor.execute(self._isolation.rollback())
        self._is_begin = False

    async def rollback_savepoint(self):
        self._check_release_rollback()
        if not self._cursor.closed:
            await self._cursor.execute(
                self._isolation.rollback_savepoint(self._unique_id))
        self._unique_id = None

    async def release_savepoint(self):
        self._check_release_rollback()
        await self._cursor.execute(
            self._isolation.release_savepoint(self._unique_id))
        self._unique_id = None

    async def savepoint(self):
        self._check_commit_rollback()
        if self._unique_id is not None:
            raise psycopg2.ProgrammingError('You do not shut down savepoint')

        self._unique_id = f's{uuid.uuid1().hex}'
        await self._cursor.execute(
            self._isolation.savepoint(self._unique_id))

        return self

    def point(self):
        return _TransactionPointContextManager(self.savepoint())

    def _check_commit_rollback(self):
        if not self._is_begin:
            raise psycopg2.ProgrammingError('You are trying to commit '
                                            'the transaction does not open')

    def _check_release_rollback(self):
        self._check_commit_rollback()
        if self._unique_id is None:
            raise psycopg2.ProgrammingError('You do not start savepoint')

    def __repr__(self):
        return (f"<{self.__class__.__name__} "
                f"transaction={self._isolation} id={id(self):#x}>")

    def __del__(self):
        if self._is_begin:
            warnings.warn(
                f"You have not closed transaction {self!r}",
                ResourceWarning)

        if self._unique_id is not None:
            warnings.warn(
                f"You have not closed savepoint {self!r}",
                ResourceWarning)

    async def __aenter__(self):
        return await self.begin()

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
