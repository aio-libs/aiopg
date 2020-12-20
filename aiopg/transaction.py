import enum
import uuid
import warnings
from abc import ABC, abstractmethod

import psycopg2

from aiopg.utils import _TransactionPointContextManager

__all__ = ('IsolationLevel', 'Transaction')


class IsolationCompiler(ABC):
    __slots__ = ()

    @property
    @abstractmethod
    def name(self):
        ...

    def savepoint(self, unique_id):
        return 'SAVEPOINT {}'.format(unique_id)

    def release_savepoint(self, unique_id):
        return 'RELEASE SAVEPOINT {}'.format(unique_id)

    def rollback_savepoint(self, unique_id):
        return 'ROLLBACK TO SAVEPOINT {}'.format(unique_id)

    def commit(self):
        return 'COMMIT'

    def rollback(self):
        return 'ROLLBACK'

    @abstractmethod
    def begin(self):
        ...

    def __repr__(self):
        return self.name


class ReadCommittedCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        if readonly or deferrable:
            raise ValueError("Readonly or deferrable are not supported")

    @property
    def name(self):
        return 'Read committed'

    def begin(self):
        return 'BEGIN ISOLATION LEVEL READ COMMITTED'


class RepeatableReadCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        if readonly or deferrable:
            raise ValueError("Readonly or deferrable are not supported")

    @property
    def name(self):
        return 'Repeatable read'

    def begin(self):
        return 'BEGIN ISOLATION LEVEL REPEATABLE READ'


class SerializableCompiler(IsolationCompiler):
    __slots__ = ('_readonly', '_deferrable')

    def __init__(self, readonly, deferrable):
        self._readonly = readonly
        self._deferrable = deferrable

    @property
    def name(self):
        return 'Serializable'

    def begin(self):
        query = 'BEGIN ISOLATION LEVEL SERIALIZABLE'

        if self._readonly:
            query += ' READ ONLY'

        if self._deferrable:
            query += ' DEFERRABLE'

        return query


class DefaultCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly, deferrable):
        if readonly or deferrable:
            raise ValueError("Readonly or deferrable are not supported")

    @property
    def name(self):
        return 'Default'

    def begin(self):
        return 'BEGIN'


class IsolationLevel(enum.Enum):
    default = DefaultCompiler
    serializable = SerializableCompiler
    repeatable_read = RepeatableReadCompiler
    read_committed = ReadCommittedCompiler

    def __call__(self, readonly, deferrable):
        return self.value(readonly, deferrable)


class Transaction:
    __slots__ = ('_cur', '_is_begin', '_isolation', '_unique_id')

    def __init__(self, cur, isolation_level,
                 readonly=False, deferrable=False):
        self._cur = cur
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
        await self._cur.execute(self._isolation.begin())
        return self

    async def commit(self):
        self._check_commit_rollback()
        await self._cur.execute(self._isolation.commit())
        self._is_begin = False

    async def rollback(self):
        self._check_commit_rollback()
        await self._cur.execute(self._isolation.rollback())
        self._is_begin = False

    async def rollback_savepoint(self):
        self._check_release_rollback()
        await self._cur.execute(
            self._isolation.rollback_savepoint(self._unique_id))
        self._unique_id = None

    async def release_savepoint(self):
        self._check_release_rollback()
        await self._cur.execute(
            self._isolation.release_savepoint(self._unique_id))
        self._unique_id = None

    async def savepoint(self):
        self._check_commit_rollback()
        if self._unique_id is not None:
            raise psycopg2.ProgrammingError('You do not shut down savepoint')

        self._unique_id = 's{}'.format(uuid.uuid1().hex)
        await self._cur.execute(
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
        return "<{} transaction={} id={:#x}>".format(
            self.__class__.__name__,
            self._isolation,
            id(self)
        )

    def __del__(self):
        if self._is_begin:
            warnings.warn(
                "You have not closed transaction {!r}".format(self),
                ResourceWarning)

        if self._unique_id is not None:
            warnings.warn(
                "You have not closed savepoint {!r}".format(self),
                ResourceWarning)

    async def __aenter__(self):
        return await self.begin()

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
