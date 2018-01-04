import asyncio
import enum
import uuid
import warnings
from abc import ABC, abstractmethod

import psycopg2
from aiopg.utils import PY_35, _TransactionPointContextManager

__all__ = ('IsolationLevel', 'Transaction')


class IsolationCompiler(ABC):
    name = ''

    __slots__ = ('_readonly', '_deferrable')

    def __init__(self, readonly, deferrable):
        self._readonly = readonly
        self._deferrable = deferrable
        self._check_readonly_deferrable()

    def _check_readonly_deferrable(self):
        available = self._readonly or self._deferrable
        if not isinstance(self, SerializableCompiler) and available:
            raise ValueError('Is only available for serializable transactions')

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
        raise NotImplementedError("Please Implement this method")

    def __repr__(self):
        return self.name


class ReadCommittedCompiler(IsolationCompiler):
    name = 'Read committed'

    def begin(self):
        return 'BEGIN'


class RepeatableReadCompiler(IsolationCompiler):
    name = 'Repeatable read'

    def begin(self):
        return 'BEGIN ISOLATION LEVEL REPEATABLE READ'


class SerializableCompiler(IsolationCompiler):
    name = 'Serializable'

    def begin(self):
        query = 'BEGIN ISOLATION LEVEL SERIALIZABLE'

        if self._readonly:
            query += ' READ ONLY'

        if self._deferrable:
            query += ' DEFERRABLE'

        return query


class IsolationLevel(enum.Enum):
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

    @asyncio.coroutine
    def begin(self):
        if self._is_begin:
            raise psycopg2.ProgrammingError(
                'You are trying to open a new transaction, use the save point')
        self._is_begin = True
        yield from self._cur.execute(self._isolation.begin())
        return self

    @asyncio.coroutine
    def commit(self):
        self._check_commit_rollback()
        yield from self._cur.execute(self._isolation.commit())
        self._is_begin = False

    @asyncio.coroutine
    def rollback(self):
        self._check_commit_rollback()
        yield from self._cur.execute(self._isolation.rollback())
        self._is_begin = False

    @asyncio.coroutine
    def rollback_savepoint(self):
        self._check_release_rollback()
        yield from self._cur.execute(
            self._isolation.rollback_savepoint(self._unique_id))
        self._unique_id = None

    @asyncio.coroutine
    def release_savepoint(self):
        self._check_release_rollback()
        yield from self._cur.execute(
            self._isolation.release_savepoint(self._unique_id))
        self._unique_id = None

    @asyncio.coroutine
    def savepoint(self):
        self._check_commit_rollback()
        if self._unique_id is not None:
            raise psycopg2.ProgrammingError('You do not shut down savepoint')

        self._unique_id = 's{}'.format(uuid.uuid1().hex)
        yield from self._cur.execute(
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

    if PY_35:
        @asyncio.coroutine
        def __aenter__(self):
            return (yield from self.begin())

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc, tb):
            if exc_type is not None:
                yield from self.rollback()
            else:
                yield from self.commit()
