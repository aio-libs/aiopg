import asyncio
from types import TracebackType
from typing import Optional, Type

from ..utils import _ContextManager
from .connection import SAConnection
from .transaction import Transaction


class _SAConnectionContextManager(_ContextManager[SAConnection]):
    __slots__ = ()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._obj is None:
            self._obj = await self._coro

        try:
            return await self._obj.__anext__()
        except StopAsyncIteration:
            try:
                await asyncio.shield(self._obj.close())
            finally:
                self._obj = None
            raise


class _TransactionContextManager(_ContextManager[Transaction]):
    __slots__ = ()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            if exc_type:
                await self._obj.rollback()
            else:
                if self._obj.is_active:
                    await self._obj.commit()
        finally:
            self._obj = None
