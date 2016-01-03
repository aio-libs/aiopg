import asyncio
from sqlalchemy.sql import ClauseVisitor
from sqlalchemy.sql.visitors import iterate


@asyncio.coroutine
def traverse_using(iterator, obj, visitors):
    """visit the given expression structure using the given iterator of
    objects.

    """
    for target in iterator:
        meth = visitors.get(target.__visit_name__, None)
        if meth:
            yield from meth(target)
    return obj


@asyncio.coroutine
def traverse(obj, opts, visitors):
    """traverse and visit the given expression structure using the default
     iterator.
    """
    result = yield from traverse_using(iterate(obj, opts), obj, visitors)
    return result


class AsyncClauseVisitor(ClauseVisitor):
    @asyncio.coroutine
    def traverse_single(self, obj, **kw):
        for v in self._visitor_iterator:
            meth = getattr(v, "visit_%s" % obj.__visit_name__, None)
            if meth:
                yield from meth(obj, **kw)

    @asyncio.coroutine
    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        result = yield from traverse(obj, self.__traverse_options__, self._visitor_dict)
        return result
