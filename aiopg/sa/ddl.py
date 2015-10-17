import asyncio
from aiopg.sa.visitor import AsyncClauseVisitor
from sqlalchemy import util, MetaData
from sqlalchemy.exc import CircularDependencyError
from sqlalchemy.sql.base import _bind_or_error
from sqlalchemy.sql.ddl import sort_tables_and_constraints, CreateTable,\
    AddConstraint, CreateSequence, \
    CreateIndex, DropSequence, DropConstraint, DropTable, DropIndex


class AsyncMetaData(MetaData):
    @asyncio.coroutine
    def create_all(self, bind=None, tables=None, checkfirst=True):
        if bind is None:
            bind = _bind_or_error(self)
        yield from bind._run_visitor(SchemaGenerator,
                                     self,
                                     checkfirst=checkfirst,
                                     tables=tables)

    @asyncio.coroutine
    def drop_all(self, bind=None, tables=None, checkfirst=True):
        if bind is None:
            bind = _bind_or_error(self)
        yield from bind._run_visitor(SchemaDropper,
                                     self,
                                     checkfirst=checkfirst,
                                     tables=tables)


class SchemaGenerator(AsyncClauseVisitor):
    __traverse_options__ = {'schema_visitor': True}

    def __init__(self, dialect, connection, checkfirst=False,
                 tables=None, **kwargs):
        self.connection = connection
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect
        self.memo = {}

    @asyncio.coroutine
    def _can_create_table(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        has_table = yield from self.dialect.has_table(self.connection,
                                                      table.name,
                                                      schema=table.schema)
        return not self.checkfirst or not has_table

    @asyncio.coroutine
    def _can_create_sequence(self, sequence):
        has_sequence = yield from self.dialect.has_sequence(
                            self.connection,
                            sequence.name,
                            schema=sequence.schema)
        return self.dialect.supports_sequences and \
            (
                (not self.dialect.sequences_optional or
                 not sequence.optional) and
                (
                    not self.checkfirst or
                    not has_sequence
                )
            )

    @asyncio.coroutine
    def visit_metadata(self, metadata):
        if self.tables is not None:
            tables = self.tables
        else:
            tables = list(metadata.tables.values())

        ts = []
        for t in tables:
            r = yield from self._can_create_table(t)
            if r:
                ts.append(t)
        collection = sort_tables_and_constraints(ts)

        seq_coll = [s for s in metadata._sequences.values()
                    if s.column is None and self._can_create_sequence(s)]

        event_collection = [
            t for (t, fks) in collection if t is not None
        ]
        metadata.dispatch.before_create(metadata, self.connection,
                                        tables=event_collection,
                                        checkfirst=self.checkfirst,
                                        _ddl_runner=self)

        for seq in seq_coll:
            yield from self.traverse_single(seq, create_ok=True)

        for table, fkcs in collection:
            if table is not None:
                yield from self.traverse_single(
                    table, create_ok=True,
                    include_foreign_key_constraints=fkcs,
                    _is_metadata_operation=True)
            else:
                for fkc in fkcs:
                    yield from self.traverse_single(fkc)

        metadata.dispatch.after_create(metadata, self.connection,
                                       tables=event_collection,
                                       checkfirst=self.checkfirst,
                                       _ddl_runner=self)

    @asyncio.coroutine
    def visit_table(
            self, table, create_ok=False,
            include_foreign_key_constraints=None,
            _is_metadata_operation=False):
        can_create_table = yield from self._can_create_table(table)
        if not create_ok and not can_create_table:
            return

        table.dispatch.before_create(
            table, self.connection,
            checkfirst=self.checkfirst,
            _ddl_runner=self,
            _is_metadata_operation=_is_metadata_operation)

        for column in table.columns:
            if column.default is not None:
                yield from self.traverse_single(column.default)

        if not self.dialect.supports_alter:
            # e.g., don't omit any foreign key constraints
            include_foreign_key_constraints = None

        yield from self.connection.execute(
            CreateTable(
                table,
                include_foreign_key_constraints=include_foreign_key_constraints
            ))

        if hasattr(table, 'indexes'):
            for index in table.indexes:
                yield from self.traverse_single(index)

        table.dispatch.after_create(
            table, self.connection,
            checkfirst=self.checkfirst,
            _ddl_runner=self,
            _is_metadata_operation=_is_metadata_operation)

    @asyncio.coroutine
    def visit_foreign_key_constraint(self, constraint):
        if not self.dialect.supports_alter:
            return
        yield from self.connection.execute(AddConstraint(constraint))

    @asyncio.coroutine
    def visit_sequence(self, sequence, create_ok=False):
        if not create_ok and not self._can_create_sequence(sequence):
            return
        yield from self.connection.execute(CreateSequence(sequence))

    @asyncio.coroutine
    def visit_index(self, index):
        yield from self.connection.execute(CreateIndex(index))


class SchemaDropper(AsyncClauseVisitor):
    __traverse_options__ = {'schema_visitor': True}

    def __init__(self, dialect, connection, checkfirst=False,
                 tables=None, **kwargs):
        self.connection = connection
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect
        self.memo = {}

    @asyncio.coroutine
    def visit_metadata(self, metadata):
        if self.tables is not None:
            tables = self.tables
        else:
            tables = list(metadata.tables.values())

        try:
            unsorted_tables = []
            for t in tables:
                can_drop_table = yield from self._can_drop_table(t)
                if can_drop_table:
                    unsorted_tables.append(t)
            collection = list(reversed(
                sort_tables_and_constraints(
                    unsorted_tables,
                    filter_fn=lambda constraint: False
                    if not self.dialect.supports_alter or
                    constraint.name is None
                    else None
                )
            ))
        except CircularDependencyError as err2:
            if not self.dialect.supports_alter:
                util.warn(
                    "Can't sort tables for DROP; an "
                    "unresolvable foreign key "
                    "dependency exists between tables: %s, and backend does "
                    "not support ALTER.  To restore at least a partial sort, "
                    "apply use_alter=True to ForeignKey and "
                    "ForeignKeyConstraint "
                    "objects involved in the cycle to mark these as known "
                    "cycles that will be ignored."
                    % (
                        ", ".join(sorted([t.fullname for t in err2.cycles]))
                    )
                )
                collection = [(t, ()) for t in unsorted_tables]
            else:
                util.raise_from_cause(
                    CircularDependencyError(
                        err2.args[0],
                        err2.cycles, err2.edges,
                        msg="Can't sort tables for DROP; an "
                        "unresolvable foreign key "
                        "dependency exists between tables: %s.  Please ensure "
                        "that the ForeignKey and ForeignKeyConstraint objects "
                        "involved in the cycle have "
                        "names so that they can be dropped using "
                        "DROP CONSTRAINT."
                        % (
                            ", ".join(
                                sorted([t.fullname for t in err2.cycles]))
                        )

                    )
                )

        seq_coll = []
        for s in metadata._sequences.values():
            if s.column is None and (yield from self._can_drop_sequence(s)):
                seq_coll.append(s)

        event_collection = [
            t for (t, fks) in collection if t is not None
        ]

        metadata.dispatch.before_drop(
            metadata, self.connection, tables=event_collection,
            checkfirst=self.checkfirst, _ddl_runner=self)

        for table, fkcs in collection:
            if table is not None:
                yield from self.traverse_single(
                    table, drop_ok=True, _is_metadata_operation=True)
            else:
                for fkc in fkcs:
                    yield from self.traverse_single(fkc)

        for seq in seq_coll:
            yield from self.traverse_single(seq, drop_ok=True)

        metadata.dispatch.after_drop(
            metadata, self.connection, tables=event_collection,
            checkfirst=self.checkfirst, _ddl_runner=self)

    @asyncio.coroutine
    def _can_drop_table(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or (
            yield from self.dialect.has_table(
                self.connection, table.name, schema=table.schema)
        )

    @asyncio.coroutine
    def _can_drop_sequence(self, sequence):
        return self.dialect.supports_sequences and \
            ((not self.dialect.sequences_optional or
              not sequence.optional) and
                (not self.checkfirst or
                 (yield from self.dialect.has_sequence(
                         self.connection,
                         sequence.name,
                         schema=sequence.schema))
                 )
             )

    @asyncio.coroutine
    def visit_index(self, index):
        yield from self.connection.execute(DropIndex(index))

    @asyncio.coroutine
    def visit_table(self, table, drop_ok=False, _is_metadata_operation=False):
        if not drop_ok and not (yield from self._can_drop_table(table)):
            return

        table.dispatch.before_drop(
            table, self.connection,
            checkfirst=self.checkfirst,
            _ddl_runner=self,
            _is_metadata_operation=_is_metadata_operation)

        for column in table.columns:
            if column.default is not None:
                yield from self.traverse_single(column.default)

        yield from self.connection.execute(DropTable(table))

        table.dispatch.after_drop(
           table, self.connection,
           checkfirst=self.checkfirst,
           _ddl_runner=self,
           _is_metadata_operation=_is_metadata_operation)

    @asyncio.coroutine
    def visit_foreign_key_constraint(self, constraint):
        if not self.dialect.supports_alter:
            return
        yield from self.connection.execute(DropConstraint(constraint))

    @asyncio.coroutine
    def visit_sequence(self, sequence, drop_ok=False):
        if not drop_ok and not self._can_drop_sequence(sequence):
            return
        yield from self.connection.execute(DropSequence(sequence))
