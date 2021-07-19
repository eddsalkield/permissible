from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Type, TypeVar
from contextlib import contextmanager

from permissible.core import BaseSession
from permissible.crud.core import CRUDBackend, CreateSchema, ReadSchema, \
        UpdateSchema, DeleteSchema, CRUDAccessType, CRUDBackendAccessRecord

NoneSession = type(None)


class PrintCRUDBackend(
        CRUDBackend[NoneSession],
        Generic[CreateSchema, ReadSchema, UpdateSchema, DeleteSchema]):
    """
    A test CRUD backend, providing accesses to simply print the given data.
    """
    def __init__(
            self,
            create_schema: Type[CreateSchema],
            read_schema: Type[ReadSchema],
            update_schema: Type[UpdateSchema],
            delete_schema: Type[DeleteSchema]):
        # For a SQL table o/e, this is where the table itself would be bound

        def create(session: NoneSession, data: CreateSchema) -> CreateSchema:
            print(f'Creating {data}')
            return data

        def read(session: NoneSession, data: ReadSchema) -> ReadSchema:
            print(f'Reading {data}')
            return data

        def update(session: NoneSession, data: UpdateSchema) -> UpdateSchema:
            print(f'Updating {data}')
            return data

        def delete(session: NoneSession, data: DeleteSchema) -> DeleteSchema:
            print(f'Deleting {data}')
            return data

        super().__init__(
            CRUDBackendAccessRecord[CreateSchema, CreateSchema, NoneSession](
                create_schema,
                create_schema,
                create,
                CRUDAccessType.create),
            CRUDBackendAccessRecord[ReadSchema, ReadSchema, NoneSession](
                read_schema,
                read_schema,
                read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[UpdateSchema, UpdateSchema, NoneSession](
                update_schema,
                update_schema,
                update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[DeleteSchema, DeleteSchema, NoneSession](
                delete_schema,
                delete_schema,
                delete,
                CRUDAccessType.delete),
            )

    @contextmanager
    def generate_session(self) -> Generator[BaseSession, None, None]:
        """
        Generate a new session in case the user didn't specify one yet
        """
        try:
            print('Session opened')
            yield None
        finally:
            print('Session closed')
