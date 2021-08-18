from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Type, TypeVar
from contextlib import asynccontextmanager
from pydantic import BaseModel
from permissible.core import BaseSession
from permissible.crud.core import CRUDBackend, CreateSchema, ReadSchema, \
        UpdateSchema, DeleteSchema, CRUDAccessType, CRUDBackendAccessRecord


class PrintSession(BaseSession):
    state: List[str]

    def __init__(self):
        self.state = []
        super().__init__()
    
    def add(self, text):
        self.state.append(text)
    
    def commit(self):
        print(f'committed {self.state}')


class PrintCRUDBackend(CRUDBackend[PrintSession]):
    """
    A test CRUD backend, providing accesses to simply print the given data.
    """
    def __init__(
            self,
            create_schema: BaseModel,
            read_schema: BaseModel,
            update_schema: BaseModel,
            delete_schema: BaseModel):
        # For a SQL table o/e, this is where the table itself would be bound


        def create(session: PrintSession, data: create_schema) -> create_schema:
            session.add(f'Creating {data}')
            return data

        def read(session: PrintSession, data: read_schema) -> read_schema:
            session.add(f'Reading {data}')
            return data

        def update(session: PrintSession, data: update_schema) -> update_schema:
            session.add(f'Updating {data}')
            return data

        def delete(session: PrintSession, data: delete_schema) -> delete_schema:
            session.add(f'Deleting {data}')
            return data

        super().__init__(
            CRUDBackendAccessRecord[create_schema, create_schema, PrintSession](
                create_schema,
                create_schema,
                create,
                CRUDAccessType.create),
            CRUDBackendAccessRecord[read_schema, read_schema, PrintSession](
                read_schema,
                read_schema,
                read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[update_schema, update_schema, PrintSession](
                update_schema,
                update_schema,
                update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[delete_schema, delete_schema, PrintSession](
                delete_schema,
                delete_schema,
                delete,
                CRUDAccessType.delete),
            )

    @asynccontextmanager
    async def generate_session(self) -> Generator[PrintSession, None, None]:
        """
        Generate a new session in case the user didn't specify one yet
        """
        try:
            print('Session opened')
            yield PrintSession()
        finally:
            print('Session closed')
