from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Type, TypeVar
from pydantic import BaseModel
from permissible.core import BaseSession
from permissible.crud.core import CRUDBackend, CreateSchema, ReadSchema, \
        UpdateSchema, DeleteSchema, CRUDAccessType, CRUDBackendAccessRecord


# TODO: investigate how session should behave after being committed
#       should state be erased, for commit only happen once?
class PrintSession(BaseSession):
    state: List[str]

    def __init__(self):
        print('session opened')
        self.state = []
        super().__init__()
    
    def add(self, text):
        self.state.append(text)
    
    def commit(self):
        print(f'committed {self.state}')

    def rollback(self):
        print('session rolled back')

    def close(self):
            print('session closed')


class PrintCRUDBackend(CRUDBackend):
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
            session.add(f'creating {data}')
            return data

        def read(session: PrintSession, data: read_schema) -> read_schema:
            session.add(f'reading {data}')
            return data

        def update(session: PrintSession, data: update_schema) -> update_schema:
            session.add(f'updating {data}')
            return data

        def delete(session: PrintSession, data: delete_schema) -> delete_schema:
            session.add(f'deleting {data}')
            return data

        super().__init__(
            CRUDBackendAccessRecord[create_schema, create_schema](
                create_schema,
                create_schema,
                create,
                CRUDAccessType.create),
            CRUDBackendAccessRecord[read_schema, read_schema](
                read_schema,
                read_schema,
                read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[update_schema, update_schema](
                update_schema,
                update_schema,
                update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[delete_schema, delete_schema](
                delete_schema,
                delete_schema,
                delete,
                CRUDAccessType.delete),
            )

    def _generate_session(self) -> PrintSession:
        """
        Generate a new session in case the user didn't specify one yet
        """
        return PrintSession()
