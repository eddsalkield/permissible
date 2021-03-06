from dataclasses import dataclass
from pydantic import BaseModel
from typing import TypeVar

from permissible.core import Backend, InputSchema, OutputSchema, \
        BackendAccessRecord, AccessRecord, Resource
from permissible.permissions import BaseAccessType


class CRUDAccessType(BaseAccessType):
    """
    Defines the possible access types for CRUD accesses.
    """
    create = 'create'
    read = 'read'
    update = 'update'
    delete = 'delete'


class CRUDBackend(Backend[CRUDAccessType]):
    """
    Base class of all backends that define CRUD operations.
    """
    ...


class CRUDBackendAccessRecord(
        BackendAccessRecord[CRUDAccessType, InputSchema, OutputSchema]):
    """
    An input argument to a CRUDBackend, defining a new type of access.
    """
    ...


@dataclass(frozen=True)
class Create(AccessRecord[CRUDAccessType, InputSchema, OutputSchema]):
    """
    Create AccessRecord for CRUD
    """
    type_: CRUDAccessType = CRUDAccessType.create


@dataclass(frozen=True)
class Read(AccessRecord[CRUDAccessType, InputSchema, OutputSchema]):
    """
    Read AccessRecord for CRUD
    """
    type_: CRUDAccessType = CRUDAccessType.read


@dataclass(frozen=True)
class Update(AccessRecord[CRUDAccessType, InputSchema, OutputSchema]):
    """
    Update AccessRecord for CRUD
    """
    type_: CRUDAccessType = CRUDAccessType.update


@dataclass(frozen=True)
class Delete(AccessRecord[CRUDAccessType, InputSchema, OutputSchema]):
    """
    Delete AccessRecord for CRUD
    """
    type_: CRUDAccessType = CRUDAccessType.delete


class CRUDResource(Resource[CRUDAccessType]):
    """
    Base class of all CRUD resources.

    CRUDResources provide methods, known as accesses, that define a
    Create, Read, Update, or Delete interaction with the backend data store,
    which are permissible only to users that satisfy the permissions of the
    given access.

    """
    async def create(self, *args, **kwargs) -> OutputSchema:
        return await super().__call__(CRUDAccessType.create, *args, **kwargs)

    async def read(self, *args, **kwargs) -> OutputSchema:
        return await super().__call__(CRUDAccessType.read, *args, **kwargs)

    async def update(self, *args, **kwargs) -> OutputSchema:
        return await super().__call__(CRUDAccessType.update, *args, **kwargs)

    async def delete(self, *args, **kwargs) -> OutputSchema:
        return await super().__call__(CRUDAccessType.delete, *args, **kwargs)

# Type variables CRUD schema


CreateSchema = TypeVar('CreateSchema', bound=BaseModel)
ReadSchema = TypeVar('ReadSchema', bound=BaseModel)
UpdateSchema = TypeVar('UpdateSchema', bound=BaseModel)
DeleteSchema = TypeVar('DeleteSchema', bound=BaseModel)
