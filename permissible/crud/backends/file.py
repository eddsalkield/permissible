from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Tuple, Type, TypeVar, Union
from contextlib import asynccontextmanager
from pathlib import Path
from os import PathLike

from pydantic import BaseModel, Field
from permissible.core import BaseSession, InputSchema, OutputSchema
from permissible.crud.core import CRUDBackend, CreateSchema, ReadSchema, \
        UpdateSchema, DeleteSchema, CRUDAccessType, CRUDBackendAccessRecord, \
        Create, Read, Update, Delete
from io import BufferedReader
from uuid import UUID, uuid4
from dataclasses import dataclass
from fasteners import InterProcessReaderWriterLock



class FileCreateSchema(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    file: BufferedReader
    class Config:
        arbitrary_types_allowed = True


class FileReadSchema(BaseModel):
    uuid: UUID


class FileUpdateSchema(BaseModel):
    uuid: UUID
    file: BufferedReader
    class Config:
        arbitrary_types_allowed = True


class FileDeleteSchema(BaseModel):
    uuid: UUID


class UUIDReturnSchema(BaseModel):
    uuid: UUID


class FileReturnSchema(BaseModel):
    uuid: UUID
    file: BufferedReader
    class Config:
        arbitrary_types_allowed = True


class FileSession(BaseSession):
    path: Path
    state: Dict[UUID, Tuple[List[Union[FileCreateSchema, FileUpdateSchema, FileDeleteSchema]], InterProcessReaderWriterLock]]

    # TODO: can this be cleaned up?
    def __init__(self, path: Path):
        self.path = path
        self.state = {}
        super().__init__()

    # Currently add and query are not async, so they can never be scheduled concurrently
    # within a thread.  Therefore, we only utilise an inter-process lock
    def add(self, data: Union[FileCreateSchema, FileUpdateSchema, FileDeleteSchema]):
        if data.uuid not in self.state.keys():
            lock = self.state[data.uuid] = ([], InterProcessLock(self.path.joinpath(data.uuid.hex)))
            # TODO: add configurable timeouts
            lock.acquire()

        # Lock acquired
        self.state[data.uuid][0].append(data)

    # Although commit is supposed to not fail, in the case that it does, the process
    # (e.g. a web server) should still remain alive.  We therefore need to rollback
    # as best we can, by releasing all the locks and then reporting the error.
    def commit(self):
        print("writing files...")
        # TODO: actually write the files
        for (operations, lock) in self.state.values():
            for operation in operations:
                path = self.path.joinpath(operation.uuid.hex)
                if isinstance(operation, FileCreateSchema):
                    try:
                        with open(path, "xb") as f:
                            f.write(operation.file)
                    except Exception as e:
                        self.rollback()
                        raise e
                elif isinstance(operation, FileUpdateSchema):
                    # Assert that the file already exists
                    if not path.is_file():
                        # TODO: check this is the correct error message
                        raise FileNotFoundError(f"Can't update non-existant file {path}")
                    try:
                        with open(self.path.joinpath(operation.uuid.hex), "wb") as f:
                            f.write(operation.file)
                    except Exception as e:
                        self.rollback()
                        raise e
                elif isinstance(operation, FileDeleteSchema):
                    path.unlink()
                else:
                    self.rollback()
                    assert(False)
            lock.release()

    def rollback(self):
        for (_, lock) in self.state:
            lock.release()

    def query(self, FileReadSchema):
        if data.uuid not in self.state.keys():
            lock = self.state[data.uuid] = ([], InterProcessLock(self.path.joinpath(data.uuid.hex)))
            # TODO: add configurable timeouts
            lock.acquire_lock()
        return open(self.path.joinpath(data.uuid.hex))


@dataclass(frozen=True)
class FileCreate(Create[FileCreateSchema, UUIDReturnSchema]):
    """
    Create AccessRecord for CRUD files
    """
    input_schema: Type[InputSchema] = FileCreateSchema
    output_schema: Type[OutputSchema] = UUIDReturnSchema


@dataclass(frozen=True)
class FileRead(Read[FileReadSchema, FileReturnSchema]):
    """
    Read AccessRecord for CRUD files
    """
    input_schema: Type[InputSchema] = FileReadSchema
    output_schema: Type[OutputSchema] = FileReturnSchema


@dataclass(frozen=True)
class FileUpdate(Update[FileCreateSchema, None]):
    """
    Update AccessRecord for CRUD files
    """
    input_schema: Type[InputSchema] = FileUpdateSchema
    output_schema: Type[OutputSchema] = None


@dataclass(frozen=True)
class FileDelete(Delete[FileDeleteSchema, None]):
    """
    Delete AccessRecord for CRUD files
    """
    input_schema: Type[InputSchema] = FileDeleteSchema
    output_schema: Type[OutputSchema] = None


class FileCRUDBackend(
        CRUDBackend):
    """
    A test CRUD backend, providing accesses to simply print the given data.
    """
    def create(self, session: FileSession, data: FileCreateSchema) -> UUIDReturnSchema:
        raise NotImplementedError("Sublass implements this")

    def read(self, session: FileSession, data: FileReadSchema) -> FileReturnSchema:
        raise NotImplementedError("Sublass implements this")

    def update(self, session: FileSession, data: FileUpdateSchema) -> None:
        raise NotImplementedError("Sublass implements this")

    def delete(self, session: FileSession, data: FileDeleteSchema) -> None:
        raise NotImplementedError("Sublass implements this")

    def __init__(self):
        print("calling init")
        super().__init__(
            CRUDBackendAccessRecord[FileCreateSchema, UUIDReturnSchema](
                FileCreateSchema,
                UUIDReturnSchema,
                self.create,
                CRUDAccessType.create),
            CRUDBackendAccessRecord[ReadSchema, FileReturnSchema](
                FileReadSchema,
                FileReturnSchema,
                self.read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[UpdateSchema, None](
                FileUpdateSchema,
                None,
                self.update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[DeleteSchema, None](
                FileDeleteSchema,
                None,
                self.delete,
                CRUDAccessType.delete),
            )


class LocalFileCRUDBackend(FileCRUDBackend):
    def __init__(self, path: PathLike):
        self.path = Path(path)
        if self.path.is_file():
            raise ValueError(f"path {path} must be a directory")
        self.path.mkdir(parents=True, exist_ok=True) # TODO: check mode (defaulting to 511)
        super().__init__()

    def create(self, session: FileSession, data: FileCreateSchema) -> UUIDReturnSchema:
        filepath = self.path.joinpath(data.uuid.hex)
        if filepath.exists():
            raise ValueError(f"File at {filepath} already exists")
        with open(filepath, "wb") as f:
            f.write(data.file.read())
        data.file.close()
        return UUIDReturnSchema(uuid=data.uuid)

    def read(self, session: FileSession, data: FileReadSchema) -> FileReturnSchema:
        return FileReturnSchema(
            uuid = data.uuid,
            file = open(self.path.joinpath(data.uuid.hex), "rb"))

    def update(self, session: FileSession, data: FileUpdateSchema) -> None:
        filepath = self.path.joinpath(data.uuid.hex)
        if not filepath.exists():
            raise ValueError(f"File at {filepath} does not exist")
        with open(filepath, "wb") as f:
            f.write(data.file.read())
        data.file.close()

    def delete(self, session: FileSession, data: FileDeleteSchema) -> None:
        self.path.joinpath(data.uuid.hex).unlink()

    def _generate_session(self) -> FileSession:
        """
        Generate a new session in case the user didn't specify one yet
        """
        return FileSession(self.path)
