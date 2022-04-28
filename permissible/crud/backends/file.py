from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Tuple, Type, TypeVar, Union
from contextlib import asynccontextmanager
from pathlib import Path
from os import PathLike

from pydantic import BaseModel, Field
from permissible.core import BaseSession, InputSchema, OutputSchema
from permissible.crud.core import CRUDBackend, CRUDAccessType, CRUDBackendAccessRecord, \
        Create, Read, Update, Delete
from io import IOBase
from uuid import UUID, uuid4
from dataclasses import dataclass
from fasteners import InterProcessLock


class FileCreateSchema(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    file: IOBase
    class Config:
        arbitrary_types_allowed = True


class FileReadSchema(BaseModel):
    uuid: UUID


class FileUpdateSchema(BaseModel):
    uuid: UUID
    file: IOBase
    class Config:
        arbitrary_types_allowed = True


class FileDeleteSchema(BaseModel):
    uuid: UUID


class UUIDReturnSchema(BaseModel):
    uuid: UUID


class FileReturnSchema(BaseModel):
    uuid: UUID
    file: IOBase
    class Config:
        arbitrary_types_allowed = True


class FileSession(BaseSession):
    path: Path
    state: Dict[UUID, Tuple[Union[FileCreateSchema, FileUpdateSchema, FileDeleteSchema, None], InterProcessLock]]

    # TODO: can this be cleaned up?
    def __init__(self, path: Path):
        self.path = path
        self.state = {}
        super().__init__()

    # Currently add and query are not async, so they can never be scheduled concurrently
    # within a thread.  Therefore, we only utilise an inter-process lock
    def add(self, data: Union[FileCreateSchema, FileUpdateSchema, FileDeleteSchema]):
        path = self.path.joinpath(data.uuid.hex)
        if data.uuid not in self.state.keys():
            lock = InterProcessLock(path.with_suffix(".lock"))
            self.state[data.uuid] = (None, lock)
            # TODO: add configurable timeouts
            print(f'acquiring {data.uuid}')
            lock.acquire()

        exists = self._exists(data.uuid)
        if isinstance(data, FileCreateSchema) and exists:
            raise ValueError(f"File {path} already exists")
        elif isinstance(data, (FileUpdateSchema, FileDeleteSchema,)) and not exists:
            raise ValueError(f"File {path} does not exist")

        # Lock acquired
        self.state[data.uuid] = (data, self.state[data.uuid][1])

    # This commit isn't supposed to fail, because it's not supposed to enforce
    # whether files do/don't exist prior to being created/deleted/updated
    # It's permissible for a file being "deleted" to not exist (it may have
    # started by not existing, and then an intermediate but non-committed create operation occurred).
    # Similarly, an update operation may be working on a file that doesn't actually exist.
    # This can also occur for create operations - the file exists, and then an
    # intermediate but non-committed delete operation is added.

    # Commit can fail, however, if underlying guarantees about the filesystem
    # are violated (e.g. another program changes one of our file's permissions)
    # In this case, the process (e.g. a web server) should still remain alive.
    # We therefore need to rollback as best we can, by releasing all the locks
    # and then reporting the error.
    def commit(self):
        for (operation, lock) in self.state.values():
            path = self.path.joinpath(operation.uuid.hex)
            if isinstance(operation, (FileCreateSchema, FileUpdateSchema,)):
                try:
                    with open(path, "wb") as f:
                        f.write(operation.file.read())
                except Exception as e:
                    self.rollback()
                    raise e
                lock.release()
            elif isinstance(operation, FileDeleteSchema):
                path.unlink(missing_ok=True)
                lock.release()
                # Purge old lock files
                path.with_suffix(".lock").unlink(missing_ok=True)

    def rollback(self):
        for k, (_, lock) in self.state.items():
            lock.release()
        self.state = {}

    def _exists(self, uuid: UUID):
        path = self.path.joinpath(uuid.hex)
        if path.exists() and not path.is_file():
            raise ValueError(f"path {path} exists but is not a file")    # TODO: different error
        try:
            return isinstance(self.state[uuid][0], (FileCreateSchema, FileUpdateSchema,))
        except KeyError:
            return path.is_file()

    def query(self, data: Union[FileReadSchema, UUID]):
        uuid = data.uuid if isinstance(data, FileReadSchema) else data
        if uuid not in self.state.keys():
            lock = self.state[uuid] = (None, InterProcessLock(self.path.joinpath(uuid.hex).with_suffix(".lock")))
            # TODO: add configurable timeouts
            lock.acquire_lock()

        # Find the last point at which the file was created or updated
        path = self.path.joinpath(uuid.hex)
        if path.exists() and not path.is_file():
            raise ValueError(f"path {path} exists but is not a file")    # TODO: different error

        if uuid in self.state.keys() and self.state[uuid][0] is not None:
            return self.state[uuid][0]
        else:
            return open(self.path.joinpath(uuid.hex))


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
            CRUDBackendAccessRecord[FileReadSchema, FileReturnSchema](
                FileReadSchema,
                FileReturnSchema,
                self.read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[FileUpdateSchema, None](
                FileUpdateSchema,
                None,
                self.update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[FileDeleteSchema, None](
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
        session.add(data)
        return UUIDReturnSchema(uuid=data.uuid)

    def read(self, session: FileSession, data: FileReadSchema) -> FileReturnSchema:
        return session.query(data)

    def update(self, session: FileSession, data: FileUpdateSchema) -> None:
        session.add(data)

    def delete(self, session: FileSession, data: FileDeleteSchema) -> None:
        session.add(data)

    def _generate_session(self) -> FileSession:
        """
        Generate a new session in case the user didn't specify one yet
        """
        return FileSession(self.path)


# Provides a way of verifying that the uploaded file is, in fact, an image
# Also provides a way of processing the image prior to storage (e.g. to create a thumbnail)
# TODO: think about whether we actually want a media storage backend?
# It could have swappable parts to verify that it's video or audio or something

# We could build a backend router that looks at the schema type and routes accordingly
# to the backend of choice.  A generic one may well auto-implement for all of the possible
# actions, such as CRUD.

# Resources to build:
# * router backend - looks at the schema type and routes accordingly
#       Used to distinguishing video, image, sound, etc.
# * tee backend - sends the same stuff to multiple resources
#       Used to implement images + thumbnails being stored
# * splitter backend - looks at a key in a dict, and routes (to multiple) accordingly
#       Used to implement combined requests (e.g. update image file and database)
# * image backend - verifies that the file being sent to the backend is actually an image
#       Video, audio, etc. respective ones
#       Maybe this could instead be a generic file type checker?
#           Probably not, because we want to inspect the files, not just their extensions
# * image compressor backend
#       Used to actually make the thumbnails
