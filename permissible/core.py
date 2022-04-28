from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Type, TypeVar, Union

from permissible.permissions import UnauthorisedError, Action, \
        BaseAccessType, Principal, Permission, has_permission
import asyncio


# From fastapi users
async def run_handler(handler: Callable, *args, **kwargs):
    if asyncio.iscoroutinefunction(handler):
        return await handler(*args, **kwargs)
    else:
        return handler(*args, **kwargs)

class BaseSession:
    """
    Write the session out persistently.  This action is not permitted to fail.
    Any integrity checks and lock obtaining must happen before the commit.
    The behaviour of running both commit and rollback is undefined.
    """
    def commit(self):
        raise NotImplementedError('Subclass implements this')

    def rollback(self):
        raise NotImplementedError('Subclass implements this')

    def close(self):
        raise NotImplementedError('Subclass implements this')

AccessType = TypeVar('AccessType', bound=BaseAccessType)
InputSchema = TypeVar('InputSchema', bound=BaseModel)
OutputSchema = TypeVar('OutputSchema', bound=BaseModel)
Session = TypeVar('Session', bound=BaseSession)

class Transaction():
    """
    Represents a collection of sessions, providing the guarantee that on commit,
    either all of the sessions commit, or none of them do.
    The behaviour of running both commit and rollback is undefined.
    """
    sessions: Dict[str, BaseSession] = {} # TODO: there's a problem with this rather than initialising in __init__
                    # What was it?...

    def __setitem__(self, key: str, session: Session) -> Session:
        self.sessions[key] = session
        return session

    def __getitem__(self, key: str) -> Session:
        return self.sessions[key]

    def commit(self):
        # TODO: could this be an async for, considering it's over IO?
        for session in self.sessions.values():
            # TODO: we may need run_handler here if the session's method can be optionally async
            session.commit()
    
    def rollback(self, error: Exception):
        for session in self.sessions.values():
            session.rollback()
        raise error
    
    def close(self):
        for session in self.sessions.values():
            session.close()

@contextmanager
def transaction_manager():
    try:
        t = Transaction()
        yield t
    except Exception as e:
        t.rollback(e)
    finally:
        t.commit()

# Backend definition

@dataclass(frozen=True)
class BackendAccessRecord(
        Generic[AccessType, InputSchema, OutputSchema]):
    """
    An input argument to a Backend, defining a new type of access.
    """
    input_schema: Type[InputSchema]
    output_schema: Type[OutputSchema]
    process: Callable[[Session, InputSchema], OutputSchema]
    type_: AccessType


class Backend(Generic[AccessType]):
    """
    Base class of all backends.

    Backends provide a method, known as an access, for each AccessType,
    which define the possible interactions with the backend's data store.
    """

    # Indexing by AccessType to speed up lookups by Resources
    access_records: Dict[AccessType, BackendAccessRecord] = {}
    _access_methods: \
        Dict[AccessType,
             Callable[[Any, BaseSession], OutputSchema]] = {}

    def _register_access(
            self,
            r: BackendAccessRecord[AccessType, InputSchema, OutputSchema]):
        """
        Internal method to register a new access with the backend.
        The details of the access are defined in r.
        The resulting access can be subsequently invoked through __call__.
        """
        async def access(data: Any, session: BaseSession) -> OutputSchema:
            processed_data = await run_handler(r.process, session, r.input_schema.parse_obj(data))
            if r.output_schema is None:
                return None
            else:
                return r.output_schema.parse_obj(processed_data)
        return access

    def __init__(
            self,
            *recs: BackendAccessRecord[AccessType, InputSchema, OutputSchema]):
        """
        Initialises a new Backend with methods as specified within the
        recs, in accordance with AccessType
        """
        print("adding access records")
        for r in recs:
            print(f"{r.type_}")
            self.access_records[r.type_] = r
            self._access_methods[r.type_] = self._register_access(r)

    async def __call__(self, type_: AccessType, data: Any,
                 transaction: Transaction) -> OutputSchema:
        """
        Invoke an access on data of the given type within the context of the
        session.
        """
        try:
            session = transaction[self.__class__.__name__]
        except KeyError:
            session = transaction[self.__class__.__name__] = self._generate_session()
        return await self._access_methods[type_](data, session)

    @contextmanager
    def _generate_session(self) -> BaseSession:
        """
        Used to generate a new session in case the user didn't specify one yet.
        It's a context manager to enable the session to be cleaned up in a
        finally block.
        """
        raise NotImplementedError('Subclass implements this')


# Resource definition

AccessName = str

@dataclass(frozen=True)
class AccessRecord(
        Generic[AccessType, InputSchema, OutputSchema]):
    """
    An input argument to a Resource, defining a new type of access.

    This access is identified by name, and is callable only by users who
    fulfil its permissions.

    The access expects data of type input_schema, which is processed
    by pre_process before being passed to the backend of the Resource.

    The access returns data of type output_schema, which is processed
    by post_process before being returned to the caller.
    """
    name: AccessName
    permissions: Union[
            List[Permission], 
            Callable[[BaseModel], List[Permission]]]
    input_schema: Type[InputSchema]
    output_schema: Type[OutputSchema]
    type_: AccessType
    pre_process: Optional[Callable[[InputSchema], BaseModel]] = None
    post_process: Optional[Callable[[BaseModel], OutputSchema]] = None


class Resource(Generic[AccessType]):
    """
    Base class of all resources.

    Resources provide methods, known as accesses, that define an interaction
    with the backend data store, which are permissible only to users that
    satisfy the permissions of the given access.
    """

    _backend: Backend
    # Indexing by AccessType and AccessName to speed up lookups by Resources and callers
    access_records: Dict[AccessType, Dict[AccessName, AccessRecord]] = \
        defaultdict(dict)
    _access_methods: \
        Dict[
            AccessType,
            Dict[
                AccessName,
                Callable[[Any, List[Principal], Optional[BaseSession]],
                         OutputSchema]
            ]] = defaultdict(dict)

    def _register_access(
            self,
            r: AccessRecord[AccessType, InputSchema, OutputSchema]):
        """
        Internal method to register a new access with the resource.
        The details of the access are defined in r.
        The resulting access can be subsequently invoked through __call__.
        """
        def pre_process(data):
            if r.pre_process is None:
                return data
            else:
                return r.pre_process(data)

        def post_process(data):
            if r.post_process is None:
                return data
            else:
                return r.post_process(data)

        async def access(
                data: Any,
                principals: List[Principal],
                transaction: Optional[Transaction] = None) -> OutputSchema:
            # Check permissions
            if isinstance(r.permissions, list):
                # Evaluate static permissions
                if has_permission(principals, r.permissions) == Action.ALLOW:
                    processed_data = pre_process(r.input_schema.parse_obj(data))
                    if transaction is None:
                        with transaction_manager() as transaction:
                            output_data = await self._backend(r.type_, processed_data, transaction)

                    else:
                        output_data = await self._backend(r.type_, processed_data, transaction)
                    if r.output_schema is None:
                        return None
                    else:
                        return r.output_schema.parse_obj(post_process(output_data))
                else:
                    raise UnauthorisedError
            else:
                # Evaluate dynamic permissions
                processed_data = pre_process(
                        r.input_schema.parse_obj(data))
                if transaction is None:
                    with transaction_manager() as transaction:
                        output_data = await self._backend(r.type_, processed_data, transaction)
                        if has_permission(principals, r.permissions(
                                output_data)) != Action.ALLOW:
                            raise UnauthorisedError
                else:
                    output_data = self._backend(r.type_, processed_data, transaction)
                    if has_permission(principals, r.permissions(
                            output_data)) != Action.ALLOW:
                        raise UnauthorisedError
                if r.output_schema is None:
                    return None
                else:
                    return r.output_schema.parse_obj(post_process(output_data))

        return access

    def __init__(self,
                 *recs: AccessRecord[AccessType, InputSchema, OutputSchema],
                 backend: Backend):
        """
        Initialises a new Resource with methods as specified within the
        recs, in accordance with AccessType
        """
        for r in recs:
            self.access_records[r.type_][r.name] = r
            method = self._register_access(r)
            self._access_methods[r.type_][r.name] = method
        self._backend = backend

    async def __call__(
            self,
            type_: AccessType,
            name: AccessName,
            data: Any,
            principals: List[Principal],
            transaction: Optional[Transaction] = None
        ) -> OutputSchema:
        """
        Invoke an access on data of the given type and name, on a user with
        the given principals within the context of the transaction.
        """
        return await run_handler(self._access_methods[type_][name], data, principals, transaction)
