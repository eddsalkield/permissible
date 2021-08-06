from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Type, TypeVar, Union

from permissible.permissions import UnauthorisedError, Action, \
        BaseAccessType, Principal, Permission, has_permission


AccessType = TypeVar('AccessType', bound=BaseAccessType)
InputSchema = TypeVar('InputSchema', bound=BaseModel)
OutputSchema = TypeVar('OutputSchema', bound=BaseModel)
Session = TypeVar('Session')


# Backend definition

@dataclass(frozen=True)
class BackendAccessRecord(
        Generic[AccessType, InputSchema, OutputSchema, Session]):
    """
    An input argument to a Backend, defining a new type of access.
    """
    input_schema: Type[InputSchema]
    output_schema: Type[OutputSchema]
    process: Callable[[Session, InputSchema], OutputSchema]
    type_: AccessType


class Backend(Generic[AccessType, Session]):
    """
    Base class of all backends.

    Backends provide a method, known as an access, for each AccessType,
    which define the possible interactions with the backend's data store.
    """

    _access_records: Dict[AccessType, BackendAccessRecord] = {}
    _access_methods: \
        Dict[AccessType,
             Callable[[Any, Session], OutputSchema]] = {}

    def _register_access(
            self,
            r: BackendAccessRecord[AccessType, InputSchema, OutputSchema,
                                   Session]):
        """
        Internal method to register a new access with the backend.
        The details of the access are defined in r.
        The resulting access can be subsequently invoked through __call__.
        """
        def access(data: Any, session: Session) -> OutputSchema:
            return r.output_schema.parse_obj(r.process(
                session, r.input_schema.parse_obj(data)))
        return access

    def __init__(
            self,
            *recs: BackendAccessRecord[AccessType, InputSchema, OutputSchema,
                                       Session]):
        """
        Initialises a new Backend with methods as specified within the
        recs, in accordance with AccessType
        """
        for r in recs:
            self._access_records[r.type_] = r
            self._access_methods[r.type_] = self._register_access(r)

    def __call__(self, type_: AccessType, data: Any,
                 session: Session) -> OutputSchema:
        """
        Invoke an access on data of the given type within the context of the
        session.
        """

        return self._access_methods[type_](data, session)

    @contextmanager
    def generate_session(self) -> Generator[Session, None, None]:
        """
        Used to generate a new session in case the user didn't specify one yet.
        """
        raise NotImplementedError('Subclass implements this')


# Resource definition

AccessName = str

class BaseSession:
    def commit(self):
        raise NotImplementedError('Subclass implements this')

@dataclass(frozen=True)
class AccessRecord(
        Generic[AccessType, InputSchema, OutputSchema]):
    """
    An input argument to a Resource, defining a new type of access.

    This access is identified by name, and is callable only by users who
    fulfil permissions.

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
    _access_records: Dict[AccessType, Dict[AccessName, AccessRecord]] = \
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

        def access(
                data: Any,
                principals: List[Principal],
                session: Optional[BaseSession] = None) -> OutputSchema:
            # Check permissions
            if isinstance(r.permissions, list):
                # Evaluate static permissions
                if has_permission(principals, r.permissions) == Action.ALLOW:
                    processed_data = pre_process(
                            r.input_schema.parse_obj(data))
                    if session is None:
                        with self._backend.generate_session() as session:
                            output_data: BaseModel = \
                                self._backend(r.type_, processed_data, session)
                            session.commit()
                    else:
                        output_data = \
                            self._backend(r.type_, processed_data, session)
                    return r.output_schema.parse_obj(post_process(output_data))
                else:
                    raise UnauthorisedError
            else:
                # Evaluate dynamic permissions
                processed_data = pre_process(
                        r.input_schema.parse_obj(data))
                if session is None:
                    with self._backend.generate_session() as session:
                        output_data = \
                            self._backend(r.type_, processed_data, session)
                        if has_permission(principals, r.permissions(
                                output_data)) != Action.ALLOW:
                            raise UnauthorisedError
                        session.commit()
                else:
                    output_data = \
                        self._backend(r.type_, processed_data, session)
                    if has_permission(principals, r.permissions(
                            output_data)) != Action.ALLOW:
                        raise UnauthorisedError
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
            self._access_records[r.type_][r.name] = r
            method = self._register_access(r)
            self._access_methods[r.type_][r.name] = method
        self._backend = backend

    def __call__(self, type_: AccessType, name: AccessName, data: Any,
                 principals: List[Principal],
                 session: Optional[BaseSession] = None) -> OutputSchema:
        """
        Invoke an access on data of the given type and name, on a user with
        the given principals within the context of the session.
        """
        return self._access_methods[type_][name](data, principals, session)
