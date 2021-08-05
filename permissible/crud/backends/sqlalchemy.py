from typing import Any, Callable, Dict, Generator, Generic, List, Optional, \
                   Type, TypeVar, Union
from contextlib import contextmanager

from permissible.core import BaseSession
from permissible.crud.core import CRUDBackend, CreateSchema, ReadSchema, \
        UpdateSchema, DeleteSchema, CRUDAccessType, CRUDBackendAccessRecord
from pydantic import BaseModel, create_model, BaseConfig, validator
from pydantic_sqlalchemy import sqlalchemy_to_pydantic
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy_filters import apply_filters

from enum import Enum
import uuid

class ValueOperation(Enum):
    equal = '=='
    equal_alt = 'eq'
    not_equal = '!='
    not_equal_alt = 'ne'
    greater_than = '>'
    greater_than_alt = 'gt'
    less_than = '<'
    less_than_alt = 'lt'
    greater_than_or_eq = '>='
    greater_than_or_eq_alt = 'ge'
    less_than_or_eq = '<='
    less_than_or_eq_alt = 'le'
    like = 'like'
    ilike = 'ilike'
    not_ilike = 'not_ilike'
    in_ = 'in'
    not_in = 'not_in'
    any_ = 'any'
    not_any = 'not_any'

class NullOperation(Enum):
    is_null = 'is_null'
    is_not_null = 'is_not_null'

class QuerySchema(BaseModel):
    class Filter(BaseModel):
        field: str
        op: Union[NullOperation, ValueOperation]
        value: Union[str, type(...)] = ...
        class Config:
            extra = 'forbid'
            arbitrary_types_allowed = True
        @validator('value', always = True)
        def value_is_none(cls, value, values):
            if 'op' in values:
                operation = values['op']
            else:
                raise ValueError('op must be provided')
            if isinstance(operation, NullOperation):
                if value != ...:
                    raise ValueError(f'For this operation {operation} value must be ...')
                else:
                    return value
            else:
                if value == ...:
                    raise ValueError(f'For this operation {operation} value must not be ...')
                else:
                    return value
    
    #Need to allow for booleans!!!
    filter_spec: List[Filter] = []



# TODO: get from webplatform helpers
def get_type_from_sql_object(sql_object):
    # Adapted from pydantic sqlalchemy
    python_type = None
    if hasattr(sql_object.type, "impl"):
        if hasattr(sql_object.type.impl, "python_type"):
            python_type = sql_object.type.impl.python_type
    elif hasattr(sql_object.type, "python_type"):
        python_type = sql_object.type.python_type
    return python_type


def get_primary_keys_from_table(Table) -> Dict[str, Any]:
    primary_keys = {}
    for primary_key in inspect(Table.__table__).primary_key:
        python_type = get_type_from_sql_object(primary_key)
        if python_type is None:
            raise ValueError(f'Type could not determined for {primary_key.name}')
        else:
            primary_keys[primary_key.name] = python_type
    return primary_keys


# Custom exceptions
class AlreadyExistsError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__('Table record already exists', *args, **kwargs)


class ORMConfig(BaseConfig):
    orm_mode = True


class SQLAlchemyCRUDBackend(CRUDBackend[Session]):
    """
    A CRUD backend for SQLAlchemy operations on the contents of a given table.
    """
    SessionLocal: Session    # TODO: type
    Model: Any               # TODO: typing according to declarative_base

    def _get_by_primary_keys(self, session, data):
        return session.query(self.Model).filter_by(
                **{k: data[k] for k, _ in self.primary_keys.items()}) \
                .all()

    def __init__(
            self,
            Model: Any,  # TODO: type
            SessionLocal: Session):

        self.SessionLocal = SessionLocal
        self.Model = Model
        # Sets orm_mode=True by default
        self.Schema = sqlalchemy_to_pydantic(Model)
        self.primary_keys: Dict[str, Any] = get_primary_keys_from_table(Model)

        self.DeleteSchema = create_model(
            f'{Model.__name__}.Delete', __config__=ORMConfig,
            **{n: (t, ...) for n, t in self.primary_keys.items()})  # type: ignore

        def create(session: Session, data: BaseModel) -> BaseModel:
            # TODO: do we need to cast data to self.Schema here?
            # Test if record with matching primary keys already exists
            if len(self._get_by_primary_keys(session, data)) != 1:
                raise AlreadyExistsError()
            # Create record in session
            model = self.Model(data.dict())
            session.add(model)
            return data

        def read(session: Session, data: QuerySchema) -> List[BaseModel]:
            query_obj = session.query(Model)
            apply_filters(query_obj, data.filter_spec)
            print(f'Reading {data}')
            return data

        def update(session: Session, data: BaseModel) -> BaseModel:
            model = self._get_by_primary_keys(session, data)
            for item, value in data.dict().items():
                setattr(model, item, value)
            return self.Schema(model)

        def delete(session: Session, data: BaseModel) -> None:
            delete_args = self.DeleteSchema(**data.dict())
            model = self._get_by_primary_keys(session, delete_args)
            session.delete(model)

        super().__init__(
            CRUDBackendAccessRecord[CreateSchema, CreateSchema, Session](
                create_schema,
                create_schema,
                create,
                CRUDAccessType.create),
            CRUDBackendAccessRecord[ReadSchema, ReadSchema, Session](
                read_schema,
                read_schema,
                read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[UpdateSchema, UpdateSchema, Session](
                update_schema,
                update_schema,
                update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[DeleteSchema, DeleteSchema, Session](
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
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()
