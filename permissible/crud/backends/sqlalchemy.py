from typing import Any, Dict, Generator, List, Union, ForwardRef
from contextlib import contextmanager
from permissible.core import BaseSession
from permissible.crud.core import CRUDBackend, CRUDAccessType, CRUDBackendAccessRecord
from pydantic import BaseModel, create_model, BaseConfig, conlist
from pydantic_sqlalchemy import sqlalchemy_to_pydantic
from sqlalchemy import inspect
from sqlalchemy.orm import Session
from sqlalchemy_filters import apply_filters
from enum import Enum


class BinOp(Enum):
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


class MonOp(Enum):
    is_null = 'is_null'
    is_not_null = 'is_not_null'


class BinFilter(BaseModel):
    field: str
    # TODO Make enum of available fields
    op: BinOp
    value: Any
    class Config:
        use_enum_values = True

class MonFilter(BaseModel):
    field: str
    # TODO Make enum of available fields
    op: MonOp
    class Config:
        use_enum_values = True

AndFilter = ForwardRef('AndFilter')
OrFilter = ForwardRef('OrFilter')
NotFilter = ForwardRef('NotFilter')

Filter = Union[BinFilter, MonFilter, AndFilter, OrFilter, NotFilter]

class AndFilter(BaseModel):
    class Config:
        fields = {'and_': 'and'}
    and_: conlist(Filter, min_items=1)


class OrFilter(BaseModel):
    class Config:
        fields = {'or_': 'or'}
    or_: conlist(Filter, min_items=1)


class NotFilter(BaseModel):
    class Config:
        fields = {'not_': 'not'}
    not_: conlist(Filter, min_items=1, max_items=1)

AndFilter.update_forward_refs()
OrFilter.update_forward_refs()
NotFilter.update_forward_refs()
    

class QuerySchema(BaseModel):
    filter_spec: List[Filter]
    #sort_spec
    #pagination_spec


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

class MultipleRecordsError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__('Table record non unique', *args, **kwargs)

class NotFoundError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__('Table record not found', *args, **kwargs)

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
        self.Schema = sqlalchemy_to_pydantic(Model)
        self.primary_keys: Dict[str, Any] = get_primary_keys_from_table(Model)
        self.DeleteSchema = create_model(
            f'{Model.__name__}.Delete', __config__=ORMConfig,
            **{n: (t, ...) for n, t in self.primary_keys.items()})  # type: ignore
        class OutputQuerySchema(BaseModel):
            results: List[self.Schema]
        self.OutputQuerySchema = OutputQuerySchema

        def create(session: Session, data: self.Schema) -> BaseModel:
            results = self._get_by_primary_keys(session, data.dict())
            if len(results) == 1:
                raise AlreadyExistsError()
            elif len(results) > 1:
                raise MultipleRecordsError()
            model = self.Model(**data.dict())
            session.add(model)
            return self.Schema.from_orm(model)

        def read(session: Session, data: QuerySchema) -> List[BaseModel]:
            query_obj = session.query(Model)
            filtered_query_obj = apply_filters(query_obj, data.dict()['filter_spec']).all()
            return OutputQuerySchema(results = [self.Schema.from_orm(i) for i in filtered_query_obj])

        def update(session: Session, data: self.Schema) -> BaseModel:
            results = self._get_by_primary_keys(session, data.dict())
            if len(results) == 0:
                raise NotFoundError()
            elif len(results) > 1:
                raise MultipleRecordsError()
            model = results[0]
            for item, value in data.dict().items():
                setattr(model, item, value)
            return self.Schema.from_orm(model)

        def delete(session: Session, data: self.DeleteSchema) -> None:
            delete_args = self.DeleteSchema(**data.dict()).dict()
            results = self._get_by_primary_keys(session, delete_args)
            if len(results) == 0:
                raise NotFoundError()
            elif len(results) > 1:
                raise MultipleRecordsError()
            model = results[0]
            session.delete(model)
            return self.Schema.from_orm(model)
                
        super().__init__(
            CRUDBackendAccessRecord[self.Schema, self.Schema, Session](
                self.Schema,
                self.Schema,
                create,
                CRUDAccessType.create),
            CRUDBackendAccessRecord[QuerySchema, OutputQuerySchema, Session](
                QuerySchema,
                OutputQuerySchema,
                read,
                CRUDAccessType.read),
            CRUDBackendAccessRecord[self.Schema, List[self.Schema], Session](
                self.Schema,
                self.Schema,
                update,
                CRUDAccessType.update),
            CRUDBackendAccessRecord[self.DeleteSchema, self.DeleteSchema, Session](
                self.DeleteSchema,
                self.Schema,
                delete,
                CRUDAccessType.delete),
            )
    

    @contextmanager
    def generate_session(self) -> Generator[Session, None, None]:
        """
        Generate a new session in case the user didn't specify one yet
        """
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()
