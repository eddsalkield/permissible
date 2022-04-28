from fastapi import FastAPI, File, UploadFile, Depends
from pydantic import BaseModel, create_model, Field

from permissible import Transaction, Resource, CRUDResource, PrintCRUDBackend, \
        Create, Read, Update, Delete, Action, Permission, Principal

from typing import List, Annotated, Union, Type, Literal, Callable, Generator, Optional, Any
from mutate_function import replace_arg

app = FastAPI()

# NB: we can add the transaction using the dependency resolution system as well
# by adding it as an argument e.g. transaction = transaction_manager()
# The context system will clean up the transaction for us, which is very nice

# TODO: add ABC for the "data" attribute
def _flatten(model: Type[BaseModel]) -> Type[BaseModel]:
    """
    Flatten out the model to contain a "flattened" set (for files)
    And the remainder of the model (under a field called "data"
    """
    # This flattened model will be used as the base of another model that contains
    # enum for access_type, which needs to get serialised
    class Config:
        use_enum_values = True

    flattened = [] # TODO: implement flattening out files
    # TODO: does this need a dynamic name?  Maybe not, because it's scoped in _flatten?
    # TODO: better name for the model
    return create_model(f"Model.{model.__name__}", data=(model, ...), *flattened, __config__=Config)


# TODO: it's imperative that the input model does NOT allow any extraneous attributes
# It must be strict in what it accepts, or else somebody could write data they're not
# supposed to into the model using dot syntax
# Ideally this would be taken care of by the underlying model in Permissible, but
# we're not taking any chances.
def _unflatten(model: BaseModel) -> BaseModel:
    return model.data

# Duplicated from core.py, because FastAPI can't deal with actual context managers
# as dependencies
def _transaction_manager():
    try:
        t = Transaction()
        yield t
    except Exception as e:
        t.rollback(e)
    finally:
        t.commit()

class Dependable:
    """ Converts a pydantic model into a FastAPI dependency
    Uses contextmanager dependencies to ensure that the session is created, handled,
    and destroyed correctly
    Flattens out of the nested model any fields that are required for the special
    case of a file (detected as subclasses of io.IOBase)
    Parameters to flatten for query/path parameters are out of scope
    (and may be handled by some helper function later down the line)
    Flattened parameters are addressed by dot-delimited names
    Filenames can contain dots if the dot is escaped (\.)
    """
    def __init__(self, resource: Resource):
        self.resource = resource

        class Config:
            use_enum_values = True

        access_models = []
        for access_type, accesses in resource.access_records.items():
            access = tuple((
                        create_model(
                            f"Access.{access_type}.{access_name}",
                            access_type=(Literal[access_type], access_type),
                            access_name=(Literal[access_name], access_name),
                            data=(access.input_schema, ...),
                        ) for access_name, access in accesses.items()))
            if len(access) > 1:
                access_models.append(
                    Annotated[Union[access], Field(discriminator="access_name")])
            elif len(access) == 1:
                access_models.append(access[0])
        
        if len(access_models) == 1:
            root = access_models[0]
        else:
            root = Annotated[Union[tuple(access_models)], Field(discriminator="access_type")]

        input_schema = create_model(
            "InputSchema",
            __root__ = (root, ...),
            __config__ = Config # TODO: is this required?
        )
        
        output_schema = Union[tuple((val.output_schema for sublist in resource.access_records.values() for val in sublist.values()))]

        async def __call__(
                self,
                principals: List[Principal],    # TODO: find a way of raising a warning
                                                # if these aren't provided by dependency
                data: input_schema = Depends(input_schema),
                transaction: Transaction = Depends(_transaction_manager)) -> output_schema:
            return await self.resource(
                    type_=data.__root__.access_type,
                    name=data.__root__.access_name,
                    data=data.__root__.data,
                    principals=principals,
                    transaction=transaction)

        setattr(self.__class__, "__call__", __call__)

        # Create other helper functions based on access_type. Partial function on __call__?
        

class Profile(BaseModel):
    """
    The base model, defining the complete attributes of a profile.
    """
    full_name: str
    age: int

class CreateProfile(BaseModel):
    """
    The restricted interface for creating profiles.
    Does not permit the user to select an age.
    """
    full_name: str


# Create the profile backend
ProfileBackend = PrintCRUDBackend(
    Profile, Profile, Profile, Profile
)

# Create the profile resource
ProfileResource = CRUDResource(
        # Admin interface to create profiles
        Create[Profile, Profile](
            name='admin_create',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'admin'))],
            input_schema=Profile,
            output_schema=Profile
        ),
        # Restricted interface to create profiles
        Create[CreateProfile, CreateProfile](
            name='restricted_create',
            permissions=[Permission(Action.ALLOW, Principal('group', 'user'))],
            input_schema=CreateProfile,
            output_schema=CreateProfile,
            pre_process=lambda x: Profile(full_name=x.full_name, age=23),
            post_process=lambda x: CreateProfile(full_name=x.full_name)
        ),
        backend=ProfileBackend
    )

profile_depends = Dependable(ProfileResource)

@app.post("/test/", response_model=None)
async def create_upload_file(profile: Any = Depends(profile_depends), principals: List[Principal] = [Principal('group', 'user')]):
    return None


class Model(BaseModel):
    hello: str
    world: int

@app.post("/test2/")
async def create_upload_file(model: Model = Depends()):
    return {"filename": model.hello}

@app.post("/files/")
async def create_file(file: bytes = File(...)):
    return {"file_size": len(file)}


# NB: currently only "succeeds" with request:
# {
#     "__root__": {
#       "data": {
#         "full_name": "string",
#         "age": 0
#       },
#       "access_type": "create",
#       "access_name": "restricted_create"
#     }
# }

# Seems there's a bug in __root__ model handling somewhere...
# https://github.com/tiangolo/fastapi/issues/911
# https://github.com/samuelcolvin/pydantic/issues/2100
# https://github.com/tiangolo/fastapi/issues/1437

# Outstanding issues:
# * Each request requires an explicit __root__
# * access_name always expects the value that you don't give it
# * flatten isn't implemented
