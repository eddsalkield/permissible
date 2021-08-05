from permissible.crud.backends.sqlalchemy import QuerySchema, AlreadyExistsError



#print(QuerySchema(filter_spec = filter_spec))



from pydantic import BaseModel
from typing import Callable, Generator, Optional, Type
from contextlib import contextmanager

from permissible import CRUDResource, SQLAlchemyCRUDBackend, \
        Create, Read, Update, Delete, Action, Permission, Principal


from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, String, Text, Column, Integer
# In this example, we define a Profile resource, which is accessible
# through admin and restricted accesses
# The admin accesses are accessible only to users in the admin group, and
# provide complete access to modify the model.
# The restricted accesses are accessible to standard users, but can only
# perform limited modifications.


class Profile(BaseModel):
    """
    The base model, defining the complete attributes of a profile.
    """
    full_name: str
    age: int

class UpdateProfile(BaseModel):
    full_name: str
    age: int

class DeleteProfile(BaseModel):
    full_name: str

class CreateProfile(BaseModel):
    """
    The restricted interface for creating profiles.
    Does not permit the user to select an age.
    """
    full_name: str

DATABASE_URL = "sqlite:///./test.db"

declarative_base_instance: DeclarativeMeta = declarative_base()
engine = create_engine(
    DATABASE_URL,
    connect_args={'check_same_thread': False}
)
Session = sessionmaker(bind=engine)


class BackModel(declarative_base_instance):
    __tablename__ = 'Test_table'
    full_name = Column(Text(), primary_key = True)
    age = Column(Integer())


declarative_base_instance.metadata.create_all(engine)


ProfileBackend = SQLAlchemyCRUDBackend(BackModel, Session)
Profile = ProfileBackend.Schema
DeleteProfile = ProfileBackend.DeleteSchema
session = Session()
try:
    ProfileBackend.create(session, Profile(full_name=  'Johnny English22', age = 42))
except AlreadyExistsError:
    pass
session.commit()
ProfileBackend.update(session, Profile(full_name=  'Johnny English23', age = 23))
session.commit()
filter_spec = [{'field': 'age', 'op': 'lt', 'value': '100'}]

return_obj = ProfileBackend.read(session, QuerySchema(filter_spec = filter_spec))

ProfileBackend.delete(session, DeleteProfile(full_name = 'Johnny English23'))

print(return_obj)
"""
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

# Invoke admin_create to create a new profile as an administrative user
ProfileResource.create(
        'admin_create',
        {'full_name': 'Johnny English', 'age': 58},
        principals=[Principal('group', 'admin')],
        session=None)
"""
"""
Session opened
Creating full_name='Johnny English' age=58
Session closed
"""
"""
# Invoke restricted_create to create a new profile as an unprivileged user
ProfileResource.create(
        'restricted_create',
        {'full_name': 'Mr. Bean'},
        principals=[Principal('group', 'user')],
        session=None)
"""
"""
Session opened
Creating full_name='Mr. Bean' age=23
Session closed
"""

