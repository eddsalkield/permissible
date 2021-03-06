from permissible.crud.backends.sqlalchemy import QuerySchema, AlreadyExistsError
from pydantic import BaseModel
from typing import Callable, Generator, Optional, Type
from contextlib import contextmanager

from permissible import CRUDResource, SQLAlchemyCRUDBackend, \
        Create, Read, Update, Delete, Action, Permission, Principal

from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, String, Text, Column, Integer
from pydantic_sqlalchemy import sqlalchemy_to_pydantic
import asyncio
# In this example, we define a Profile resource, which is accessible
# through admin and restricted accesses
# The admin accesses are accessible only to users in the admin group, and
# provide complete access to modify the model.
# The restricted accesses are accessible to standard users, but can only
# perform limited modifications.


engine = create_engine(
    "sqlite:///./test.db",
    connect_args={'check_same_thread': False}
)
Session = sessionmaker(bind=engine)

declarative_base_instance: DeclarativeMeta = declarative_base()
class BackModel(declarative_base_instance):
    __tablename__ = 'Test_table'
    full_name = Column(Text(), primary_key = True)
    age = Column(Integer())

declarative_base_instance.metadata.create_all(engine)

ProfileBackend = SQLAlchemyCRUDBackend(BackModel, Session)

CreateProfile = sqlalchemy_to_pydantic(BackModel, exclude = ['age'])
Profile = ProfileBackend.Schema
DeleteProfile = ProfileBackend.DeleteSchema
OutputQuerySchema = ProfileBackend.OutputQuerySchema
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
        Read[QuerySchema, OutputQuerySchema](
            name='admin_read',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'admin'))],
            input_schema=QuerySchema,
            output_schema=OutputQuerySchema
        ),
        Update[Profile, Profile](
            name='admin_update',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'admin'))],
            input_schema=Profile,
            output_schema=Profile
        ),
        Delete[DeleteProfile, Profile](
            name='admin_delete',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'admin'))],
            input_schema=DeleteProfile,
            output_schema=Profile
        ),
        backend=ProfileBackend
    )

# Invoke admin_create to create a new profile as an administrative user
async def main():
    try:
        await ProfileResource.create(
                'admin_create',
                {'full_name': 'Johnny English', 'age': 58},
                principals=[Principal('group', 'admin')],
                transaction=None)
    except AlreadyExistsError:
        pass

    # Invoke restricted_create to create a new profile as an unprivileged user
    try:
        await ProfileResource.create(
                'restricted_create',
                {'full_name': 'Mr. Bean'},
                principals=[Principal('group', 'user')],
                transaction=None)
    except AlreadyExistsError:
        pass

    test = await ProfileResource.read(
        'admin_read', 
        {'filter_spec': [{'field': 'full_name', 'op': '==', 'value': 'Mr. Bean'}]},
        principals=[Principal('group', 'admin')]
    )
    test = await ProfileResource.update(
        'admin_update', 
        {'full_name': 'Johnny English', 'age': 20},
        principals=[Principal('group', 'admin')]
    )
    test = await ProfileResource.delete(
        'admin_delete', 
        {'full_name': 'Johnny English'},
        principals=[Principal('group', 'admin')]
    )
    test = await ProfileResource.read(
        'admin_read', 
        {'filter_spec': [{'field': 'full_name', 'op': '==', 'value': 'Mr. Bean'}]},
        principals=[Principal('group', 'admin')]
    )
    print(test)

if __name__ == '__main__':
    asyncio.run(main())
