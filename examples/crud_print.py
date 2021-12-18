from pydantic import BaseModel
from typing import Callable, Generator, Optional, Type
from contextlib import contextmanager

from permissible import CRUDResource, PrintCRUDBackend, \
        Create, Read, Update, Delete, Action, Permission, Principal
import asyncio
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
async def main():
    # Invoke admin_create to create a new profile as an administrative user
    await ProfileResource.create(
            'admin_create',
            {'full_name': 'Johnny English', 'age': 58},
            principals=[Principal('group', 'admin')],
            session=None)
    """
    Session opened
    Creating full_name='Johnny English' age=58
    Session closed
    """

    # Invoke restricted_create to create a new profile as an unprivileged user
    await ProfileResource.create(
            'restricted_create',
            {'full_name': 'Mr. Bean'},
            principals=[Principal('group', 'user')],
            session=None)
    """
    Session opened
    Creating full_name='Mr. Bean' age=23
    Session closed
    """

if __name__ == '__main__':
    asyncio.run(main())