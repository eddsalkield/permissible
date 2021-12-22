from pydantic import BaseModel
from typing import Callable, Generator, Optional, Type
from contextlib import contextmanager
import asyncio
import uuid

from permissible import CRUDResource, LocalFileCRUDBackend, \
        Create, Read, Update, Delete, Action, Permission, Principal, \
        FileCreate, FileRead, FileUpdate, FileDelete, transaction_manager
# In this example, we define a Profile resource, which is accessible
# through admin and restricted accesses
# The admin accesses are accessible only to users in the admin group, and
# provide complete access to modify the model.
# The restricted accesses are accessible to standard users, but can only
# perform limited modifications.

FileBackend = LocalFileCRUDBackend("/tmp/permissible_test")

# Enforce that anyone can upload, only those in the owner's group can read, and only the owner can update
ImageResource = CRUDResource(
        FileCreate(
            name='public_create',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'user'))]),
        FileRead(
            name='public_read',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'user'))]),
        FileUpdate(
            name='public_update',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'user'))]),
        FileDelete(
            name='public_delete',
            permissions=[Permission(Action.ALLOW,
                                    Principal('group', 'user'))]),
        backend=FileBackend)

async def main():
    # Invoke admin_create to create a new profile as an administrative user
    image_id_1 = uuid.uuid4()
    image_id_2 = uuid.uuid4()
    print(f"image_id_1: {image_id_1}")
    print(f"image_id_2: {image_id_2}")

    with transaction_manager() as transaction:
        await ImageResource.create(
                'public_create',
                {"uuid": image_id_1, "file": open("/usr/share/backgrounds/sway/Sway_Wallpaper_Blue_1920x1080.png", "rb")},
                principals=[Principal('group', 'user')],
                transaction=transaction)

        await ImageResource.create(
                'public_create',
                {"uuid": image_id_2, "file": open("/usr/share/backgrounds/sway/Sway_Wallpaper_Blue_1920x1080.png", "rb")},
                principals=[Principal('group', 'user')],
                transaction=transaction)

        await ImageResource.update(
                'public_update',
                {"uuid": image_id_1, "file": open("/usr/share/backgrounds/eddos/wallpaper_1920x1080_x4_foundations.png", "rb")},
                principals=[Principal('group', 'user')],
                transaction=transaction)

        await ImageResource.delete(
                'public_delete',
                {"uuid": image_id_1},
                principals=[Principal('group', 'user')],
                transaction=transaction)

if __name__ == '__main__':
    asyncio.run(main())
