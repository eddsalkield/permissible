from arq import create_pool
from permissible.crud.backends.arq import ARQBackend, CreateSchema, ARQSessionMaker, GetSchema, ArqSessionAbortFailure, JobPromiseModel, JobModel, GetModel, PoolJobCompleted
import asyncio
from permissible import CRUDResource, Create, Read, Update, Delete, Permission, Action, Principal
from arq.connections import RedisSettings
import time
from random import random
from math import log10
from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime
async def test_func(ctx):
    random_3 = 2*random()
    for i in range(20000000):
        list_of_list = log10(abs(log10(random_3**random_3**random_3)))
    return list_of_list

async def main():
    pool = await create_pool()

    sessionmaker = ARQSessionMaker(pool=pool)
    backend = ARQBackend(sessionmaker)
    session = sessionmaker()
    ProfileResource = CRUDResource(
        # Admin interface to create profiles
        Create[CreateSchema, JobPromiseModel](
            name='admin_create',
            permissions=[Permission(Action.ALLOW, Principal('group', 'admin'))],
            input_schema=CreateSchema,
            output_schema=GetModel
        ),
        Read[GetSchema, GetModel](
            name='admin_read',
            permissions=[Permission(Action.ALLOW, Principal('group', 'admin'))],
            input_schema=GetSchema,
            output_schema=GetModel
        ),
        Delete[GetSchema, GetModel](
            name='admin_delete',
            permissions=[Permission(Action.ALLOW, Principal('group', 'admin'))],
            input_schema=GetSchema,
            output_schema=GetModel
        ),
        backend=backend
    )

    created = await ProfileResource.create(
        'admin_create',
        {'function': 'test_func', 'defer_by': 1},
        principals=[Principal('group', 'admin')],
        session=session
    )
    print(created)
    read = await ProfileResource.read(
        'admin_read',
        {'job_id': created.job_id},
        principals=[Principal('group', 'admin')],
        session=session
    )
    print(read)

    await session.commit()
    read = await ProfileResource.read(
        'admin_read',
        {'job_id': created.job_id},
        principals=[Principal('group', 'admin')],
        session=session
    )
    print(read)

    for i in range(10):
        time.sleep(0.5)
        read = await ProfileResource.read(
            'admin_read',
            {'job_id': created.job_id},
            principals=[Principal('group', 'admin')],
            session=session
        )
        print(read)
    
    await session.commit()
    try:
        deleted = await ProfileResource.delete(
            'admin_delete',
            {'job_id': created.job_id},
            principals=[Principal('group', 'admin')],
            session=session
        )
        
    except PoolJobCompleted:

        print('too late')

class WorkerSettings:
    functions = [test_func]
    allow_abort_jobs = True

if __name__ == '__main__':
    asyncio.run(main())
