from arq import create_pool
from permissible.crud.backends.arq import ARQBackend, CreateSchema, ARQSessionMaker, GetSchema, ArqSessionAbortFailure
import asyncio
from arq.connections import RedisSettings
import time
from random import random
from math import log10

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

    data = CreateSchema(function='test_func', defer_by=1)
    create_data = await backend.create(session = session, data = data)
    job_id = create_data.job_id
    get_data = GetSchema(job_id = job_id)
    #delete_data = await backend.delete(session=session, data = get_data)
    #await session.commit()
    n=1
    read_data = await backend.read(session = session, data = get_data)
    print(read_data)
    await session.commit()
    delete_data = await backend.delete(session=session, data = get_data)
    while n<20:
        read_data = await backend.read(session = sessionmaker(), data = get_data)
        print(read_data)
        time.sleep(0.5)
        print()
        n = n+1
    try:
        await session.commit()
    except ArqSessionAbortFailure as e:
        job_id = str(e).split(': ')[1]
        session.remove_operations(job_id)
    await session.commit()
    read_data = await backend.read(session = session, data = get_data)

    print(read_data)


class WorkerSettings:
    functions = [test_func]
    allow_abort_jobs = True

if __name__ == '__main__':
    asyncio.run(main())