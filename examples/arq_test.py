from arq import create_pool
from permissible.crud.backends.arq import ARQBackend, CreateSchema
import asyncio
from arq.connections import RedisSettings
import time
async def test_func(ctx):
    
    #time.sleep(1)
    return 'yes'

async def main():
    pool = await create_pool()
    backend = ARQBackend()
    data = CreateSchema(function='test_func')
    create_data = await backend.create(pool = pool, data = data)
    job_id = create_data.job_id
    n=1
    while n<10:
        read_data = await backend.read(pool = pool, job_id = job_id)
        print(read_data)
        n = n+1
        time.sleep(0.2)
    delete_data = await backend.delete(pool=pool, job_id=job_id)
    read_data = await backend.read(pool = pool, job_id = job_id)
    print(read_data)



class WorkerSettings:
    functions = [test_func]
    allow_abort_jobs = True

if __name__ == '__main__':
    asyncio.run(main())