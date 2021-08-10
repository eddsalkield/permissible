from arq import create_pool
from arq.connections import RedisSettings, ArqRedis, SSLContext
from arq.jobs import Job
from typing import Any, Optional, Union, Dict, Callable, Generator
from datetime import datetime, timedelta
from pydantic import BaseModel
from permissible.crud.core import CRUDBackend
from contextlib import contextmanager
"""
Analogies

create_pool == get session
pool.enqueue_job == session.add (create)
job.info()/job.status() == session.query (read)
job.abort() == session.delete (delete)

if job.status() == 'queued' or 'deferred':
    job.abort
    pool.enqueue_job(new_settings)


To abort a job, call arq.job.Job.abort(). (Note for the arq.job.Job.abort()
 method to have any effect, you need to set allow_abort_jobs to True on the worker,
  this is for performance reason. allow_abort_jobs=True may become the default in future)

arq.job.Job.abort() will abort a job if it’s already running or 
prevent it being run if it’s currently in the queue.

WorkerSettings defines functions available on startup




"""

class CreateSchema(BaseModel):
    function: str
    job_id: Optional[str] = None
    queue_name: Optional[str] = None
    defer_until: Optional[datetime] = None
    defer_by: Optional[Union[int, float, timedelta]] = None
    expires: Optional[Union[int, float, timedelta]] = None
    job_try: Optional[int] = None
    class Config:
        fields = {
            'job_id': '_job_id',
            'queue_name': '_queue_name',
            'defer_until': '_defer_until',
            'defer_by': '_defer_by',
            'expires': '_expires',
            'job_try': '_job_try'
        }

class ResultSchema:
    pass

class JobSchema(BaseModel):
    job_id: str
    status: str
    function: str
    job_try: Optional[int]
    enqueue_time: datetime
    score: Optional[int]
    #results_info: Optional[ResultSchema]#Optional[arq.jobs.JobResult]


async def job_to_schema(job):
    info = await job.info()
    status = await job.status()
    #result_info = await job_details.result_info()
    output_details = {}
    output_details['job_id'] = job.job_id
    output_details['status'] = status.value
    output_details['function'] = info.function
    output_details['job_try'] = info.job_try
    output_details['enqueue_time'] = info.enqueue_time
    output_details['score'] = info.score
    return output_details

class PoolJobNotFound(ValueError):
    pass



class ARQBackend(CRUDBackend[ArqRedis]):
    pool_settings: Dict[str, Any]
    jobs: Dict[str, Job]
    def __init__(
        self,
        redis_settings: Optional[RedisSettings] = None,
        retry: int = 0, 
        job_serializer: Optional[Callable[[Dict[str, Any]], bytes]] = None, 
        job_deserializer: Optional[Callable[[bytes], Dict[str, Any]]] = None, 
        default_queue_name: str = 'arq:queue'
    ):
        self.pool_settings = {
            'settings_': redis_settings,
            'retry': retry,
            'job_serializer': job_serializer,
            'job_deserializer': job_deserializer,
            'default_queue_name': default_queue_name
        }
        self.jobs = {}
        async def create(pool: ArqRedis, data: CreateSchema) -> JobSchema:
            job_details = await pool.enqueue_job(**data.dict(by_alias=True))
            self.jobs[job_details.job_id] = job_details
            output_details = await job_to_schema(job_details)
            return JobSchema.parse_obj(output_details)

        async def read(pool: ArqRedis, job_id: str) -> JobSchema:
            pool_jobs = await pool.zrange(pool.default_queue_name, withscores=True)
            active_pool_job_index = {job_id: job for job_id, job in pool_jobs}
            if job_id in active_pool_job_index and job_id not in self.jobs:
                raise ValueError(f'{job_id} was present in pool job index but not in internal pool!!')
            elif job_id not in self.jobs:
                raise PoolJobNotFound()
            job = self.jobs[job_id]
            output = await job_to_schema(job)
            return JobSchema.parse_obj(output)
        
        async def delete(pool: ArqRedis, job_id: str):
            pool_jobs = await pool.zrange(pool.default_queue_name, withscores=True)
            active_pool_job_index = {job_id: job for job_id, job in pool_jobs}
            if job_id in active_pool_job_index and job_id not in self.jobs:
                raise ValueError(f'{job_id} was present in pool job index but not in internal pool!!')
            elif job_id not in self.jobs:
                raise PoolJobNotFound()
            job = self.jobs[job_id]
            job.abort()
            return None
        
        self.create = create
        self.read = read
        self.delete = delete





    @contextmanager
    def generate_session(self) -> Generator[ArqRedis, None, None]:
        """
        Generate a new session in case the user didn't specify one yet
        """
        session = create_pool(**self.pool_settings)
        try:
            yield session
        finally:

            pass
            # Await all jobs to be completed and then terminate pool?
            #session.close()



