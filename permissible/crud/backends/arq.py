from arq import create_pool
from arq.connections import RedisSettings, ArqRedis, SSLContext
from arq.jobs import Job
from permissible.core import BaseSession
from typing import Any, Optional, Union, Dict, Callable, Generator, List
from datetime import datetime, timedelta
from pydantic import BaseModel
from permissible.crud.core import CRUDBackend, CRUDAccessType, CRUDBackendAccessRecord
from contextlib import contextmanager, asynccontextmanager
import uuid
from time import time
from dataclasses import dataclass, asdict
from arq.jobs import deserialize_job, JobDef, JobResult
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
    function_args: Dict[str, Any] = {}
    job_id: Optional[str] = None
    queue_name: Optional[str] = None
    defer_until: Optional[datetime] = None
    defer_by: Optional[Union[int, float, timedelta]] = None
    expires: Optional[Union[int, float, timedelta]] = None
    job_try: Optional[int] = None


class UpdateSchema(BaseModel):
    job_id: str
    function: Optional[str] = None
    queue_name: Optional[str] = None
    defer_until: Optional[datetime] = None
    defer_by: Optional[Union[int, float, timedelta]] = None
    expires: Optional[Union[int, float, timedelta]] = None
    job_try: Optional[int] = None

class GetModel(BaseModel):
    job_id: str
    function: str
    status: Optional[str] = None
    job_try: Optional[int] = None
    enqueue_time: datetime = None
    score: Optional[int] = None
    success: Optional[bool] = None
    result: Optional[Any] = None
    start_time: Optional[datetime] = None
    finish_time: Optional[datetime] = None
    queue_name: Optional[str] = None

class GetSchema(BaseModel):
    job_id: str

class PoolJobNotFound(ValueError):
    pass

class PoolJobCompleted(ValueError):
    pass

class PoolJobAlreadyExists(ValueError):
    pass

class ArqSessionAbortFailure(ValueError):
    pass

def as_int(f: float) -> int:
    return int(round(f))

def timestamp_ms() -> int:
    return as_int(time() * 1000)

async def abort_in_pool(pool, job_id) -> bool:
    results = await pool.all_job_results()
    results_found = {i.job_id: i for i in results}
    await pool.zadd('arq:abort', timestamp_ms(), job_id)
    status = None
    if await pool.exists('arq:result:' + job_id):
        status = 'complete'
    elif await pool.exists('arq:in-progress:' + job_id):
        status = 'in_progress'
    abort_complete = not(status == 'in_progress' or status == 'complete')
    return abort_complete

async def get_info_from_pool(pool, job_id, queue_name) -> Optional[JobDef]:
    results = await pool.all_job_results()
    results_found = {i.job_id: i for i in results}
    if job_id in results_found:
        info = results_found[job_id]
    else:
        info = None
    if not info:
        v = await pool.get('arq:job:' + job_id, encoding=None)

        if v:
            info = deserialize_job(v)
    if info:
        info.score = await pool.zscore(queue_name, job_id)
    return info

async def get_status_from_pool(pool, job_id, queue_name) -> str:
    """
    Status of the job.
    """
    if await pool.exists('arq:result:' + job_id):
        return 'complete'
    elif await pool.exists('arq:in-progress:' + job_id):
        return 'in_progress'
    else:
        score = await pool.zscore(queue_name, job_id)
        if not score:
            return 'not_found'
        return 'deferred' if score > timestamp_ms() else 'queued'

@dataclass
class Add:
    create_model: CreateSchema

@dataclass
class Delete:
    job_id: str

@dataclass
class JobPromise:
    function:str
    job_id: Optional[str] = None
    queue_name: Optional[str] = None
    defer_until: Optional[datetime] = None
    defer_by: Optional[Union[int, float, timedelta]] = None
    expires: Optional[Union[int, float, timedelta]] = None
    job_try: Optional[int] = None



class JobPromiseModel(BaseModel):
    function: str
    job_id: str
    status: str

class JobDefModel(JobPromiseModel):
    job_try: Optional[int]
    enqueue_time: datetime
    score: Optional[int]

class JobResultModel(JobDefModel):
    success: bool
    result: Any
    start_time: datetime
    finish_time: datetime
    queue_name: str

JobModel = Union[JobPromiseModel, JobDefModel, JobResultModel]

class ARQSession(BaseSession):
    pool: ArqRedis
    operations: Dict[str, Union[Add, Delete]]
    def __init__(self, pool):
        self.pool = pool
        self.operations = {}
    
    async def add(self, job_create: CreateSchema):
        if job_create.job_id in self.operations:
            self.operations[job_create.job_id].append(Add(create_model = job_create))
        else:
            self.operations[job_create.job_id] = [Add(create_model = job_create)]
        return JobPromiseModel(function = job_create.function, job_id = job_create.job_id, status='promise')
    
    async def delete(self, job_id):
        if job_id in self.operations:
            self.operations[job_id].append(Delete(job_id = job_id))
        else:
            self.operations[job_id] = [Delete(job_id = job_id)]
    
    async def query(self, job_id, queue_name = None):
        if queue_name is None:
            queue_name = self.pool.default_queue_name

        info = None
        status = None
        if job_id in self.operations:
            last_operation = self.operations[job_id][-1]
            if isinstance(last_operation, Add):
                last_dict = last_operation.create_model.dict()
                info = JobPromise(last_dict['function'], job_id)
                status = 'promise'
            else:
                info = ...
                status = None

        if info is None:
            info = await get_info_from_pool(self.pool, job_id, queue_name)
            status = await get_status_from_pool(self.pool, job_id, queue_name)
        if info == ...:
            info = None
        
        if isinstance(info, JobPromise):
            return JobPromiseModel(**asdict(info), status=status)
        elif isinstance(info, JobResult):
            info_dict = asdict(info)
            info_dict['status'] = status
            info_dict['job_id'] = job_id
            return JobResultModel(**info_dict)
        elif isinstance(info, JobDef):
            info_dict = asdict(info)
            info_dict['status'] = status
            info_dict['job_id'] = job_id
            return JobDefModel(**info_dict)

        elif info is None:
            return None

    async def commit(self):
        for job_id, operations in self.operations.items():
            for operation in operations:
                if isinstance(operation, Add):
                    input_data = operation.create_model.dict()
                    job_details = {
                        'function': input_data['function'],
                        '_job_id': input_data['job_id'],
                        '_queue_name': input_data['queue_name'],
                        '_defer_until': input_data['defer_until'],
                        '_defer_by': input_data['defer_by'],
                        '_expires': input_data['expires'],
                        '_job_try': input_data['job_try'],
                        **input_data['function_args']
                    }
                    await self.pool.enqueue_job(**job_details)
                elif isinstance(operation, Delete):
                    abort_completed = await abort_in_pool(self.pool, operation.job_id)
                    if not abort_completed:
                        raise ArqSessionAbortFailure(f'Job_id: {operation.job_id}')
                else:
                    raise ValueError(f'Unknown operation {operation}')
        self.operations = {}
    
    async def close(self):
        self.operations = {}
    
    async def remove_operations(self, job_id):
        self.operations.pop(job_id)


class ARQSessionMaker:
    def __init__(self, pool_future):
        self.pool_future = pool_future 
    async def get_session(self):
        await self.pool_future
        return ARQSession(self.pool_future.result())


class ARQBackend(CRUDBackend[ArqRedis]):
    session_maker: ARQSessionMaker
    def __init__(
        self,
        session_maker: ARQSessionMaker
    ):
        self.session_maker = session_maker
        async def create(session: ARQSession, data: CreateSchema) -> JobPromiseModel:
            job_data = data.dict(by_alias=True)
            if job_data['job_id'] is None:
                job_data['job_id'] = str(uuid.uuid4())

            job_id = job_data['job_id']
            return_model = CreateSchema.parse_obj(job_data)
            result = await session.query(return_model.job_id)
            if result is not None:
                raise PoolJobAlreadyExists()
            await session.add(return_model)
            return await session.query(job_id)

        async def read(session: ARQSession, data: GetSchema) -> JobPromiseModel:
            result = await session.query(data.job_id)
            if result is None:
                raise PoolJobNotFound()
            return result
                
        async def delete(session: ARQSession, data: GetSchema):
            result = await session.query(data.job_id)
            if result is None:
                raise PoolJobNotFound()
            elif hasattr(result,'status') and (result.status == 'complete' or result.status == 'in_progress'):
                raise PoolJobCompleted()
            await session.delete(data.job_id)
            return result
        
        self.create = create
        self.read = read
        self.delete = delete

        super().__init__(
            CRUDBackendAccessRecord[CreateSchema, GetModel, ARQSession](
                CreateSchema,
                GetModel,
                create,
                CRUDAccessType.create
            ),
            CRUDBackendAccessRecord[GetSchema, GetModel, ARQSession](
                GetSchema,
                GetModel,
                read,
                CRUDAccessType.read
            ),
            CRUDBackendAccessRecord[GetSchema, GetModel, ARQSession](
                GetSchema,
                GetModel,
                delete,
                CRUDAccessType.delete
            )
        )


    @asynccontextmanager
    async def generate_session(self) -> Generator[ArqRedis, None, None]:
        """
        Generate a new session in case the user didn't specify one yet
        """
        session = await self.session_maker.get_session()
        try:
            yield session
        finally:
            await session.close()
