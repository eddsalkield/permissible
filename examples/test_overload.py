from fastapi import FastAPI, Depends
from typing import overload, Annotated, Union
from pydantic import BaseModel, Field

app = FastAPI()

class M(BaseModel):
    i: int
    s: str

class N(BaseModel):
    i: int
    s2: str

class Model(BaseModel):
    __root__: Annotated[Union[M, N], Field(discriminator="i")]

@app.post("/files/")
def create_file(m = Depends(Model)):
    return {"file_size": len(file)}

