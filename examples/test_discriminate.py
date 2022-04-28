from fastapi import FastAPI, Depends
from typing import overload, Annotated, Union, Literal
from pydantic import BaseModel, Field, create_model

app = FastAPI()

class BlackCat(BaseModel):
    pet_type: Literal['cat']
    color: Literal['black']
    black_name: str


class WhiteCat(BaseModel):
    pet_type: Literal['cat']
    color: Literal['white']
    white_name: str


# Previous mistake:
#class Cat(BaseModel):
#    __root__: Annotated[
#                Union[BlackCat, WhiteCat],
#                Field(discriminator="color")]
            
Cat = Annotated[Union[BlackCat, WhiteCat], Field(discriminator='color')]

#outer_model = create_model(
#        "Model",
#        pet =(
#            Annotated[
#                Union[Cat],
#                Field(discriminator="pet_type")],
#            ...))

class Dog(BaseModel):
    pet_type: Literal['dog']
    name: str


Pet = Annotated[Union[Cat, Dog], Field(discriminator='pet_type')]


class Model(BaseModel):
    __root__: Pet



@app.post("/files/")
def create_file(m = Depends(Model)):
    return {"file_size": len(file)}

