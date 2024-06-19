from typing import Union
from fastapi import FastAPI, UploadFile, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import pandas as pd
import io
from irradiance_processor import IrradianceProcessor
from script import run

app = FastAPI()

@app.post("/file")
async def upload_file(file: UploadFile):
    contents = await file.read()
    # dataframe = pd.read_csv(filepath_or_buffer=io.StringIO(contents.decode('utf-8')), decimal=',', sep=';')
    ip = IrradianceProcessor(dir=io.StringIO(contents.decode('utf-8')), decimal=',', sep=';')
    run(ip)
    return {"filename": file.filename, "size": file.size}

# class Item(BaseModel):
#     name: str
#     price: float
#     is_offer: Union[str, None] = None

# @app.get("/")
# def read_root():
#     return {"Hello": "World"}

# @app.get("/items/{item_id}")
# def read_item(item_id: int, q: Union[str, None] = None):
#     return {"item_id": item_id, "q": q}

# @app.put("/items/{item_id}")
# def update_item(item_id: int, item: Item):
#     return {"item_id": item_id, "item_name": item.name, "item_price": item.price, "item_is_offer": item.is_offer}