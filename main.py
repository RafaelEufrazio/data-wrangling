from typing import Union
from fastapi import FastAPI, UploadFile
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[str, None] = None

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_id": item_id, "item_name": item.name, "item_price": item.price, "item_is_offer": item.is_offer}

@app.post("/file")
async def upload_file(file: UploadFile):
    return {"filename": file.filename, "size": file.size}