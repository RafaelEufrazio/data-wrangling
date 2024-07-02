import io
import pandas as pd
from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import BaseModel

from app.operations import OPERATIONS

app = FastAPI()


class OperationData(BaseModel):
    code: str
    attributes: dict


@app.post("/file")
async def upload_file(data: list[OperationData]):
    validate_operations(data)
    #contents = await file.read()
    #df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
    #return {"filename": file.filename, "size": file.size}
    return


def validate_operations(operations: list[OperationData]):
    for op in operations:
        if op.code not in OPERATIONS:
            raise HTTPException(status_code=501, detail=f"operation {op.code} is not a valid operation")
        
        try:
            OPERATIONS[op.code](**op.attributes)
        except:
            raise HTTPException(status_code=422, detail=f"attributes {op.attributes} for operation {op.code} are not valid")