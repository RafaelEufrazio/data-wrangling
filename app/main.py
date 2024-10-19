import io
import pandas as pd
from typing import List, Dict, Any, Union
from fastapi import FastAPI, UploadFile, HTTPException, File
from pydantic import BaseModel
from io import BytesIO

from app.operations import OPERATIONS, Operation

app = FastAPI()

class OperationData(BaseModel):
    code: str
    attributes: Dict[str, Any]


class FileData(BaseModel):
    alias: str
    data: list[dict]

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.data)

    def set_file_data(self, file: pd.DataFrame):
        self.data = file.to_dict('records')
        return


class RequestBody(BaseModel):
    file: FileData
    operations: list[OperationData]


# Health check endpoint
@app.get('/')
async def hello():
    return "Hello!"


@app.post("/file")
async def data_wrangle(body: RequestBody) -> FileData:
    validate_operations(body.operations)
    validate_file(body.file)
    processed_file = run_operations(body.operations, body.file)
    return processed_file


def run_operations(operations: List[OperationData], file: FileData) -> FileData:
    file_df = file.to_dataframe()
    for operation in operations:
        op: Operation = OPERATIONS[operation.code](**operation.attributes)
        file_df = op(file_df)

    file.set_file_data(file_df)
    return file
    

def validate_operations(operations: List[OperationData]):
    for op in operations:
        if op.code not in OPERATIONS:
            raise HTTPException(status_code=501, detail=f"operation {op.code} is not a valid operation")
        
        try:
            OPERATIONS[op.code](**op.attributes)
        except:
            raise HTTPException(status_code=422, detail=f"attributes {op.attributes} for operation {op.code} are not valid")


def validate_file(file: FileData):
    try:
        file.to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"unprocessable file")