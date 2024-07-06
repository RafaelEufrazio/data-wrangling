import io
import pandas as pd
from fastapi import FastAPI, UploadFile, HTTPException,  File
from pydantic import BaseModel
from io import BytesIO

from app.operations import OPERATIONS, Operation

app = FastAPI()

class OperationData(BaseModel):
    code: str
    attributes: dict

class DataWrangleBody(BaseModel):
    operations: list[OperationData]

@app.post("/file")
async def data_wrangle(data: DataWrangleBody):#, files: list[UploadFile] = File(...)):
    validate_operations(data.operations)
    #dfs = load_files(files)
    #dfs = run_operations(data.operations, dfs)
    return


def run_operations(operations: list[OperationData], dfs: list[dict[str, pd.DataFrame]]) -> list[dict[str, pd.DataFrame]]:
    for operation in operations:
        op: Operation = OPERATIONS[operation.code](**operation.attributes)
        res = op(dfs)


def load_files(files: list[UploadFile]) -> dict[str: pd.DataFrame]:
    data = {}
    for file in files:
        contents = file.file.read()
        buffer = BytesIO(contents)
        df = pd.read_csv(buffer)
        data[file.filename] = df
        buffer.close()
        file.file.close()

    return data

def validate_operations(operations: list[OperationData]):
    for op in operations:
        if op.code not in OPERATIONS:
            raise HTTPException(status_code=501, detail=f"operation {op.code} is not a valid operation")
        
        try:
            OPERATIONS[op.code](**op.attributes)
        except:
            raise HTTPException(status_code=422, detail=f"attributes {op.attributes} for operation {op.code} are not valid")