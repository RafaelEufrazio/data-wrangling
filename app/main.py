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

class RequestBody(BaseModel):
    files: Dict[str, UploadFile]
    operations: List[OperationData]

# Health check endpoint
@app.get('/')
async def hello():
    return "Hello!"


@app.post("/file")
async def data_wrangle(files: Dict[str, UploadFile] = File(...), operations: List[Operation] = File(...)):

    dataframes = {}
    for name, file in files.items():
        contents = await file.read()
        dataframes[name] = pd.read_csv(io.BytesIO(contents))
    
    # Process the operations here if needed
    
    # Example return, normally you would process and return results
    return {"message": "Files received and read into dataframes.", "dataframe_keys": list(dataframes.keys())}



def run_operations(operations: List[OperationData], dfs: List[Dict[str, pd.DataFrame]]) -> List[Dict[str, pd.DataFrame]]:
    for operation in operations:
        op: Operation = OPERATIONS[operation.code](**operation.attributes)
        res = op(dfs)


def load_files(files: List[UploadFile]) -> Dict[str, pd.DataFrame]:
    data = {}
    for file in files:
        contents = file.file.read()
        buffer = BytesIO(contents)
        df = pd.read_csv(buffer)
        data[file.filename] = df
        buffer.close()
        file.file.close()

    return data


def validate_operations(operations: List[OperationData]):
    for op in operations:
        if op.code not in OPERATIONS:
            raise HTTPException(status_code=501, detail=f"operation {op.code} is not a valid operation")
        
        try:
            OPERATIONS[op.code](**op.attributes)
        except:
            raise HTTPException(status_code=422, detail=f"attributes {op.attributes} for operation {op.code} are not valid")