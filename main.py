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
    ip = IrradianceProcessor(dir=io.StringIO(contents.decode('utf-8')), decimal=',', sep=';')
    run(ip)
    return {"filename": file.filename, "size": file.size}