import pandas as pd
from pydantic import BaseModel

class DataTable(BaseModel):
    alias: str
    df: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True