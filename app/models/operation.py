from pydantic import BaseModel
import pandas as pd


class Operation(BaseModel):
    __code__: str = ''

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        pass