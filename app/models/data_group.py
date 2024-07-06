from pydantic import BaseModel

from app.models import DataTable

class DataGroup(BaseModel):
    dfs: list[DataTable] = []

