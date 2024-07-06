from pydantic import BaseModel

from .column_operations import *
from .dataset_operations import *
from .row_operations import *


OPERATIONS: dict[str, BaseModel] = {
    **COLUMN_OPERATIONS,
    **DATASET_OPERATIONS,
}