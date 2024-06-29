import pandas as pd
from enum import Enum
from typing import Type
import io

class FileType(Enum):
    CSV = 1

class OpenArgs():
    file_type: Type[FileType]
    decimal: str
    sep: str

def open(source: io.StringIO, rules: OpenArgs) -> pd.DataFrame:
    match rules.file_type:
        case FileType.CSV:
            return pd.read_csv(dir=source, decimal=rules.decimal, sep=rules.sep)