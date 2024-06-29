import pandas as pd
from enum import Enum
from typing import Type
import io

class FileType(Enum):
    CSV = 1

class IoArgs():
    file_type: Type[FileType]
    decimal: str
    sep: str
    
    def __init__(self, file_type: Type[FileType], decimal: str, sep: str):
        self.file_type = file_type
        self.decimal = decimal
        self.sep = sep

def open(source: io.StringIO, rules: IoArgs) -> pd.DataFrame:
    match rules.file_type:
        case FileType.CSV:
            return pd.read_csv(dir=source, decimal=rules.decimal, sep=rules.sep)
        
        
        
def convert(dataframe: pd.DataFrame, rules: IoArgs) -> any: # Figure out how to type this properly
    match rules.file_type:
        case FileType.CSV:
            return convert_to_csv(dataframe=dataframe, rules=rules)
            
            
def convert_to_csv(dataframe: pd.DataFrame, rules: IoArgs) -> io.StringIO:
    csv = io.StringIO()
    dataframe.to_csv(csv)
    return csv