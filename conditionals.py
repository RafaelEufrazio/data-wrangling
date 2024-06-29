import pandas as pd
from enum import Enum
from typing import Type, Callable

class Events(Enum):
    ABRUPT = 1
    STAGNANT = 2

def handle_event(dataframe: pd.DataFrame, event: Type[Events], callback: Callable) -> pd.DataFrame:
    