import pandas as pd
from typing import Callable

"""
    This class is used to carry referential dataframe information to be used on another one
    
    df_name -> Is used as referential dataframe for operations. If None, main dataframe should be used
    
    both (default: False) -> Is used to guide event behavior:
        - If True, only triggers behaviors if conditions apply in both main and reference dataframes
        - If Flase, triggers behaviors even if conditions apply only in main dataframe
    
    callback -> Function that will be called somewhere on the code that implements this class when conditions apply
"""
class Reference():
    column_names: list[str]
    df_name: str | None
    both: bool
    callback: Callable
    
    def __init__(self, column_names: list[str], callback: Callable, df_name: str | None = None, both: bool = False):
        self.column_names = column_names
        self.df_name = df_name
        self.both = both
        self.callback = callback


class AbruptRules():
    column_names: list[str]
    threshold: int
    reference: Reference

def abrupt(dataframe: pd.DataFrame, rules: AbruptRules) -> pd.DataFrame:
    previous_index = dataframe.index[0]
    previous_value = dataframe.iloc[0][rules.column_names[0]]
    for row_index in dataframe.index:
        for column_name_index, column_name in rules.column_names:
            if (abs(dataframe.loc[row_index][column_name] - previous_value) >= rules.threshold):
                if rules.reference.df_name != None:
                    # Figure out wtf do I do here
                    return # Remove this
        previous_index = row_index
        previous_value = dataframe.loc[row_index][rules.column_names[0]]
        
        

# TODO: maybe add a function that runs an excel formula across all rows on specific columns