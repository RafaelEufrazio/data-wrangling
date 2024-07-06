
import sys, inspect
import pandas as pd
from pydantic import model_validator

from app.models import DataGroup, DataTable, Operation


class SplitDataTable(Operation):
    """
    Separates existing dataframe into many dataframes in a map
    - The key for each rule is the set of columns that will define the new dataframe
    - The rules define the title of the columns within the new dataframes and the name of the dataframes themselves
    - Both information are necessary to be passed as arguments
    - The column names must be passed in the same order as the columns in the key
    
    This may be used to separate a dataframe into as many new dataframes as wanted
    It does not matter if columns are repeated or not, or if all columns are being used.
    """
    __code__ = 'SPLIT_DATA_TABLE'

    dfs_names: list[str]
    columns_indexes: list[list[int]]
    columns_names: list[list[str]]


    @model_validator(mode='after')
    def enforce_equal(cls, values):
        if len(values['dfs_names']) == len(values['columns_indexes']) == len(values['columns_names']):
            return values
        
        raise ValueError('dfs_names, columns_indexes and columns_names must have the same length')  


    def __call__(self, df: pd.DataFrame) -> DataGroup:
        df_group = DataGroup()
        for name, indexes, columns in zip(self.dfs_names, self.columns_indexes, self.columns_names):
            original_column_names: list[str] = df.columns[indexes]
            # Getting columns of data based on original names and in the correct order
            new_df = df[original_column_names]
            # Setting new column names in the same order
            new_df.columns = columns
            # Saving dataframe on map
            df_group.dfs.append(DataTable(alias=name, df=new_df))
        
        return df_group



class JoinDataTable(Operation):
    """
    Joins dataframes in a map into a single one, horizontally
    - The key for each rule is the name of each dataframe
    - The columns extracted from the dataframes will be added in the same sequence as they are passed
    - At the moment there is no way to specify the final sequence of columns specifically apart from each dataframe
    
    This may be used to join as many columns from as many dataframes as wanted
    It does not matter if dataframes are duplicated, or columns are passed more than once.
    """
    __code__ = 'JOIN_DATA_TABLE'

    columns_names: list[list[str]]


    def __call__(self, df_group: DataGroup) -> pd.DataFrame:
        if len(self.columns_names) != len(df_group.dfs):
            raise ValueError('columns_names must have the same size as the Data Group')  

        new_df = pd.concat([df.df[columns] for df, columns in zip(df_group.dfs, self.columns_names)], axis = 1)
        return new_df

# TODO: create a function to iterate through dataframes and execute the dataframe operation on each of them



# --------------------------------------------------------------------------
# Always leave this at the bottom so it gets all the classes
DATASET_OPERATIONS: dict[str, Operation] = {
    cls.__code__: cls
    for _, cls in inspect.getmembers(sys.modules[__name__], predicate=inspect.isclass)
    if hasattr(cls, '__code__') and len(cls.__code__)
}