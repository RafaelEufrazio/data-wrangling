import sys, inspect
import pandas as pd
from typing import Optional

from app.operations import Operation


class SortColumn(Operation):
    """
    Sort rules:
    - Defining the column from which the dataframe will be sorted
    - Can be sorted ascending or descending (default is ascending)
    """
    __code__ = 'SORT_COLUMN'
    
    column_name: str
    ascending: bool = True

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(self.column_name, ascending=self.ascending)


class ReindexColumn(Operation):
    """
    Reindex rules:
    - Defining the column which will be the new index
    """
    __code__ = 'REINDEX_COLUMN'

    column_name: str
        
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.set_index(self.column_name)


class RemoveDuplicateValuesColumn(Operation):
    """
    Remove Duplicates rules:
    - Defining the columns which will be considered to remove duplicates
    """
    __code__ = 'REMOVE_DUPLICATES_VALUES_COLUMN'

    column_names: list[str]

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=self.column_names)


class RemoveMissingValuesColumn(Operation):
    """
    Remove Missing rules:
    - Defining if the data will be removed if one column has a NAN value or only if all of them have NAN values
    
    how: "any" | "all"
    """
    __code__ = 'REMOVE_MISSING_VALUES_COLUMN'

    how: str
        
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how=self.how)


class ParseDatetimeColumn(Operation):
    """
    Parse Datetime rules:
    - Defining the columns which will be parsed
    - Defining the formatting which will be used to parse the columns
    
    At the moment only one rule of parsing is passed at a time, even with many columns
    The default parsing should probably be "%d/%m/%Y %H:%M:%S" (it is not being defined right now)
    """
    __code__ = 'PARSE_DATETIME_COLUMN'

    formatting: str = "%d/%m/%Y %H:%M:%S"
    column_names: list[str]

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in self.column_names:
            df[column] = pd.to_datetime(df[column], format=self.formatting)
        
        return df


class StandardizeColumn(Operation):
    """
    Standardize rules:
    - Defining the columns which will be standardized
    
    At the moment only one rule of standardization is used, which is flooring the data
    """
    __code__ = 'STANDARDIZE_COLUMN'

    column_name: str
        
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, self.column_name] = df[self.column_name].dt.floor('Min')
        return df


class RenameColumn(Operation):
    """
    Rename rules:
    - Renames specific columns in dataframe
    - New names must be passed in the same order as the ones that will be changed
    """
    __code__ = 'RENAME_COLUMN'
    
    column_names: list[str]
    new_names: list[str]
        
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={name: new_name for name, new_name in zip(self.column_names, self.new_names)})


class ClipValuesColumn(Operation):
    """
    Clip Rules:
    - Clips values from a column that go above an upper value and/or below a lower value
    - If not passed, None will be considered for each limit
    - The same rules will apply for every column passed on each call.
    """
    __code__ = 'CLIP_VALUES_COLUMN'

    column_names: list[str]
    lower_value: Optional[float]
    upper_value: Optional[float]
        
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        for column_name in self.column_names:
            df[column_name] = df[column_name].clip(lower=self.lower_value, upper=self.upper_value)
        
        return df


class ResampleValuesColumn(Operation):
    """
    Resample Rules:
    - Resamples timeseries data based on a specific column
    - Only accepts one column so far (need to add being able to use index column?)
    - Expects a frequency to resample as a string (could accept an EnumStr type or timedelta object?)
    
    """
    __code__ = 'RESAMPLE_VALUES_COLUMN'

    column_name: str
    frequency: str
    
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.resample(rule=self.frequency, on=self.column_name)


class MeanValuesColumn(Operation):
    __code__ = 'MEAN_VALUES_COLUMN'

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.mean()

# TODO: add remove conditional (maybe this will fit on cell functions as maybe it will use replace/remove functions) (not used on mvp)
# TODO: maybe add a function to change type of columns (not used on mvp)







# --------------------------------------------------------------------------
# Always leave this at the bottom so it gets all the classes
COLUMN_OPERATIONS = {
    cls.__code__: cls
    for _, cls in inspect.getmembers(sys.modules[__name__], predicate=inspect.isclass)
    if len(cls.__code__)
}