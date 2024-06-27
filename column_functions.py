import pandas as pd

"""
    Sort rules:
    - Defining the column from which the dataframe will be sorted
    - Can be sorted ascending or descending (default is ascending)
"""

class SortRules():
    column_name: str
    ascending: bool
    def __init__(self, column_name: str, ascending: bool = True):
        self.column_name = column_name
        self.ascending = ascending

def sort(dataframe: pd.DataFrame, rules: SortRules) -> pd.DataFrame:
    df = dataframe.copy()
    return df.sort_values(rules.column_name, ascending=rules.ascending)

"""
    Reindex rules:
    - Defining the column which will be the new index
"""

class ReindexRules():
    column_name: str
    def __init__(self, column_name: str):
        self.column_name = column_name
        
def reindex(dataframe: pd.DataFrame, rules: ReindexRules) -> pd.DataFrame:
    df = dataframe.copy()
    return df.set_index(rules.column_name)


"""
    Remove Duplicates rules:
    - Defining the columns which will be considered to remove duplicates
"""

class RemoveDuplicatesRules():
    column_names: list[str]
    def __init__(self, column_names: list[str]):
        self.column_names = column_names

def remove_duplicates(dataframe: pd.DataFrame, rules: RemoveDuplicatesRules) -> pd.DataFrame:
    df = dataframe.copy()
    return df.drop_duplicates(subset=rules.column_names)

"""
    Remove Missing rules:
    - Defining if the data will be removed if one column has a NAN value or only if all of them have NAN values
    
    how: "any" | "all"
"""

class RemoveMissingRules():
    how: str
    def __init__(self, how: str = 'any'):
        self.how = how
        
def remove_missing(dataframe: pd.DataFrame, rules: RemoveMissingRules) -> pd.DataFrame:
    return dataframe.dropna(how=rules.how)

"""
    Parse Datetime rules:
    - Defining the columns which will be parsed
    - Defining the formatting which will be used to parse the columns
    
    At the moment only one rule of parsing is passed at a time, even with many columns
    The default parsing should probably be "%d/%m/%Y %H:%M:%S" (it is not being defined right now)
"""

class ParseDatetimeRules():
    formatting: str
    column_names: list[str]
    def __init__(self, formatting: str, column_names: str):
        self.formatting = formatting
        self.column_names = column_names

def parse_datetime(dataframe: pd.DataFrame, rules: ParseDatetimeRules) -> pd.DataFrame:
    df = dataframe.copy()
    for column in rules.column_names:
        df[column] = pd.to_datetime(df[column], format=rules.formatting)
    
    return df

"""
    Standardize rules:
    - Defining the columns which will be standardized
    
    At the moment only one rule of standardization is used, which is flooring the data
"""

class StandardizeRules():
    column_name: str
    def __init__(self, column_name: str):
        self.column_name = column_name
        
def standardize(dataframe: pd.DataFrame, rules: StandardizeRules) -> pd.DataFrame:
    df = dataframe.copy()
    df.loc[:, rules.column_name] = df[rules.column_name].dt.floor('Min')
    return df


"""
    Rename rules:
    - Renames specific columns in dataframe
    - New names must be passed in the same order as the ones that will be changed
"""
class RenameRules():
    column_names: list[str]
    new_names: list[str]
    def __init__(self, column_names: list[str], new_names: list[str]):
        self.column_names = column_names
        self.new_names = new_names
        
def rename(dataframe: pd.DataFrame, rules: RenameRules) -> pd.DataFrame:
    df = dataframe.copy()
    for index, column_name in rules.column_names:
        df.columns[column_name] = rules.new_names[index]
    
    return df


"""
    Clip Rules:
    - Clips values from a column that go above an upper value and/or below a lower value
    - If not passed, None will be considered for each limit
    - The same rules will apply for every column passed on each call.
    
"""
class ClipRules():
    column_names: list[str]
    lower_value: float | None
    upper_value: float | None
    def __init__(self, column_names: list[str], lower_value: float | None, upper_value: float | None):
        self.column_names = column_names
        self.lower_value = lower_value
        self.upper_value = upper_value
        
def clip(dataframe: pd.DataFrame, rules: ClipRules) -> pd.DataFrame:
    df = dataframe.copy()
    for column_name in rules.column_names:
        df[column_name] = df[column_name].clip(lower=rules.lower_value, upper=rules.upper_value)
    
    return df

# TODO: add remove conditional (maybe this will fit on cell functions as maybe it will use replace/remove functions) (not used on mvp)
# TODO: maybe add a function to change type of columns (not used on mvp)