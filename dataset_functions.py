
import pandas as pd

"""
    Split rules:
    - Setting the name of the new columns on each new dataframe*
    - Setting the name of each new dataframe*
"""
class SplitRules():
    df_name: str = ""
    column_names: list[str] = []
    
    def __init__(self, df_name: str, column_names: list[str]):
        self.df_name = df_name
        self.column_names = column_names
        
"""
    Separates existing dataframe into many dataframes in a map
    - The key for each rule is the set of columns that will define the new dataframe
    - The rules define the title of the columns within the new dataframes and the name of the dataframes themselves
    - Both information are necessary to be passed as arguments
    - The column names must be passed in the same order as the columns in the key
    
    This may be used to separate a dataframe into as many new dataframes as wanted
    It does not matter if columns are repeated or not, or if all columns are being used.
"""
def split_df(dataframe: pd.DataFrame, rules: dict[list[int], SplitRules]) -> dict[str, pd.DataFrame]:
    df_map: dict[str, pd.DataFrame] = {}
    
    for columns, rule in rules.items():
        original_column_names: list[str] = []
        new_column_names: list[str] = []
        
        # Extracting names from original columns and new columns
        for index, column_index in columns:
            # Gets column name from column index in the original dataframe
            # This is not to be confused with the index of the value on the list that was passed as key
            original_column_names.append(dataframe.columns[column_index])
            # Sets name of column in the same index as the column that was passed to keep the order
            new_column_names.append(rule.column_names[index])
        
        # Getting columns of data based on original names and in the correct order
        new_df = dataframe[original_column_names]
        # Setting new column names in the same order
        new_df.columns = new_column_names
        # Saving dataframe on map
        df_map[rule.df_name] = new_df
    
    return df_map

class JoinRules():
    column_names: list[str] = []
    
    def __init__(self, column_names: list[str]):
        self.column_names = column_names

"""
    Joins dataframes in a map into a single one, horizontally
    - The key for each rule is the name of each dataframe
    - The columns extracted from the dataframes will be added in the same sequence as they are passed
    - At the moment there is no way to specify the final sequence of columns specifically apart from each dataframe
    
    This may be used to join as many columns from as many dataframes as wanted
    It does not matter if dataframes are duplicated, or columns are passed more than once.
"""
def join_dfs(df_map: dict[str, pd.DataFrame], rules: dict[str, JoinRules]) -> pd.DataFrame:
    new_df: pd.DataFrame
    for df_name, rule in rules.items():
        if new_df is None:
            new_df = df_map[df_name][rule.column_names]
        else:
            new_df = pd.concat([new_df, df_map[df_name][rule.column_names]], axis = 1)
            
    return new_df