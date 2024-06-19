#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 14:44:26 2023

@author: suellen.costa
"""

from enum import StrEnum
from datetime import timedelta
import pandas as pd
from typing import Type, Callable
import io

# Ignoring deprecation warnings, which can confuse the code messages being generated
# Obs.: Comment this code in case any errors arise
import warnings
warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


"""

<---------------- CONSTANTS ---------------->

"""



# File location
CSV = 'src/examples/04-04-24-10-04-24Irradiancia.csv'
NEW_CSV = 'src/examples/new_csv.csv'
MEAN_CSV = 'src/examples/mean_csv.csv'

# Lists new sensor names for organized dataframes
class SENSOR_NAMES(StrEnum):
    GHI ='GHI'
 #   GHI2 ='GHI2'
    PIR1 = 'PIR1'
    PIR2 = 'PIR2'
    PIR5 = 'PIR5'
    PIR7 = 'PIR7'
 #   GHI2 = 'GHI2'
#    Temp1 = 'Temp1'
    Temp2 = 'Temp2'
    Temp3 = 'Temp3'
#    RH1 = 'RH1'
    RH2 = 'RH2'

# Maps each intended sensor indentifier with the original name in the data source
SENSORS = {
    'Datalogger[7].Meteo[1].MRI_IrradianceGlobal': SENSOR_NAMES.GHI,
#    'Datalogger[1].Meteo[1].MRI_IrradianceGlobal': SENSOR_NAMES.GHI2,
    'Datalogger[1].SensorAI[2].MRI_Value01': SENSOR_NAMES.PIR1,
    'Datalogger[2].SensorAI[2].MRI_Value01': SENSOR_NAMES.PIR2,
    'Datalogger[5].SensorAI[2].MRI_Value01': SENSOR_NAMES.PIR5,
    'Datalogger[7].SensorAI[2].MRI_Value01': SENSOR_NAMES.PIR7,
#    'Datalogger[7].Meteo[1].MRI_IrradianceGlobal': SENSOR_NAMES.GHI2,
#    'Datalogger[1].Meteo[1].MRI_TemperatureAmbient': SENSOR_NAMES.Temp1,
    'Datalogger[1].Meteo[1].MRI_TemperatureAmbient': SENSOR_NAMES.Temp2,
 #   'Datalogger[1].Meteo[1].MRI_Humidity': SENSOR_NAMES.RH1,
    'Datalogger[7].Meteo[1].MRI_Humidity': SENSOR_NAMES.RH2,
    'Datalogger[7].Meteo[1].MRI_TemperatureAmbient': SENSOR_NAMES.Temp3,
    }

# Maps correlation between similar sensors so that data can be shared between them in case of corruption
SENSOR_EQUIVALENTS = {
    SENSOR_NAMES.GHI: SENSOR_NAMES.GHI,
#    SENSOR_NAMES.GHI2: SENSOR_NAMES.GHI2,
    SENSOR_NAMES.PIR1: SENSOR_NAMES.PIR2,
    SENSOR_NAMES.PIR2: SENSOR_NAMES.PIR1,
    SENSOR_NAMES.PIR5: SENSOR_NAMES.PIR7,
    SENSOR_NAMES.PIR7: SENSOR_NAMES.PIR5,
#    SENSOR_NAMES.Temp1: SENSOR_NAMES.Temp2,
    SENSOR_NAMES.Temp2: SENSOR_NAMES.Temp2,
 #   SENSOR_NAMES.RH1: SENSOR_NAMES.RH2,
    SENSOR_NAMES.RH2: SENSOR_NAMES.RH2,
    SENSOR_NAMES.Temp3: SENSOR_NAMES.Temp3,
    }

# Lists new column names for organized dataframes
class NEW_COLUMN_NAMES(StrEnum):
    TAGNAME = 'TagName'
    VALUE = 'Value'
    TIMESTAMP = 'Timestamp'
    
    

"""

<---------------- PROCESSOR CLASS ---------------->

"""



"""

Class used to process irradiance dataframes

This class is separated by Main, Auxiliar and General methods:
    Main Methods: To be used with a dataframe map as an input (executing operations on all sensor data within the map)
    Auxiliar Methods: To be used with only one dataframe as input (executing operations only wihtin the given dataframe)
    General Methods: General usage methods to be used within the class
    
Auxiliar methods are to be used in bulk within the from irradiance_constants import CSV, NEW_CSV, MEAN_CSV
from irradiance_processor import IrradianceProcessormain methods, but if the user would like to use them as standalone features
for further expansion of the code, different implementation or special use cases, they can and should be used as so.

"""
class IrradianceProcessor(object):
    
    class Executable(object):
        function: Callable
        args: dict[str, any]

        def __init__(self, function: Callable, **kwargs):
            self.function = function
            self.args = kwargs
            return None
    
    executables: list[Executable] # List of functions that will be executed
    original_dataframe: pd.DataFrame
    processed_dataframe: pd.DataFrame
    dataframe_dictionary: dict[Type[NEW_COLUMN_NAMES], pd.DataFrame]
    
    """ Initalizes list of existing Operations """
    def __init__(self, dir: str | io.StringIO, decimal: str, sep: str):
        self.executables = []
        self.open_csv(dir=dir, decimal=decimal, sep=sep)
        return None
        
    """
    Main Methods
    """
    
    """ Imports CSV file using pandas """
    def open_csv(self, dir: str, decimal: str, sep: str):
        self.original_dataframe = pd.read_csv(filepath_or_buffer=dir, decimal=decimal, sep=sep)
    
    """ Adds executable object on list of functions to be later called """
    def add_func(self, function: Callable, **kwargs):
            self.executables.append(self.Executable(function=function, **kwargs))
        
    """ Executes functions and concatenates dataframe map into a single dataframe """
    def execute_functions(self) -> pd.DataFrame:
        for executable in self.executables:
            if len(executable.args) > 0: executable.function(**executable.args)
            else: executable.function()
        self.concat_df_map()
        return self.processed_dataframe
        
    """ Separates original dataframe into a map of sensor data """
    def separate_dataframes(self):

        print('Separating dataframes ...')
        # Transforms dataframe into list of dataframes (each dataframe has data from one sensor)
        df_list = self._df_to_dflist(self.original_dataframe)
        # Transforms list of dataframes into map of dataframes (removing the need for a sensor "name" column)
        self.dataframe_dictionary = self._dflist_to_dfmap(df_list)
        # Renames keys in the map for convenient sensor names (defined in the SENSORS and SENSOR_NAMES variables)
        self.dataframe_dictionary = self._rename_dfmap_keys(self.dataframe_dictionary)
    
    
    """
    Removes NAN values and reindex all dataframes by standardized frequency timestamps 
    Obs.: NAN values due to reindexing will still exist at the end here    
    """
    def clean_and_reindex_all(self):
        
        print('Cleaning and reindexing dataframes ...')
        for key in self.dataframe_dictionary: self.dataframe_dictionary[key] = self._clean_and_reindex(self.dataframe_dictionary[key])
    
    """ Fills NAN values for all dataframes, with corresponding values from equivalent sensors or near values """
    def fill_missing_all(self):
        
        print('Filling missing or corrupt data ...')
        
        # Gets min and max boundaries for each sensor data timestamps
        # This is done using GHI timestamps as baseline
        boundaries = { 'start': self.dataframe_dictionary[SENSOR_NAMES.GHI].index.min(), 'end': self.dataframe_dictionary[SENSOR_NAMES.GHI].index.max() }
        new_timestamps = pd.date_range(boundaries['start'], boundaries['end'], freq=" 1min")
        
        # Fill indexes with new complete timestamps
        for key in self.dataframe_dictionary:
            self.dataframe_dictionary[key] = self._complete_timestamps(self.dataframe_dictionary[key], new_timestamps)
        
        # Fills missing data in each dataframe
        for key in self.dataframe_dictionary:
            self.dataframe_dictionary[key] = self._fill_missing(key, self.dataframe_dictionary[key], self.dataframe_dictionary[SENSOR_EQUIVALENTS[key]])
    
    """ 
    Removes rows with indexes before initial time and after ending time from all dataframes 
    IMPORTANT: TYPES ARE NOT WELL MADE HERE AND ON PRIVATE FUNCTION YET AND THIS MAY NOT WORK
    """
    def remove_all_meaningless_timestamps(self, initial_time, ending_time):
        
        print('Removing useless timestamps ...')
        for key in self.dataframe_dictionary: self.dataframe_dictionary[key] = self._remove_meaningless_timestamps(self.dataframe_dictionary[key], initial_time, ending_time)
    
    class ClipRules():
        lower_value: float
        upper_value: float
        
        def __init__(self, lower_value: float, upper_value: float):
            self.lower_value = lower_value
            self.upper_value = upper_value
        
    """ Clips all dataframes between lower and upper value to guarantee values stay within limits """
    def clip_range_all(self, specific_rules: dict[Type[SENSOR_NAMES], ClipRules], rules: ClipRules):
        
        print('Clipping out of boundaries values ...')
        for key in self.dataframe_dictionary:
            if len(specific_rules) > 0 and key in specific_rules:
                self.dataframe_dictionary[key] = self._clip_range(self.dataframe_dictionary[key], lower_value=specific_rules[key].lower_value, upper_value=specific_rules[key].upper_value)
            else:
                self.dataframe_dictionary[key] = self._clip_range(self.dataframe_dictionary[key], lower_value=rules.lower_value, upper_value=rules.upper_value)
    
    """ Finds and substitutes all stagnant values across all dataframes for a given frequency window """
    def fix_all_stagnant_data(self, frequency: int, threshold: float):
        
        print('Replacing stagnant data ...')
        for key in self.dataframe_dictionary: self.dataframe_dictionary[key] = self._fix_stagnant_data(key, self.dataframe_dictionary[key], self.dataframe_dictionary[SENSOR_EQUIVALENTS[key]], frequency, threshold)
    
    
    """ Finds and substitutes values where abrupt changes where detected over a threshold on all dataframes """
    def fix_all_abrupt_changes(self, specific_rules: dict[SENSOR_NAMES, int], threshold: int):
        
        print('Fixing abrupt changes ...')
        for key in self.dataframe_dictionary: 
            if len(specific_rules) > 0 and key in specific_rules:
                self.dataframe_dictionary[key] = self._fix_abrupt_changes(key, dataframe=self.dataframe_dictionary[key], equivalent_dataframe=self.dataframe_dictionary[SENSOR_EQUIVALENTS[key]], threshold=specific_rules[key])
            else:
                self.dataframe_dictionary[key] = self._fix_abrupt_changes(key, self.dataframe_dictionary[key], self.dataframe_dictionary[SENSOR_EQUIVALENTS[key]], threshold)
    
    """ Finds and substitutes all incorrect zeroes from all dataframes comparing with equivalent dataframes """
    def remove_all_incorrect_zeroes(self):
        
        print('Removing incorrect zeroes ...')
        for key in self.dataframe_dictionary: self.dataframe_dictionary[key] = self._remove_incorrect_zeroes(key, self.dataframe_dictionary[key], self.dataframe_dictionary[SENSOR_EQUIVALENTS[key]])
    
    """ Concatenates dataframe map into a single dataframe in which the column names are the sensor names """
    def concat_df_map(self):
        
        print('Concatenating dataframes ...')
        # Creates a list in which every index has a 'Value' column of the dataframes in the map
        values_list = [df[NEW_COLUMN_NAMES.VALUE] for key, df in self.dataframe_dictionary.items()]
        # Concatenates all dataframes along the column axis using the map keys as column names, creating a single dataframe
        self.processed_dataframe = pd.concat(values_list, axis = 1, keys = self.dataframe_dictionary.keys())
    
    
    """
    Auxiliar methods
    """
    
    """ Separates dataframe into list of dataframes """
    def _df_to_dflist(self, dataframe: pd.DataFrame) -> list[pd.DataFrame]:
        
        df_list: list[pd.DataFrame] = list()
        # Iterates for each group of 3 columns (which defines a sensor)
        for i in range(0, len(dataframe.columns), 3):
            # Each new dataframe represents a sensor and has 3 columns of data: sensor name, value and timestamp
            new_df = dataframe[[dataframe.columns[i], dataframe.columns[i+1], dataframe.columns[i+2]]]
            new_df.columns = [NEW_COLUMN_NAMES.TAGNAME, NEW_COLUMN_NAMES.VALUE, NEW_COLUMN_NAMES.TIMESTAMP]
            df_list.append(new_df)
        return df_list
    
    """
    Creates map with all dataframes organized by key
    Obs.: Also removes first column from each dataframe (as it is redundant)
    """
    def _dflist_to_dfmap(self, df_list: list[pd.DataFrame]) -> dict[Type[NEW_COLUMN_NAMES], pd.DataFrame]:
        
        df_map: dict[Type[NEW_COLUMN_NAMES], pd.DataFrame] = {}
        for i in range(0, len(df_list)):
            
            # Creates new dataframe for each element of the list
            aux_df = df_list[i]
            
            # Names key as sensor name value mapped in the NEW_COLUMN_NAMES variable
            df_map[aux_df[NEW_COLUMN_NAMES.TAGNAME].iloc[0]] = aux_df[[aux_df.columns[2], aux_df.columns[1]]]
            
        return df_map
    
    """ Renames sensor identifier names using constant names map """
    def _rename_dfmap_keys(self, df_map: dict[Type[NEW_COLUMN_NAMES], pd.DataFrame]) -> dict[Type[NEW_COLUMN_NAMES], pd.DataFrame]:
        
        new_map:dict[Type[NEW_COLUMN_NAMES], pd.DataFrame] = {}
        for key in df_map: new_map[SENSORS[key]] = df_map[key]
        return new_map
     
    """ Removes NAN values and reindex one dataframe by timestamp """
    def _clean_and_reindex(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        
        # Removes NAN values from dataframe
        dataframe = dataframe.dropna()
        # Parses timestamps, sorts timestamps, standardizes time frequency and indexes dataframe by timestamps
        return self._reindex_by_timestamp(dataframe)
    
    """ Parses timestamps, sorts timestamps, standardizes time frequency and indexes dataframe by timestamps """
    def _reindex_by_timestamp(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        
        # Creates copy of dataframe to perform changes
        df = dataframe.copy()
        # Parses timestamps
        # Direct setting is done here to avoid mistyping the new column, as may be the standard behavior of .loc with pd.to_datetime
        # Reference: https://stackoverflow.com/questions/76766136/pandas-pd-to-datetime-assigns-object-dtype-instead-of-datetime64ns
        df[NEW_COLUMN_NAMES.TIMESTAMP] = pd.to_datetime(df[NEW_COLUMN_NAMES.TIMESTAMP], format="%d/%m/%Y %H:%M:%S")
        # Sorts timestamps
        df = df.sort_values(NEW_COLUMN_NAMES.TIMESTAMP)
        # Standardizes frequency by flooring irregular measures
        df.loc[:, NEW_COLUMN_NAMES.TIMESTAMP] = df[NEW_COLUMN_NAMES.TIMESTAMP].dt.floor('Min')
        # Removes duplicate timestamps if they exist
        df = df.drop_duplicates(subset=[NEW_COLUMN_NAMES.TIMESTAMP])
        # Reindexes dataframe by timestamps
        df = df.set_index(NEW_COLUMN_NAMES.TIMESTAMP)
        
        return df
    
    """ Fill indexes with new complete timestamps """
    def _complete_timestamps(self, dataframe: pd.DataFrame, timestamps: pd.DatetimeIndex) -> pd.DataFrame:
        
        # Fill indexes with new complete timestamps
        dataframe = dataframe.reindex(timestamps)
        
        return dataframe
    
    """ Fills NAN values for one dataframe, with corresponding values from equivalent sensors or near values """
    def _fill_missing(self, sensor, dataframe: pd.DataFrame, equivalent_dataframe: pd.DataFrame) -> pd.DataFrame:
        
        # Get indexes of all rows with null values after reindexing
        na_indexes = dataframe[dataframe[NEW_COLUMN_NAMES.VALUE].isna()].index.tolist()
        
        # Substitutes values by using correlated values from equivalent sensor
        # It may be a good idea to change this condition to "all sensors without equivalency" in the future, instead of specific sensor name
        if (sensor != SENSOR_NAMES.GHI):
            # Substitutes null values for valid values (using equivalent sensor data)
            for index in na_indexes: dataframe = self._substitute(index, dataframe, equivalent_dataframe)
        else:
            # If sensor is GHI (or any within condition above), fill with zero
            dataframe = dataframe.fillna(0)
        
        return dataframe
    
    """ Removes rows from dataframe before initial time and after ending time """
    def _remove_meaningless_timestamps(self, dataframe: pd.DataFrame, initial_time, ending_time):
        return dataframe[(dataframe.index.hour > initial_time) & (dataframe.index.hour < ending_time)]
    
    """ Clips dataframe between lower and upper value to guarantee values stay within limits """
    def _clip_range(self, dataframe: pd.DataFrame, lower_value: float, upper_value: float) -> pd.DataFrame:
        return dataframe.clip(lower=lower_value, upper=upper_value)
    
    """ Finds and substitutes stagnant values for a given frequency window """
    def _fix_stagnant_data(self, sensor, dataframe: pd.DataFrame, equivalent_dataframe: pd.DataFrame, frequency: int, threshold: float) -> pd.DataFrame:
        
        window_count = 0
        window_start = None
        previous_index = None
        previous_value = dataframe.iloc[0][NEW_COLUMN_NAMES.VALUE]
        
        if (sensor != SENSOR_NAMES.GHI):
            for index in dataframe.index:
                if (abs(dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] - previous_value) <= threshold):
                    if (window_count == 0): window_start = index
                    window_count += 1
                else:
                    if (window_count >= frequency):
                        stagnant_indexes = pd.date_range(window_start, previous_index, freq="1min")
                        for i in stagnant_indexes:
                            #print(i.strftime("%d/%m/%Y, %H:%M:%S") + '  ===>  ' + str(dataframe.loc[i]['Value']))
                            dataframe = self._substitute(i, dataframe, equivalent_dataframe)
                    window_count = 0
                    window_start = None
                previous_index = index
                previous_value = dataframe.loc[previous_index][NEW_COLUMN_NAMES.VALUE]
        
        return dataframe
                
    """ Finds and substitutes values where abrupt changes where detected over a threshold on a dataframe """
    def _fix_abrupt_changes(self, sensor, dataframe: pd.DataFrame, equivalent_dataframe: pd.DataFrame, threshold: int) -> pd.DataFrame:
        
        previous_value = dataframe.iloc[0][NEW_COLUMN_NAMES.VALUE]
        previous_index = dataframe.index[0]
        
        if (sensor != SENSOR_NAMES.GHI):
            for index in dataframe.index:
                # Checks if variation between two rows is bigger than threshold
                if (abs(dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] - previous_value) >= threshold):
                    # Checks if variation is smaller than threshold on equivalent dataframe (otherwise it's not an abrupt change)
                    if ((index in equivalent_dataframe.index) and (previous_index in equivalent_dataframe.index)):
                        if (abs(equivalent_dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] - equivalent_dataframe.loc[previous_index][NEW_COLUMN_NAMES.VALUE]) < threshold):
                            # Checks if variation between actual index and next one is also abrupt (if it isn't, it probably isn't an abrupt variation)
                            if (index + timedelta(minutes=1) in dataframe.index):
                                if (abs(dataframe.loc[index + timedelta(minutes=1)][NEW_COLUMN_NAMES.VALUE] - dataframe.loc[index][NEW_COLUMN_NAMES.VALUE]) >= threshold):
                                    dataframe = self._substitute(index, dataframe, equivalent_dataframe)
                                    
                previous_index = index
                previous_value = dataframe.loc[index][NEW_COLUMN_NAMES.VALUE]
        
        return dataframe
    
    """ Finds and substitutes incorrect zeroes comparing with another sensor """
    def _remove_incorrect_zeroes(self, sensor, dataframe: pd.DataFrame, equivalent_dataframe: pd.DataFrame) -> pd.DataFrame:
        
        if (sensor != SENSOR_NAMES.GHI):
            for index in dataframe.index:
                # Checks if value is zero in this sensor but not on the other sensor
                if ((dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] == 0) and (equivalent_dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] != 0)):
                    dataframe = self._substitute(index, dataframe, equivalent_dataframe)
        
        return dataframe
    
    """
    General Methods
    """
    
    """ Substitutes one value by index from a dataframe for the same index from another dataframe """
    def _substitute(self, index: pd.Index, dataframe: pd.DataFrame, equivalent_dataframe: pd.DataFrame) -> pd.DataFrame:
        
        # Checks if index exists in equivalent sensor
        if (index in equivalent_dataframe.index):
            equivalent_value = equivalent_dataframe.loc[index][NEW_COLUMN_NAMES.VALUE]
            # Checks if value in equivalent sensor exists
            if (equivalent_value != None and equivalent_value != 0):
                # Substitutes value in dataframe for existing value in another dataframe
                dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] = equivalent_dataframe.loc[index][NEW_COLUMN_NAMES.VALUE]
        else:
            # If value doesn't exist in equivalent dataframe, substitutes for last registered value
            dataframe.loc[index][NEW_COLUMN_NAMES.VALUE] = dataframe.loc[index - pd.Timedelta(minutes = 1)][NEW_COLUMN_NAMES.VALUE]
        
        return dataframe
    
