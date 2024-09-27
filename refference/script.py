from irradiance_processor import IrradianceProcessor, CSV, NEW_CSV, MEAN_CSV, SENSOR_NAMES

def run(ip: IrradianceProcessor):
    # Creates instance of data processor
    # ip = IrradianceProcessor(dir=CSV, decimal=',', sep=';')

    # Separates original dataframe into a map of sensor data
    ip.add_func(ip.separate_dataframes)

    # Removes pre-existing NAN values and reindexes all dataframes by standardized frequency timestamps
    ip.add_func(ip.clean_and_reindex_all)

    # Fills NAN values for all dataframes, with corresponding values from equivalent sensors (if there is one) or near values
    ip.add_func(ip.fill_missing_all)

    # Function below was removed because all data needs to be at the final file. If needed on occasion, this can still be used.
    # Removes rows with indexes before initial time and after ending time from all dataframes
    # ip.add_func(ip.remove_all_meaningless_timestamps, initial_time=6, ending_time=17)

    # Clips all dataframes between lower and upper value to guarantee values stay within limits
    irradiance_clip_rules = ip.ClipRules(lower_value=0, upper_value=1500)
    temperature_clip_rules = ip.ClipRules(lower_value=0, upper_value=50)
    ip.add_func(ip.clip_range_all, specific_rules={SENSOR_NAMES.Temp2: temperature_clip_rules, SENSOR_NAMES.Temp3: temperature_clip_rules}, rules=irradiance_clip_rules)

    # Finds and substitutes all stagnant values across all dataframes for a given frequency window
    ip.add_func(ip.fix_all_stagnant_data, frequency=6, threshold=0.0001)

    # Finds and substitutes values where abrupt changes where detected over a threshold on all dataframes
    irradiance_abrupt_threshold = 800
    temperature_abrupt_threshold = 4
    ip.add_func(ip.fix_all_abrupt_changes, specific_rules={SENSOR_NAMES.Temp2: temperature_abrupt_threshold, SENSOR_NAMES.Temp3: temperature_abrupt_threshold}, threshold=800)

    # Finds and substitutes all incorrect zeroes from all dataframes comparing with equivalent dataframes
    ip.add_func(ip.remove_all_incorrect_zeroes)

    # Executes functions and concatenates dataframe map into a single dataframe in which the column names are the sensor names
    processed_dataframe = ip.execute_functions()

    # Processes the mean value for each day of data
    print('Processing mean for each day ...')
    mean_dataframe = processed_dataframe.resample('1D').mean()

    # Saves processed dataframe into csv file
    print('Saving result files ...')
    processed_dataframe.to_csv(NEW_CSV, decimal=',', sep=';')
    # Saves mean dataframe into csv file
    mean_dataframe.to_csv(MEAN_CSV, decimal=',', sep=';')

    print('---------------- All Done! ----------------')