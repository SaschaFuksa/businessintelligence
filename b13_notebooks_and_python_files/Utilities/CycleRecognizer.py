import pandas
import numpy
import stumpy
from datetime import datetime

class CycleRecognizer:
    
    ref_path = 'WI3_BusinessIntelligence_Data_Bi3/data_outside_20211018_000100.pkl.csv'
    production_path = 'WI3_BusinessIntelligence_Result_Bi3/production.csv'
    production_dataframe = None
    dataframe_ref = None
    cycle_number = 1
    
    def __init__(self):
        self.__prepare_ref()
        self.production_dataframe = pandas.read_csv(self.production_path, usecols=['timer', 'logCycleCounter'])
        self.production_dataframe = self.production_dataframe.rename(columns={'logCycleCounter': 'cycle_number'})
        self.production_dataframe['cycle_number'] = self.production_dataframe['cycle_number'].astype('int')
        self.production_dataframe['timer'] = self.production_dataframe['timer'].astype('datetime64[ns]')
    
    # Prepare reference cycle in frame and optionaly save reference cycle to file
    def __prepare_ref(self):
        self.dataframe_ref = pandas.read_csv(self.ref_path, usecols=['magnetometer_z_median', 'Unnamed: 0']) 
        self.dataframe_ref = self.dataframe_ref.rename(columns={'Unnamed: 0':'timer'})
        self.dataframe_ref = self.dataframe_ref.loc[(self.dataframe_ref.index >= 106) & (self.dataframe_ref.index <= 478)]
        save_ref_frame = False
        if save_ref_frame:
            reference_cycle = pandas.read_csv(self.ref_path)
            reference_cycle = reference_cycle.rename(columns={'Unnamed: 0':'timer'})
            reference_cycle = reference_cycle.loc[(reference_cycle.index >= 106) & (reference_cycle.index <= 478)]
            reference_cycle.to_csv('WI3_BusinessIntelligence_Result_Bi3/reference_cycle.csv')
    
    # recognize cycles of frame file and ref cycle
    def recognize_cycle(self, file):
        dataframe = self.__prepare_file(file)
        match_df = self.__prepare_matches(dataframe)
        dataframe = self.__add_cycles_to_frame(dataframe, match_df)
        dataframe = self.__match_to_production(match_df, dataframe)
        return dataframe
    
    # Prepare file and timer col
    def __prepare_file(self, file):
        dataframe = pandas.read_csv(file)
        dataframe = dataframe.rename(columns={'Unnamed: 0':'timer'})
        dataframe['timer'] = dataframe['timer'].astype('datetime64[ns]')  
        return dataframe
    
    # Recognize cycles via stumpy match and prepare result by sorting and handling long cycles
    def __prepare_matches(self, dataframe):
        matches = stumpy.match(self.dataframe_ref['magnetometer_z_median'], dataframe['magnetometer_z_median'])
        matches = sorted(matches, key=lambda x: x[1])
        match_df = self.__create_match_df(dataframe, matches)
        match_df = self.__handle_long_cycles(dataframe, match_df)
        return match_df
    
    # Create df and calculate distance column
    def __create_match_df(self, dataframe, matches):
        match_df = pandas.DataFrame(matches, columns=['similarity', 'index'])
        match_df['distance'] = match_df['index'].shift(-1).sub(match_df['index'])
        last_index = int(match_df['index'].iloc[-1])
        minindex = dataframe.magnetometer_z_median[last_index+200:last_index+400].idxmin()
        match_df['distance'].iloc[-1] = minindex-last_index
        match_df['distance'] = match_df['distance'].astype('int')
        return match_df
    
    # Handling long cycles by reduce them by calculation of "next" min because: Cycles are between 17-19 seconds long
    def __handle_long_cycles(self, dataframe, match_df):
        unexpected_distances = match_df[match_df['distance'] > 400]
        for row in unexpected_distances.itertuples():
            start_index = row.index
            end_index = dataframe.magnetometer_z_median[start_index+200:start_index+400].idxmin()
            match_df['distance'].iloc[row.Index] = end_index-start_index
        return match_df
    
    # Add calculated stumpy cycles to sensor frame
    def __add_cycles_to_frame(self, dataframe, match_df):
        dataframe['sensor_cycle'] = numpy.nan
        dataframe['sensor_cycle_distance'] = numpy.nan
        for row in match_df.itertuples():
            start_index = row.index
            end_index = start_index + row.distance
            dataframe.loc[start_index:end_index, 'sensor_cycle'] = self.cycle_number
            dataframe.loc[start_index:end_index, 'sensor_cycle_similarity'] = row.similarity
            self.cycle_number += 1
        return dataframe
    
    # Do matching by group cycles in both (prod + sensor) frames by minutes
    def __match_to_production(self, match_df, dataframe):
        production_dataframe_day_snippet = self.__get_production_snippet(dataframe)
        dataframe = self.__add_rough_timer_cols(dataframe)
        
        grouped_df = dataframe.groupby('timer_minute')
        grouped_lists = grouped_df['sensor_cycle'].apply(set)
        grouped_lists = grouped_lists.reset_index()
        grouped_lists = grouped_lists.set_index('timer_minute')
        
        grouped_df_prod = self.production_dataframe.groupby("timer")
        grouped_lists_prod = grouped_df_prod["cycle_number"].apply(set)
        grouped_lists_prod = grouped_lists_prod.reset_index()
        grouped_lists_prod = grouped_lists_prod.set_index('timer')
        
        merged_cycles = pandas.concat([grouped_lists, grouped_lists_prod], axis=1)
        
        mapping_cycles = pandas.DataFrame(columns=['sensor_cycle', 'cycle_number'])
        for row in merged_cycles.itertuples():
            cycles = row.sensor_cycle
            cycle_numbers = row.cycle_number
            cycle_index = 0
            if pandas.notnull(cycles) and pandas.notnull(cycle_numbers):
                cycles = sorted(cycles)
                cycle_numbers = sorted(cycle_numbers)
                for cycle_number in cycle_numbers:
                    if len(cycles) > cycle_index:
                        new_row = {'sensor_cycle': cycles[cycle_index], 'cycle_number': cycle_number}
                        mapping_cycles = mapping_cycles.append(new_row, ignore_index=True)
                        cycle_index += 1
        
        dataframe['cycle_number'] = numpy.nan
        for row in mapping_cycles.itertuples():
            dataframe.cycle_number.loc[dataframe['sensor_cycle'] == row.sensor_cycle] = row.cycle_number
        dataframe['cycle_number'] = dataframe['cycle_number'].astype(numpy.float).astype('Int32')
        dataframe['sensor_cycle'] = dataframe['sensor_cycle'].astype(numpy.float).astype('Int32')
        return dataframe
    
    # Get snippet of current day of production log
    def __get_production_snippet(self, dataframe):
        start_date = dataframe.timer.iloc[0]
        start_date = datetime.strftime(start_date, '%Y-%m-%d')
        end_date = pandas.to_datetime(start_date) + pandas.DateOffset(days=1)
        end_date = datetime.strftime(end_date, '%Y-%m-%d')
        production_dataframe_day_snippet = self.production_dataframe.loc[(self.production_dataframe.timer > start_date) & (self.production_dataframe.timer < end_date)]
        return production_dataframe_day_snippet
    
    # Add rough timer by minute and second
    def __add_rough_timer_cols(self, dataframe):
        dataframe['timer_second'] = dataframe.timer.dt.round('s')
        dataframe['timer_minute'] = dataframe.timer.dt.round('min')
        return dataframe
    
    # Append already calculated cycles to other frame like: Calculated outside frame cycles to inside frame cycle because both are synchronized
    def append_cycle_to_file(self, cycle_frame, file):
        dataframe = pandas.read_csv(file)
        dataframe = dataframe.rename(columns={'Unnamed: 0':'timer'})
        dataframe['timer'] = dataframe['timer'].astype('datetime64[ns]')
        merged_df = pandas.concat([dataframe, cycle_frame], axis=1)
        return merged_df