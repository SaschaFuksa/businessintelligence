import pandas
import stumpy
import numpy


class AnomalyDetector:
    
    
    ref_path = 'D:\OneDrive\Dokumente\Jupyter\WI3_BusinessIntelligence_Sycn_Result_WP2_Bi3/20211022-outside.csv'
    ref_start = 309735
    ref_length = 380
    ref_dataframe = None
    m = 6
    cycle_number = 1
    
    def __init__(self, detect_ref_cycle):
        self.ref_dataframe = pandas.read_csv(self.ref_path)
        if detect_ref_cycle:
            self.ref_dataframe = self.__detect_reference_cycle()
        else:
            self.ref_dataframe = self.ref_dataframe[self.ref_start: self.ref_start + self.ref_length]
        
    
    def detect_distances(self, outside_file):
        outside_dataframe = pandas.read_csv(outside_file)
        outside_dataframe = self.__find_cycles(outside_dataframe)
        outside_dataframe = self.__detect_distance_profile(outside_dataframe)
        return outside_dataframe
    
    
    def __detect_reference_cycle(self):
        ref_dataframe = self.ref_dataframe.loc[(self.ref_dataframe['timer'] > '2021-10-22 10:00:00') & (self.ref_dataframe['timer'] < '2021-10-22 16:00:00')]
        mp = stumpy.stump(ref_dataframe['magnetometer_median'], self.m)
        motif_idx = numpy.argsort(mp[:, 0])[0]
        ref_dataframe = ref_dataframe[motif_idx:motif_idx + self.ref_length]
        return ref_dataframe
    
    def __find_cycles(self, outside_dataframe): 
        matches = stumpy.match(self.ref_dataframe['magnetometer_median'], outside_dataframe['magnetometer_median'])
        matches = sorted(matches, key=lambda x: x[1])
        match_df = pandas.DataFrame(matches, columns=['distance', 'index'])
        match_df['range'] = match_df['index'].shift(-1).sub(match_df['index'])
        match_df['potential_anomaly'] = False
        match_df['range'] = match_df['range'].astype(numpy.float).astype('Int32')
        match_df.loc[(match_df.range > 400),'potential_anomaly']= True
        match_df.loc[match_df['range'] > 400, 'range'] = 380
        match_df.iloc[match_df.index[-1], match_df.columns.get_loc('range')] = 380
        
        outside_dataframe['stumpy_cycle'] = numpy.nan
        outside_dataframe['potential_anomaly'] = numpy.nan
        outside_dataframe['match_distance'] = numpy.nan


        for row in match_df.itertuples():
            start_index = row.index
            end_index = start_index + row.range
            outside_dataframe.loc[start_index:end_index, 'potential_anomaly'] = row.potential_anomaly
            outside_dataframe.loc[start_index:end_index, 'match_distance'] = row.distance
            outside_dataframe.loc[start_index:end_index, 'stumpy_cycle'] = self.cycle_number
            self.cycle_number += 1
        return outside_dataframe
    
    def __detect_distance_profile(self, outside_dataframe):
        distance_profile = stumpy.mass(self.ref_dataframe['magnetometer_median'], outside_dataframe['magnetometer_median'])
        mass_df = pandas.DataFrame(distance_profile, columns=['distance_mass'])
        mass_df.replace([numpy.inf, -numpy.inf], 40.0, inplace=True)
        outside_dataframe['distance_mass'] = numpy.nan
        outside_dataframe.distance_mass = mass_df.distance_mass
        outside_dataframe = outside_dataframe.drop(columns = ['sensor_cycle_similarity', 'sensor_cycle', 'cycle_number'])
        return outside_dataframe
    
    
    
    
    