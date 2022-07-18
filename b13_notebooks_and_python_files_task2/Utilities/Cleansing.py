import pandas
import numpy

class Cleansing:
    
    ref_outside_path = 'WI3_BusinessIntelligence_Data_WP2_Bi3/20211022-outside.csv'
    ref_inside_path = 'WI3_BusinessIntelligence_Data_WP2_Bi3/20211022-inside.csv'
    inside_ref_values = None
    outside_ref_values = None
    in_mag_x_med = None
    in_mag_y_med = None
    in_mag_z_med = None
    in_accmin_x_med = None
    in_accmin_y_med = None
    in_accmin_z_med = None
    out_mag_x_med = None
    out_mag_y_med = None
    out_mag_z_med = None
    out_accmin_x_med = None
    out_accmin_y_med = None
    out_accmin_z_med = None
    
    def __init__(self):
        self.__prepare_inside_disturbance_values(self.ref_inside_path)
        self.__prepare_outside_disturbance_values(self.ref_outside_path)
        
    def clean_inside(self, file):
        dataframe = pandas.read_csv(file)
        dataframe['magnetometer_x_median'] = dataframe.magnetometer_x_median - self.in_mag_x_med
        dataframe['magnetometer_y_median'] = dataframe.magnetometer_y_median - self.in_mag_y_med
        dataframe['magnetometer_z_median'] = dataframe.magnetometer_z_median - self.in_mag_z_med
        dataframe['magnetometer_x_mean'] = dataframe.magnetometer_x_mean - self.in_mag_x_med
        dataframe['magnetometer_y_mean'] = dataframe.magnetometer_y_mean - self.in_mag_y_med
        dataframe['magnetometer_z_mean'] = dataframe.magnetometer_z_mean - self.in_mag_z_med
        dataframe['accelerometer_x_max'] = dataframe.accelerometer_x_max - self.in_accmin_x_med
        dataframe['accelerometer_y_max'] = dataframe.accelerometer_y_max - self.in_accmin_y_med
        dataframe['accelerometer_z_max'] = dataframe.accelerometer_z_max - self.in_accmin_z_med
        dataframe['accelerometer_x_min'] = dataframe.accelerometer_x_min - self.in_accmin_x_med
        dataframe['accelerometer_y_min'] = dataframe.accelerometer_y_min - self.in_accmin_y_med
        dataframe['accelerometer_z_min'] = dataframe.accelerometer_z_min - self.in_accmin_z_med
        dataframe['accelerometer_x_max_quantile'] = dataframe.accelerometer_x_max_quantile - self.in_accmin_x_med
        dataframe['accelerometer_y_max_quantile'] = dataframe.accelerometer_y_max_quantile - self.in_accmin_y_med
        dataframe['accelerometer_z_max_quantile'] = dataframe.accelerometer_z_max_quantile - self.in_accmin_z_med
        dataframe['accelerometer_x_min_quantile'] = dataframe.accelerometer_x_min_quantile - self.in_accmin_x_med
        dataframe['accelerometer_y_min_quantile'] = dataframe.accelerometer_y_min_quantile - self.in_accmin_y_med
        dataframe['accelerometer_z_min_quantile'] = dataframe.accelerometer_z_min_quantile - self.in_accmin_z_med
        dataframe = self.__calc_scalar_product(dataframe)
        dataframe = self.__set_float64_to_float32(dataframe)
        dataframe = dataframe[dataframe.audio_max.notnull()]
        return dataframe
    
    def clean_outside(self, file):
        dataframe = pandas.read_csv(file)
        dataframe['magnetometer_x_median'] = dataframe.magnetometer_x_median - self.out_mag_x_med
        dataframe['magnetometer_y_median'] = dataframe.magnetometer_y_median - self.out_mag_y_med
        dataframe['magnetometer_z_median'] = dataframe.magnetometer_z_median - self.out_mag_z_med
        dataframe['magnetometer_x_mean'] = dataframe.magnetometer_x_mean - self.out_mag_x_med
        dataframe['magnetometer_y_mean'] = dataframe.magnetometer_y_mean - self.out_mag_y_med
        dataframe['magnetometer_z_mean'] = dataframe.magnetometer_z_mean - self.out_mag_z_med
        dataframe['accelerometer_x_max'] = dataframe.accelerometer_x_max - self.out_accmin_x_med
        dataframe['accelerometer_y_max'] = dataframe.accelerometer_y_max - self.out_accmin_y_med
        dataframe['accelerometer_z_max'] = dataframe.accelerometer_z_max - self.out_accmin_z_med
        dataframe['accelerometer_x_min'] = dataframe.accelerometer_x_min - self.out_accmin_x_med
        dataframe['accelerometer_y_min'] = dataframe.accelerometer_y_min - self.out_accmin_y_med
        dataframe['accelerometer_z_min'] = dataframe.accelerometer_z_min - self.out_accmin_z_med
        dataframe['accelerometer_x_max_quantile'] = dataframe.accelerometer_x_max_quantile - self.out_accmin_x_med
        dataframe['accelerometer_y_max_quantile'] = dataframe.accelerometer_y_max_quantile - self.out_accmin_y_med
        dataframe['accelerometer_z_max_quantile'] = dataframe.accelerometer_z_max_quantile - self.out_accmin_z_med
        dataframe['accelerometer_x_min_quantile'] = dataframe.accelerometer_x_min_quantile - self.out_accmin_x_med
        dataframe['accelerometer_y_min_quantile'] = dataframe.accelerometer_y_min_quantile - self.out_accmin_y_med
        dataframe['accelerometer_z_min_quantile'] = dataframe.accelerometer_z_min_quantile - self.out_accmin_z_med
        dataframe = self.__calc_scalar_product(dataframe)
        dataframe = self.__set_float64_to_float32(dataframe)
        dataframe = dataframe[dataframe.audio_max.notnull()]
        return dataframe
    
    def __calc_scalar_product(self, dataframe):
        dataframe['magnetometer_median'] = (dataframe['magnetometer_x_median']**2 + dataframe['magnetometer_y_median']**2 + dataframe['magnetometer_z_median']**2)**0.5
        dataframe['magnetometer_mean'] = (dataframe['magnetometer_x_mean']**2 + dataframe['magnetometer_y_mean']**2 + dataframe['magnetometer_z_mean']**2)**0.5
        dataframe = dataframe.drop(columns=['magnetometer_x_median', 'magnetometer_y_median',\
                                            'magnetometer_z_median', 'magnetometer_x_mean',\
                                            'magnetometer_y_mean', 'magnetometer_z_mean'])
        dataframe['accelerometer_max'] = (dataframe['accelerometer_x_max']**2 + dataframe['accelerometer_y_max']**2 + dataframe['accelerometer_z_max']**2)**0.5
        dataframe['accelerometer_min'] = (dataframe['accelerometer_x_min']**2 + dataframe['accelerometer_y_min']**2 + dataframe['accelerometer_z_min']**2)**0.5
        dataframe = dataframe.drop(columns=['accelerometer_x_max', 'accelerometer_y_max',\
                                            'accelerometer_z_max', 'accelerometer_x_min',\
                                            'accelerometer_y_min', 'accelerometer_z_min'])
        dataframe['accelerometer_max_quantile'] = (dataframe['accelerometer_x_max_quantile']**2 + dataframe['accelerometer_y_max_quantile']**2 + dataframe['accelerometer_z_max_quantile']**2)**0.5
        dataframe['accelerometer_min_quantile'] = (dataframe['accelerometer_x_min_quantile']**2 + dataframe['accelerometer_y_min_quantile']**2 + dataframe['accelerometer_z_min_quantile']**2)**0.5
        dataframe = dataframe.drop(columns=['accelerometer_x_max_quantile', 'accelerometer_y_max_quantile',\
                                            'accelerometer_z_max_quantile', 'accelerometer_x_min_quantile',\
                                            'accelerometer_y_min_quantile', 'accelerometer_z_min_quantile'])

        return dataframe
    
    def __set_float64_to_float32(self, dataframe):
        dataframe['audio_max'] = dataframe['audio_max'].astype(numpy.float32)
        dataframe['audio_max_quantile'] = dataframe['audio_max_quantile'].astype(numpy.float32)
        dataframe['sensor_cycle_similarity'] = dataframe['sensor_cycle_similarity'].astype(numpy.float32)
        dataframe['sensor_cycle'] = dataframe['sensor_cycle'].astype(numpy.float).astype('Int32')
        dataframe['cycle_number'] = dataframe['cycle_number'].astype(numpy.float).astype('Int32')
        dataframe['shift'] = dataframe['shift'].astype(numpy.float).astype('Int32')
        dataframe['magnetometer_median'] = dataframe['magnetometer_median'].astype(numpy.float32)
        dataframe['magnetometer_mean'] = dataframe['magnetometer_mean'].astype(numpy.float32)
        dataframe['accelerometer_max'] = dataframe['accelerometer_max'].astype(numpy.float32)
        dataframe['accelerometer_min'] = dataframe['accelerometer_min'].astype(numpy.float32)
        dataframe['accelerometer_max_quantile'] = dataframe['accelerometer_max_quantile'].astype(numpy.float32)
        dataframe['accelerometer_min_quantile'] = dataframe['accelerometer_min_quantile'].astype(numpy.float32)
        return dataframe

    def __prepare_inside_disturbance_values(self, ref_path):
        dataframe = pandas.read_csv(ref_path)
        dataframe = dataframe.loc[(dataframe.timer_minute >= '2021-10-22 10:05:00.000') & (dataframe.timer_minute <= '2021-10-22 10:07:00.000')]
        self.in_mag_x_med = dataframe.magnetometer_x_median.median()
        self.in_mag_y_med = dataframe.magnetometer_y_median.median()
        self.in_mag_z_med = dataframe.magnetometer_z_median.median()
        self.in_accmin_x_med = dataframe.accelerometer_x_min.median()
        self.in_accmin_y_med = dataframe.accelerometer_y_min.median()
        self.in_accmin_z_med = dataframe.accelerometer_z_min.median()
    
    def __prepare_outside_disturbance_values(self, ref_path):
        dataframe = pandas.read_csv(ref_path)
        dataframe = dataframe.loc[(dataframe.timer_minute >= '2021-10-22 10:05:00.000') & (dataframe.timer_minute <= '2021-10-22 10:07:00.000')]
        self.dataframe = pandas.read_csv(ref_path)
        self.dataframe = dataframe.loc[(dataframe.timer_minute >= '2021-10-22 10:05:00.000') & (dataframe.timer_minute <= '2021-10-22 10:07:00.000')]
        self.out_mag_x_med = dataframe.magnetometer_x_median.median()
        self.out_mag_y_med = dataframe.magnetometer_y_median.median()
        self.out_mag_z_med = dataframe.magnetometer_z_median.median()
        self.out_accmin_x_med = dataframe.accelerometer_x_min.median()
        self.out_accmin_y_med = dataframe.accelerometer_y_min.median()
        self.out_accmin_z_med = dataframe.accelerometer_z_min.median()