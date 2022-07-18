import pandas

class DataSyncer:
    
    fac = 20
    number = 0
    part_size = 0
    max_size = 0
    
    def sync(self, file_path, diff):
        dataframe = pandas.read_csv(file_path)
        self.__calc_parameters(dataframe, diff)
        sensor_frame = dataframe[['magnetometer_median', 'magnetometer_mean', 'accelerometer_max', 'accelerometer_min', 'accelerometer_max_quantile', 'accelerometer_min_quantile']]
        synced_frame = self.__create_synced_frame(sensor_frame)
        dataframe = self.__transfer_values(dataframe, synced_frame)
        return dataframe
    
    def __calc_parameters(self, dataframe, diff):
        self.number = int(diff * self.fac)
        self.part_size = int(dataframe.index[-1] / self.number)
        self.max_size = self.part_size * self.number
    
    def __create_synced_frame(self, sensor_frame):
        synced_frame = pandas.DataFrame(columns=['magnetometer_median', 'magnetometer_mean', 'accelerometer_max', 'accelerometer_min', 'accelerometer_max_quantile', 'accelerometer_min_quantile'])
        index = 0
        while index <= self.max_size:
            part_frame = sensor_frame[index:index+self.part_size]
            last_line = pandas.DataFrame(part_frame[-1:])
            new_frame = part_frame.append(last_line, ignore_index=True)
            synced_frame = synced_frame.append(new_frame, ignore_index=True)
            index += self.part_size
        synced_frame = synced_frame[:-1]
        return synced_frame
    
    def __transfer_values(self, dataframe, synced_frame):
        dataframe.magnetometer_median = synced_frame.magnetometer_median
        dataframe.magnetometer_mean = synced_frame.magnetometer_mean
        dataframe.accelerometer_max = synced_frame.accelerometer_max
        dataframe.accelerometer_min = synced_frame.accelerometer_min
        dataframe.accelerometer_max_quantile = synced_frame.accelerometer_max_quantile
        dataframe.accelerometer_min_quantile = synced_frame.accelerometer_min_quantile
        return dataframe