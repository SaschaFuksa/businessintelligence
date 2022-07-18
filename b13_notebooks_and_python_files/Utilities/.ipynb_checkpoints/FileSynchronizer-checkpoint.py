import pickle
import pandas
import textwrap

class FileSynchronizer:
    
    name_seperator = '_'
    accelerometer = 'accelerometer'
    accelerometer_x = 'accelerometer_x'
    accelerometer_y = 'accelerometer_z'
    accelerometer_z = 'accelerometer_y'
    magnetometer = 'magnetometer'
    magnetometer_x = 'magnetometer_x'
    magnetometer_y = 'magnetometer_z'
    magnetometer_z = 'magnetometer_y'
    timer = 'timer'
    audio = 'audio'
    frequence = '50ms'
    _max = '_max'
    _min = '_min'
    _median = '_median'
    _mean = '_mean'
    _max_quantile = '_max_quantile'
    _min_quantile = '_min_quantile'
    
    def __init__(self, input_path, out_path, sensor_file):
        self.input_path = input_path
        self.out_path = out_path
        self.sensor_file = sensor_file

    # Integration of operations (IOSP)
    def synchronize_sensor_file(self):
        data = pandas.read_pickle(self.input_path+self.sensor_file)
        sensor_frame = self.__create_sensor_frame(data)
        audio_frame = self.__create_audio_frame(data)
        date = self.__determine_date()
        synchronized_sensors = self.__synchronize_sensors(sensor_frame, date)
        synchronized_audio = self.__synchronize_audio(audio_frame, date)
        result = synchronized_sensors.merge(synchronized_audio, left_index=True, right_index=True)
        result.to_csv(self.out_path+self.sensor_file+'.csv')
    
    # Create frame for sensors
    def __create_sensor_frame(self, data):
        sensor_frame = pandas.DataFrame(dict([ (k,pandas.Series(v)) for k,v in data.items() ]))
        sensor_frame = sensor_frame.drop('audio', axis=1)
        return sensor_frame
    
    # Create frame for audio
    def __create_audio_frame(self, data):
        audio_frame = pandas.DataFrame(data, columns=[self.audio])
        return audio_frame
    
    # Determine date of frames depending on start date and start delay of file
    def __determine_date(self):
        file_name = self.sensor_file.split(self.name_seperator)
        delay_parts = textwrap.wrap(file_name[3], 2)
        date = file_name[2] + ' ' + delay_parts[0] + ':' + delay_parts[1] + ':' + delay_parts[2]
        return date
    
    # Integration of synchronize sensor operations
    def __synchronize_sensors(self, sensor_frame, date):
        sensor_frame = self.__convert_timer_to_ms(sensor_frame, date)
        synchronized_accelerometer = self.__synchronize_accelerometer(sensor_frame)
        synchronized_magnetometer = self.__synchronize_magnetometer(sensor_frame)
        synchronized_sensors = pandas.concat([synchronized_accelerometer, synchronized_magnetometer], axis=1)
        return synchronized_sensors
    
    # Convert timer data to milliseconds and set timer as index
    def __convert_timer_to_ms(self, sensor_frame, date):
        sensor_frame[self.timer] = pandas.to_datetime(sensor_frame[self.timer], unit='ms', origin=pandas.Timestamp(date))
        sensor_frame = sensor_frame.set_index(self.timer)
        return sensor_frame

    # Synchronize accelerometer
    def __synchronize_accelerometer(self, sensor_frame):
        accelerometer_frame = self.__drop_columns(sensor_frame, self.magnetometer)
        accelerometer_frame_max = accelerometer_frame.resample(self.frequence).max()
        accelerometer_frame_max = accelerometer_frame_max.rename(columns={self.accelerometer_x: self.accelerometer_x+self._max,\
                                                                          self.accelerometer_y: self.accelerometer_y+self._max,\
                                                                          self.accelerometer_z: self.accelerometer_z+self._max})
        accelerometer_frame_min = accelerometer_frame.resample(self.frequence).min()
        accelerometer_frame_min = accelerometer_frame_min.rename(columns={self.accelerometer_x: self.accelerometer_x+self._min,\
                                                                          self.accelerometer_y: self.accelerometer_y+self._min,\
                                                                          self.accelerometer_z: self.accelerometer_z+self._min})
        accelerometer_frame_max_quantile = accelerometer_frame.resample(self.frequence).quantile(.95)
        accelerometer_frame_max_quantile = accelerometer_frame_max_quantile.rename(columns={self.accelerometer_x: self.accelerometer_x+self._max_quantile,\
                                                                                            self.accelerometer_y: self.accelerometer_y+self._max_quantile,\
                                                                                            self.accelerometer_z: self.accelerometer_z+self._max_quantile})                                        
        accelerometer_frame_min_quantile = accelerometer_frame.resample(self.frequence).quantile(.05)
        accelerometer_frame_min_quantile = accelerometer_frame_min_quantile.rename(columns={self.accelerometer_x: self.accelerometer_x+self._min_quantile,\
                                                                                            self.accelerometer_y: self.accelerometer_y+self._min_quantile,\
                                                                                            self.accelerometer_z: self.accelerometer_z+self._min_quantile})                                                 
        accelerometer_frame = pandas.concat([accelerometer_frame_max, accelerometer_frame_min, accelerometer_frame_max_quantile, accelerometer_frame_min_quantile], axis=1)
        return accelerometer_frame
    
    # Synchronize magnetometer
    def __synchronize_magnetometer(self, sensor_frame):
        magnetometer_frame = self.__drop_columns(sensor_frame, self.accelerometer)
        magnetometer_frame_mean = magnetometer_frame.resample(self.frequence).mean()
        magnetometer_frame_mean = magnetometer_frame_mean.rename(columns={self.magnetometer_x: self.magnetometer_x+self._mean,\
                                                                          self.magnetometer_y: self.magnetometer_y+self._mean,\
                                                                          self.magnetometer_z: self.magnetometer_z+self._mean})
        magnetometer_frame_median = magnetometer_frame.resample(self.frequence).median()
        magnetometer_frame_median = magnetometer_frame_median.rename(columns={self.magnetometer_x: self.magnetometer_x+self._median,\
                                                                              self.magnetometer_y: self.magnetometer_y+self._median,\
                                                                              self.magnetometer_z: self.magnetometer_z+self._median})
        magnetometer_frame = pandas.concat([magnetometer_frame_mean, magnetometer_frame_median], axis=1)
        return magnetometer_frame
    
    # Drop not needed columns
    def __drop_columns(self, sensor_frame, dropable_columns):
        dropped_frame = sensor_frame
        dropped_frame = dropped_frame.drop(dropable_columns+'_x', axis=1)
        dropped_frame = dropped_frame.drop(dropable_columns+'_y', axis=1)
        dropped_frame = dropped_frame.drop(dropable_columns+'_z', axis=1)
        return dropped_frame
    
    # Synchronize audio
    def __synchronize_audio(self, audio_frame, date):
        audio_frame_max = audio_frame
        audio_frame_max = audio_frame_max.groupby(audio_frame_max.index // 105).max()
        audio_frame_max = audio_frame_max.rename(columns={self.audio: self.audio+self._max})
        audio_frame_max_quantile = audio_frame
        audio_frame_max_quantile = audio_frame_max_quantile.groupby(audio_frame_max_quantile.index // 105).quantile(.95)
        audio_frame_max_quantile = audio_frame_max_quantile.rename(columns={self.audio: self.audio+self._max_quantile})
        synchronized_audio = audio_frame_max.merge(audio_frame_max_quantile, left_index=True, right_index=True)
        synchronized_audio.index = pandas.to_datetime(synchronized_audio.index * 50, unit='ms', origin=pandas.Timestamp(date))
        return synchronized_audio