import pandas
from datetime import datetime

class ShiftRecognizer:
    
    # Add column shift to sensor file
    def add_shift(self, sensor_file):
        date = sensor_file.timer.iloc[0]
        date = datetime.strftime(date, '%Y-%m-%d')
        sensor_file['shift'] = 3
        sensor_file['shift'].mask((sensor_file.timer >= date + ' 06:00:00.000') & (sensor_file.timer < date + ' 14:00:00.000'), 1, inplace=True)
        sensor_file['shift'].mask((sensor_file.timer >= date + ' 14:00:00.000') & (sensor_file.timer < date + ' 22:00:00.000'), 2, inplace=True)
        return sensor_file