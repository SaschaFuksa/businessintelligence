import os
        
class FileCollector:
    
    start_date = 20211015
    name_seperator = '_'
    
    # Collect, filter and map files to date
    def get_files_matched_to_date(self, input_path):
        all_files = os.listdir(input_path)
        files = self.__filter_files('.csv', all_files)
        date_to_sensor_files = self.__match_files_to_date(files)
        date_to_sensor_files = self.__remove_dates_before_start_date(date_to_sensor_files)
        return date_to_sensor_files
    
    # Filter pickle files by exlude example files
    def __filter_files(self, file_type, all_files):
        files = []
        for file in all_files:
            if file.endswith(file_type) and 'example' not in file:
               files.append(file)
        return files
    
    # Match files to date
    def __match_files_to_date(self, files):
        date_to_sensor_files = {}
        for file in files:
            file_name =  file.split(self.name_seperator)
            date = int(file_name[2])
            if date in date_to_sensor_files:
                date_to_sensor_files[date].append(file)
            else:
                date_to_sensor_files[date] = [file]
        return date_to_sensor_files
    
    # Remove dates before specific start date
    def __remove_dates_before_start_date(self, date_to_sensor_files):
        date_to_sensor_files_since_start_date = {}
        for date in date_to_sensor_files:
            if date > self.start_date:
                date_to_sensor_files_since_start_date[date] = date_to_sensor_files[date]
        return date_to_sensor_files_since_start_date
    
    # Collect, filter and map files to position
    def get_files_matched_to_position(self, input_path):
        all_files = os.listdir(input_path)
        files = self.__filter_files('.csv', all_files)
        position_to_sensor_files = self.__match_files_to_position(files)
        return position_to_sensor_files
    
    # Match files to position
    def __match_files_to_position(self, files):
        position_to_sensor_files = {}
        for file in files:
            file_name =  file.split(self.name_seperator)
            position = file_name[1]
            if position in position_to_sensor_files:
                position_to_sensor_files[position].append(file)
            else:
                position_to_sensor_files[position] = [file]
        return position_to_sensor_files
    
    # Collect and return event file
    def get_event_file(self, input_path):
        all_files = os.listdir(input_path)
        event_file = None
        for file in all_files:
            if file.endswith('.xlsx') and 'Ereignis' in file:
                event_file = file
                break
        return event_file
    
    # Collect and return production file
    def get_production_file(self, input_path):
        all_files = os.listdir(input_path)
        production_file = None
        for file in all_files:
            if file.endswith('.xlsx') and 'Produktion' in file:
                production_file = file
                break
        return production_file
    
    
    # get inside file
    def get_inside_file(self, sensor_files):
        for file in sensor_files:
            if 'inside' in file:
                return file
        
    # get outside file
    def get_outside_file(self, sensor_files):
        for file in sensor_files:
            if 'outside' in file:
                return file