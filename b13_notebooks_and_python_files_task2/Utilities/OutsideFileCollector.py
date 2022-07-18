import os
        
class OutsideFileCollector:
    
      # get outside file
    def get_outside_files(self, sensor_files):
        files = []
        for file in sensor_files:
            if ('outside' in file) and (file.endswith('.csv')):
                files.append(file)
        return files
    
            
        
            