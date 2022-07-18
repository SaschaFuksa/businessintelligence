import pandas
import numpy

class EventPreparer:
    
    # Prepare event file to dataframe
    def prepare_event(self, event_file):
        event_dataframe = pandas.read_excel(event_file, skiprows = 2)
        event_dataframe = pandas.DataFrame(event_dataframe, columns=['Zeitpunkt',\
                                                                 'Ressource',\
                                                                 'Werkzeug',\
                                                                 'Programm',\
                                                                 'Auftrag',\
                                                                 'Ma.alarm',\
                                                                 'Alarmtext',\
                                                                 'Typ',\
                                                                 'Ereignisinfo',\
                                                                 'Status',\
                                                                 'Stückzahl',\
                                                                 'Klasse',\
                                                                 'Datum',\
                                                                 'Schicht',\
                                                                 'Artikel',\
                                                                 'Material'])
        event_dataframe = self.__rename_event(event_dataframe)
        event_dataframe = self.__prepare(event_dataframe)
        event_dataframe = self.__create_color_col(event_dataframe)
        event_dataframe = self.__create_past_time_col(event_dataframe)
        event_dataframe = self.__add_offset_col(event_dataframe)
        return event_dataframe
    
    # Rename columns of event log
    def __rename_event(self, event_dataframe):
        event_dataframe = event_dataframe.rename(columns={'Zeitpunkt':'timer'})
        event_dataframe = event_dataframe.rename(columns={'Ressource':'ressource'})
        event_dataframe = event_dataframe.rename(columns={'Werkzeug':'tool'})
        event_dataframe = event_dataframe.rename(columns={'Programm':'program'})
        event_dataframe = event_dataframe.rename(columns={'Auftrag':'order'})
        event_dataframe = event_dataframe.rename(columns={'Ma.alarm':'employeeAlert'})
        event_dataframe = event_dataframe.rename(columns={'Alarmtext':'alertText'})
        event_dataframe = event_dataframe.rename(columns={'Typ':'type'})
        event_dataframe = event_dataframe.rename(columns={'Ereignisinfo':'eventInfo'})
        event_dataframe = event_dataframe.rename(columns={'Status':'status'})
        event_dataframe = event_dataframe.rename(columns={'Stückzahl':'quantity'})
        event_dataframe = event_dataframe.rename(columns={'Klasse':'class'})
        event_dataframe = event_dataframe.rename(columns={'Datum':'date'})
        event_dataframe = event_dataframe.rename(columns={'Schicht':'shift'})
        event_dataframe = event_dataframe.rename(columns={'Artikel':'article'})
        event_dataframe = event_dataframe.rename(columns={'Material':'material'})
        return event_dataframe
    
    # set timer range
    def __prepare(self, event_dataframe):
        event_dataframe = event_dataframe.loc[(event_dataframe['timer'] > '2021-10-15 09:04:36') & (event_dataframe['timer'] < '2021-11-01 03:01:21')]
        event_dataframe = event_dataframe.set_index(event_dataframe.timer)
        return event_dataframe
    
    # create new col color from status
    def __create_color_col(self, event_dataframe):
        event_dataframe['color'] = numpy.nan
        event_dataframe.loc[event_dataframe['status'].str.contains('Alarm', na=False), 'color'] = 'RED'
        event_dataframe.loc[event_dataframe['status'].str.contains('Automat', na=False), 'color'] = 'GREEN'
        event_dataframe.loc[event_dataframe['status'].str.contains('Offline', na=False), 'color'] = 'GREY'
        event_dataframe.loc[event_dataframe['status'].str.contains('Störung', na=False), 'color'] = 'BLUE'
        event_dataframe.loc[event_dataframe['status'].str.contains('Stillstand', na=False), 'color'] = 'BLUE'
        event_dataframe.loc[event_dataframe['status'].str.contains('Nicht', na=False), 'color'] = 'WHITE'
        return event_dataframe
    
    # calculate timedelta between each event into new column
    def __create_past_time_col(self, event_dataframe):
        event_dataframe['tvalue'] = event_dataframe.index
        event_dataframe['pastTime'] = (event_dataframe['tvalue'].shift(-1)-event_dataframe['tvalue']).fillna(pandas.Timedelta('0 days'))
        event_dataframe.drop('tvalue', 1, inplace = True)
        event_dataframe['pastTime'] = event_dataframe['pastTime'].astype(str).map(lambda x: x[7:])
        return event_dataframe
    
    # Add column with calculated offset
    def __add_offset_col(self, event_dataframe):
        event_dataframe['timerOffsetSensor'] = numpy.nan
        given_time = event_dataframe['timer']
        n = 80
        event_dataframe['timerOffsetSensor'] = given_time - pandas.DateOffset(seconds=n)
        event_dataframe = event_dataframe.drop('timer', axis=1)
        return event_dataframe