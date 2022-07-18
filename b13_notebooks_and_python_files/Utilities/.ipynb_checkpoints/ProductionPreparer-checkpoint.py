import pandas

class ProductionPreparer:
    
    # Prepare production file to dataframe
    def prepare_production(self, production_file):
        production_dataframe = pandas.read_excel(production_file, skiprows = 19)
        production_dataframe = pandas.DataFrame(production_dataframe, columns=['f1403',\
                                                                                't008',\
                                                                               't007',\
                                                                               'p4055',\
                                                                               'p4072',\
                                                                               'V4062',\
                                                                               't4012',\
                                                                               't4015',\
                                                                               't4018',\
                                                                               'T805I',\
                                                                               'T840I',\
                                                                               't4052'])
        production_dataframe = production_dataframe.iloc[3:]
        production_dataframe = self.__rename_col_prod(production_dataframe)
        production_dataframe = self.__create_timer_prod(production_dataframe)
        production_dataframe['logCycleCounter']= production_dataframe.logCycleCounter.astype('int64')
        return production_dataframe
    
    # Rename columns of prod log
    def __rename_col_prod(self, production_dataframe):
        production_dataframe = production_dataframe.rename(columns={'f1403':'logCycleCounter'})
        production_dataframe = production_dataframe.rename(columns={'t008':'day_month'})
        production_dataframe = production_dataframe.rename(columns={'t007':'time'})
        production_dataframe = production_dataframe.rename(columns={'p4055':'maximumSprayPressure, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'p4072':'changeoverPressure, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'V4062':'massPad, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'t4012':'cycleTime, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'t4015':'dosingTime, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'t4018':'injectionTime, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'T805I':'cylinderHeatingZone 5, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'T840I':'toolHeatingCircuit 10, ActualValue'})
        production_dataframe = production_dataframe.rename(columns={'t4052':'toolSavingTime, ActualValue'})
        return production_dataframe
    
    # Create col timer, remove old cols and reduce time slot
    def __create_timer_prod(self, production_dataframe):
        production_dataframe['day_month'] = production_dataframe['day_month'].astype(str)
        production_dataframe['day_month'] = production_dataframe['day_month'].str.rstrip('00:00:00')
        production_dataframe['timer'] = production_dataframe['day_month'].astype(str) + production_dataframe['time'].astype(str)
        production_dataframe = production_dataframe.drop('day_month', axis=1)
        production_dataframe = production_dataframe.drop('time', axis=1)
        production_dataframe = production_dataframe.set_index(production_dataframe.timer)
        production_dataframe = production_dataframe[(production_dataframe['timer'] > '2021-10-15 23:59:00') & (production_dataframe['timer'] < '2021-11-01 22:43:00')]
        production_dataframe = production_dataframe.drop('timer', axis=1)
        return production_dataframe