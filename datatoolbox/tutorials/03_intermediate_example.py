#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:21:32 2020

@author: Andreas Geiges

TUTORIAL: Temperature categorization

GOAL: Classify a given variable in the IAMC database according to the temperature
      response in 2100 and visualize the outcome
      
      
"""

import datatoolbox as dt
import pandas as pd
import matplotlib.pyplot as plt

#%% SETUP

# define the data source used for this analysis
source = 'IAMC15_2019_R2'

# define the variable which is analyzed
variable = 'Emissions|CO2|Energy|Demand|Transportation'

#define years that you want to compare
years  = list(range(2000,2101,10))


# %% Data preparation
#search for the specifice reported entity 'Emissions|CO2|Energy|Demand|Transportation' within the source
searchResult = dt.find(entity =variable, source= source)

# extract the list of scenarios that report this varibble
scenarios = searchResult.scenario.unique()

# dictionary to be used to save scenario/model combinations according to the temperarture response
scenarioCategory = {'1.5':[],
              '2':[],
              '3':[],
              '4':[],
              '>4':[]}

# dictionay to temporaly saving the data to be able to compute statistics
categorizedData = {'1.5':pd.DataFrame(columns=list(range(2000,2101,10))) ,
              '2':pd.DataFrame(columns=list(range(2000,2101,10))),
              '3':pd.DataFrame(columns=list(range(2000,2101,10))),
              '4':pd.DataFrame(columns=list(range(2000,2101,10))),
              '>4':pd.DataFrame(columns=list(range(2000,2101,10)))}

# looping over all scenarios that report the variable
for scenario in scenarios:
    
    # ID of the temperature data for the scenarios (median of the MAGICC output)
    tableID ='|'.join(['Global_temp_MAGICC_p50', 'total', scenario, source])
    
    
    if dt.isAvailable(tableID): # checking that the table is in the database
        
        # reading the data using getTable
        table = dt.getTable(tableID)
        
        # dependen on the temperature in 2100, scenarios are put into different bins
        if table.loc['World',2100] < 1.5:
            scenarioCategory['1.5'].append(scenario)
        elif table.loc['World',2100] < 2:
            scenarioCategory['2'].append(scenario)
        elif table.loc['World',2100] < 3:
            scenarioCategory['3'].append(scenario)
        elif table.loc['World',2100] < 4:
            scenarioCategory['4'].append(scenario)
        else:
            scenarioCategory['>4'].append(scenario)
    else:
        pass

#%% temporarly saving the emission data 
        
for category in scenarioCategory.keys():
    
    for scenario in scenarioCategory[category]:
        tableID ='|'.join([variable, '', scenario, source])
        if dt.isAvailable(tableID):
            # loading the data
            table = dt.getTable(tableID) 
            
            # extracting the data for the the region "world" and selected years
            values = table.loc['World',years] 

            #storing the values of the time series in the temporal data set
            categorizedData[category].loc[scenario] = values

          
#%% Defintion of a plotting function
def plotSubplot(subplotData, temp, scenarioCategory, color):
    
    ax = plt.subplot(*subplotData)
    for scenario in scenarioCategory[temp]:
        # creating ID
        tableID ='|'.join([variable, '', scenario, source])
        if dt.isAvailable(tableID):
            # loading the data
            table = dt.getTable(tableID) 
            
            # extracting the data for the the region "world" and selected years
            values = table.loc['World',years] 
            
            # pltting the data using matplotlib
            h = plt.plot(years, values,color, linewidth=1)
        else:
            print(scenario + ' IS NOT AVAILABLE')

            
    plt.ylim([0,20000])
    plt.xlim([2000,2050])
    if subplotData[-1] > 1:
        ax.set_yticklabels([])
    plt.legend([h[0]],[ temp + '°C'])
    
#%% Create comparison plot

plt.figure(1, figsize=[10,8])

# plotting the individual scenarios
plt.clf()
plotSubplot((2,5,1), '>4', scenarioCategory, 'black')
plotSubplot((2,5,2), '4', scenarioCategory, '#DF7358')
plotSubplot((2,5,3), '3', scenarioCategory, '#E8B162')                        
plotSubplot((2,5,4), '2', scenarioCategory, '#F0E67F')     
plotSubplot((2,5,5), '1.5', scenarioCategory, '#B6CC79')     

# plotting the median of the five categories
plt.subplot(2,1,2)
plt.plot(categorizedData['>4'].columns, categorizedData['>4'].median(axis=0),'k', linewidth=2)
plt.plot(categorizedData['4'].columns, categorizedData['4'].median(axis=0),'red', linewidth=2)
plt.plot(categorizedData['3'].columns, categorizedData['3'].median(axis=0),'orange', linewidth=2)
plt.plot(categorizedData['2'].columns, categorizedData['2'].median(axis=0),'y', linewidth=2)
plt.plot(categorizedData['1.5'].columns, categorizedData['1.5'].median(axis=0),'g', linewidth=2)
plt.xlim([2000,2050])
plt.tight_layout()
plt.legend(['median ' + temp + '°C' for temp in ['>4','<4','<3','<2','<1.5']])
plt.text(2015,1150,variable)


    
