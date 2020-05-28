#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Andreas Geiges

TUTORIAL:       
      
"""

import datatoolbox as dt
import pandas as pd
import matplotlib.pyplot as plt

#%% seach for data
dt.find(entity = 'Emissions|CO2').source.unique()
source = 'WDI_2019'
res = dt.find(entity = 'Emissions|CO2',source=source)
res.scenario.unique()
res.entity.unique()
res.index

#%%load data
data = dict()
data['APAC']   = dt.getTable('Emissions|CO2|Energy|Total||Historic|APAC_2019')
data['UNFCCC'] = dt.getTable('Emissions|CO2|IPC0||Historic|CR|UNFCCC_CRF_2019')
data['WDI']    = dt.getTable('Emissions|CO2||historic|WDI_2019')
data['PRIMAP'] = dt.getTable('Emissions_CO2|IPCM0EL|HISTCR|PRIMAP_2018').convert('Mt CO2')
data['IEA']    = dt.getTable('Emissions|Fuel|CO2|Total||historic|IEA_CO2_FUEL_2019')


#%% plot data
coISO = 'USA'
plt.clf()
for key in data.keys():
    if coISO in data[key].index:
        plt.plot(data[key].columns,data[key].loc[coISO], label = key)
plt.legend()
