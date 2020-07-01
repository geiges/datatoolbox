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
data['APAC']   = dt.getTable('Emissions|CO2||Energy|Total__Historic__APAC_2019')
data['UNFCCC'] = dt.getTable('Emissions|CO2||IPC0__Historic|CR__UNFCCC_CRF_2019')
data['WDI']    = dt.getTable('Emissions|CO2__Historic__WDI_2019')
data['PRIMAP'] = dt.getTable('Emissions|CO2|IPCM0EL__Historic|country_reported__PRIMAP_2019').convert('Mt CO2')
data['IEA']    = dt.getTable('Emissions|CO2|combustion|Total__Historic__IEA_CO2_FUEL_2019')


#%% plot data
coISO = 'USA'
plt.clf()
for key in data.keys():
    if coISO in data[key].index:
        plt.plot(data[key].columns,data[key].loc[coISO], label = key)
plt.legend()
