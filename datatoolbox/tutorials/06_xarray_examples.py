#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 14 16:58:04 2020

@author: ageiges
"""

import datatoolbox as dt

#%%
pattern = 'Electricity_generation|Solar**'
pattern = 'Electricity_generation|Hydro**'
#pattern = 'Electricity|generation|Wind**'
#pattern ='Electricity|generation|Renewables**'
#pattern = 'Primary_Energy|Total**'
#pattern = 'Primary_Energy|Gas**'
res = dt.findp(variable = pattern, source='IEA_WEB**')

tSet = dt.getTables(res.index)
xData = tSet.to_xarray(dimensions=['region', 'year', 'source'])

        #%%
import matplotlib.pyplot as plt
plt.clf()
coISO = 'CHN'
plt.plot(xData.year, xData.loc[coISO,:,:].values.squeeze())
plt.legend(xData.source.values)
plt.title(pattern + ' - ' + coISO)
