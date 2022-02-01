#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 13:57:46 2020

@author: ageiges
"""
import numpy as np
import datatoolbox as dt 


#%% Datatables for testing 
data = np.asarray([[1,2.2,3,4 ],
                    [1, np.nan, 4, np.nan],
                    [1.3, np.nan, np.nan, np.nan],
                    [np.nan, 3.4, 2.4, 3.2]])


df = dt.Datatable(data, 
                  columns = [2010, 2012, 2013, 2015], 
                  index = ['ARG', 'DEU', 'FRA', 'GBR'],
                  meta={'entity' : 'Emissions|CO2',
                        'scenario' : 'Historic',
                        'source' : 'XYZ_2020',
                        'unit' : 'm'}, )

sourceMeta = {'SOURCE_ID': 'XYZ_2020',
              'collected_by': dt.config.CRUNCHER,
              'date': 'last day',
              'source_url': 'www.sandbox.de',
              'licence': 'free for all'}    

metaDict2 = meta={ 'entity' : 'GDP|PPP',
                    'category' : 'Trade|imports',
                    'scenario' : 'Historic',
                    'source' : 'XYZ_2020',
                    'unit' : 'USD 2010'}
    
data2 = np.asarray([[1,22,3,4 ],
                    [23, np.nan, 34, 15],
                    [13, 1e6, np.nan, 41],
                    [np.nan, 34, 27.4, 32]])
data3 = np.asarray([[1,22,3,4 ],
                    [23, np.nan, 34, 15],
                    [13, 1e6, np.nan, 41],
                    [np.nan, 34, 27.4, 32]])

df1 = df
df2 = dt.Datatable(data2, meta=metaDict2, columns = [2008, 2012, 2013, 2015], index = ['TUN', 'DEU', 'FRA', 'GBR'])
df3 = dt.Datatable(data3, meta=metaDict2, columns = [2008, 2012, 2013, 2015], index = ['TUN', 'DEU', 'FRA', 'GBR'])
df3 =   df = dt.Datatable(data, 
                  columns = [2010, 2012, 2013, 2015], 
                  index = ['ARG', 'DEU', 'FRA', 'GBR'],
                  meta={'entity' : 'Emissions|CO2',
                        'scenario' : 'Historic',
                        'source' : 'XYZ_2020',
                        'unit' : 'm'}, )
df3.loc['ARG', 2014] = 5


# %%Datetime datatable
data_datetime = np.asarray([[1, 22    , 3     ,4  ],
                           [23, np.nan, 34    , 15],
                           [13, 1e2   , np.nan, 41],
                           [10, np.nan, np.nan, 35]])

meta_datetime = { 'entity' : 'GDP|PPP',
                   'category' : 'Trade|imports',
                   'scenario' : 'Historic',
                   'source' : 'table_with_datetime',
                   'unit' : 'USD 2010',
                   '_timeformat' : '%Y-%m-%d %H:%M:%S'}

df_datetime = dt.Datatable(data_datetime, 
                           meta=meta_datetime, 
                           columns = ['2018-08-09 11:00:00', '2018-08-09 12:00:00', '2018-08-09 13:00:00', '2018-08-10 12:00:00'], 
                           index = ['TUN', 'DEU', 'FRA', 'GBR'])
from datatoolbox.tools.for_datatables import interpolate
# print(interpolate(df_datetime).values)