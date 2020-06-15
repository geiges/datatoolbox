#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 13:57:46 2020

@author: ageiges
"""
import numpy as np
import datatoolbox as dt 

data = np.asarray([[1,2.2,3,4 ],
                   [2.3, np.nan, 3.4, np.nan],
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


df2 = dt.Datatable(data2, meta=metaDict2, columns = [2008, 2012, 2013, 2015], index = ['TUN', 'DEU', 'FRA', 'GBR'])
    