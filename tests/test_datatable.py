#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  6 09:30:40 2020

@author: ageiges
"""
import datatoolbox as dt
import numpy as np


def test_creation():
    metaDict = {'source' : 'TEST',
                    'entity' : 'values',
                    'unit' : 'm'}
    metaDict2 = {'source' : 'TEST2',
                'entity' : 'area',
                'unit' : 'km'}    
        
    data = np.asarray([[1,2.2,3,4 ],
                       [2.3, np.nan, 3.4, np.nan],
                       [1.3, np.nan, np.nan, np.nan],
                       [np.nan, 3.4, 2.4, 3.2]])
    
    
    data2 = np.asarray([[1,2.2,3,4.5 ],
                   [2.3, np.nan, 3.4, np.nan],
                   [1.1, np.nan, np.nan, np.nan],
                   [np.nan, 3.3, 2.4, np.nan]])
    
    
    df = dt.Datatable(data, meta=metaDict, columns = [2010, 2012, 2013, 2015], index = ['ARG', 'DEU', 'FRA', 'GBR'])
    df2 = dt.Datatable(data2, meta=metaDict2, columns = [2009, 2012, 2013, 2015], index = ['ARG', 'DEU', 'FRA', 'GBR'])
    
    
    assert isinstance(df, dt.Datatable)
    assert isinstance(df2, dt.Datatable)
    
    
test_creation()