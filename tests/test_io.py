#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 18:15:56 2020

@author: ageiges
"""

import datatoolbox as dt
import os
import numpy.testing as npt

def test_csv_io():
    metaDict = {'entity':'Emissions|Candy',
                               'category': '',
                               'scenario':'Historic',
                               'source': 'SANDBOX',
                               'unit' : 'Mt CO2'}
    
    
    
    df1 = dt.Datatable([[10,20,30,],
                       [40,40,50,]], 
                       columns = [2000, 2010, 2020],
                       index= ['DEU', 'IND'],
                       meta = metaDict)
    
    filePath = os.path.join(dt.config.PATH_TO_DATASHELF,'test.csv')
    
    df1.to_csv(filePath)

    df_copy = dt.data_structures.read_csv(filePath)
    
    obs = df_copy
    exp = df1
    npt.assert_array_almost_equal(obs, exp, decimal = 6)
    
    os.remove(filePath)
