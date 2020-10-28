#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 12:23:20 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy as np
import pandas as pd
import os

def test_to_xarray_interface():
    #%%
    inv = dt.findp(variable = 'Emissions|CO2|Total',
                  source='SOURCE_A_2020')
    
    tableSet = dt.getTables(inv.index)
    
    
    xData = tableSet.to_xarray(dimensions= ['region', 'time', 'pathway'])
    
    assert xData.attrs['unit'] == 'Mt CO2'
    assert xData.attrs['source'] == 'SOURCE_A_2020'
    assert xData.attrs['category'] == 'Total'

    assert xData.sum() == 2895
    
    dimSize, dimList = dt.core.get_dimension_extend(tableSet, dimensions= ['region', 'time', 'pathway'])
    

def test_to_xdset_interface():

    inv = dt.findp(variable = 'Emissions|CO2|Total',
                  source='SOURCE_A_2020')
    
    tableSet = dt.getTables(inv.index)
    
    
    xData = tableSet.to_xset()
    
    assert len(xData.time) == 51
    assert len(xData.region) == 3
    
    assert list(xData.data_vars) == list(tableSet.keys())
    assert xData[list(xData.data_vars)[0]].attrs['unit'] == tableSet[list(tableSet.keys())[0]].meta['unit']
    
#%%    
if False:
    #%%
    import xarray as xr
    table = tableSet['Emissions|CO2|Total__Medium|Projection__SOURCE_A_2020']
#    table = table.loc[['World'],:]
    table2 = tableSet['Emissions|CO2|Total__Historic__SOURCE_A_2020']
    xset = xr.Dataset({'x' : table})
    xset['y'] = table2
    print(xset)
    print(xset['y'])
    
    
    


