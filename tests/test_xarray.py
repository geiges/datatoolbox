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


def test_import():
    import xarray
    
    
def test_to_xarray_interface():
    #%%
    inv = dt.findp(variable = ['**Ones', '**Fives'])
    
    tableSet = dt.getTables(inv.index)
    
    
    xData = tableSet.to_xarray(dimensions= ['region', 'time', 'category'])
    
    assert xData.attrs['unit'] == 'm'
    assert xData.attrs['source'] == 'Numbers_2020'
    assert xData.attrs['entity'] == 'Numbers'

    assert xData.sum() == 120
    
    dimSize, dimList = dt.core.get_dimension_extend(tableSet, dimensions= ['region', 'time', 'pathway'])
    

def test_to_xdset_interface():

    inv = dt.findp(variable = 'Numbers**',)
    
    tableSet = dt.getTables(inv.index)
    
    
    xData = tableSet.to_xset()
    
    assert len(xData.time) == 5
    assert len(xData.region) == 4
    
    assert list(xData.data_vars) == list(tableSet.keys())
    assert xData[list(xData.data_vars)[0]].attrs['unit'] == tableSet[list(tableSet.keys())[0]].meta['unit']
    
    
def test_quantified_xdataset_from_tableset():
    res = dt.findp(source = 'Numbers_2020')
    tables = dt.getTables(res.index)
    
    ds = dt.tools.xarray.key_set_to_xdataset(tables)
    
    assert ds.dims['time'] == 5
    assert ds.dims['pathway'] == 1
    assert ds.dims['region'] == 4
    assert list(ds.keys()) == ['Numbers|Ones', 'Numbers|Fives']
    assert hasattr(ds['Numbers|Ones'], 'pint') # check that dataset is quantified with unit
    
def test_quantified_xdataset_from_query_result():
    res = dt.findp(source = 'Numbers_2020')
    # tables = dt.getTables(res.index)
    
    ds = dt.tools.xarray.load_as_xdataset(res)
    
    assert ds.dims['time'] == 5
    assert ds.dims['pathway'] == 1
    assert ds.dims['region'] == 4
    assert list(ds.keys()) == ['Numbers|Ones', 'Numbers|Fives']
    assert hasattr(ds['Numbers|Ones'], 'pint') # check that dataset is quantified with unit
    
#%%    
dt.admin.switch_database_to_testing()

#if False:
    #%%
#    import xarray as xr
#    table = tableSet['Emissions|CO2|Total__Medium|Projection__SOURCE_A_2020']
##    table = table.loc[['World'],:]
#    table2 = tableSet['Emissions|CO2|Total__Historic__SOURCE_A_2020']
#    xset = xr.Dataset({'x' : table})
#    xset['y'] = table2
#    print(xset)
#    print(xset['y'])