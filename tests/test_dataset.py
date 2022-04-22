#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 14:49:27 2022

@author: ageiges
"""

import copy
import datatoolbox as dt

from util_for_testing import df, df2, sourceMeta
 
dt.admin.switch_database_to_testing()

def test_dataset_from_query():
    
    res = dt.findp() # find all
    ds = dt.DataSet.from_query(res)

def test_sel_methods():
    res = dt.findp() # find all
    ds = dt.DataSet.from_query(res)
    
    sub = ds.sel(scenario='Historic')
    assert dict(sub.dims) =={'year': 5, 'region': 4, 'model': 1}
    
    sub = ds.sel(region=0)
    assert dict(sub.dims) == {'year': 5, 'pathway': 1}
    
    sub = ds.sel(year=2012)
    assert dict(sub.dims) == {'region': 4, 'pathway': 1}

def test_unit_conversion():
    res = dt.findp() # find all
    ds = dt.DataSet.from_query(res)
    mm_data = ds['Numbers|Fives'].pint.to('mm')
    
    assert (mm_data.values == ds['Numbers|Fives'].values*1000).all()
    
    # test Emission unit
    test_array = ds['Numbers|Fives'].pint.dequantify().pint.quantify('Mt CO2 / yr')
    test_array = test_array.pint.to('kt CO2/ d')

    assert (test_array.values == 13.689253935660503).all()
