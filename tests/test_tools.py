#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:52:37 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy.testing as npt
import numpy as np

from util import df1, df3

def test_interpolation():
    from datatoolbox.tools.for_datatables import interpolate
    
    resTable = interpolate(df1)
    
    assert (resTable.loc['DEU',2012] == 3)
    assert np.isnan(resTable.loc['GBR',2010])
    assert np.isnan(resTable.loc['DEU',2015])
    assert np.isnan(resTable.loc['FRA',2013])
    
    # test of linked method
    df1.interpolate()
   
def test_aggregation():
    
    from datatoolbox.tools.for_datatables import aggregate_region
    
    mapping= {'EU3': ['DEU', 'GBR', 'FRA']}
    res, missingCountries = aggregate_region(df1, mapping)
    
    npt.assert_array_almost_equal(res.loc['EU3',:].values, 
                                  np.array([2.3, 3.4, 6.4, 3.2]),
                                  decimal = 6)
    npt.assert_array_almost_equal(res.loc['GBR',:].values, 
                                  np.array([np.nan, 3.4, 2.4, 3.2]),
                                  decimal = 6)
    
    
def test_growth_rates():
    
    from datatoolbox.tools.for_datatables import growth_rate
    
    res = growth_rate(df3)
    
    exp = np.array([[ 0.36363636,  0.66666667],
                    [-0.29411765,      np.nan]])
    
    npt.assert_almost_equal(res.values, exp, decimal=8)
    assert list(res.index) == ['ARG','GBR']
    assert list(res.columns) == [2013, 2014]
    assert res.meta['unit'] == 'm'
    
if __name__== '__main__':
    test_interpolation()
    test_aggregation()
    test_growth_rates()