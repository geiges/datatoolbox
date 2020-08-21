#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:52:37 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy.testing as npt
import numpy as np

from util import df1

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
if __name__== '__main__':
    test_interpolation()
    test_aggregation()