#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:52:37 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy.testing as npt
import numpy as np

from util_for_testing import df1, df_datetime


data = np.asarray([[1,2.2,3,4,5 ],
                   [1, np.nan, 4, np.nan, np.nan],
                   [1.3, np.nan, np.nan, np.nan, np.nan],
                   [np.nan, 3.4, 2.4, 3.2, np.nan]])
df3 =   df = dt.Datatable(data, 
                  columns = [2010, 2012, 2013, 2015, 2014], 
                  index = ['ARG', 'DEU', 'FRA', 'GBR'],
                  meta={'entity' : 'Emissions|CO2',
                       'scenario' : 'Historic',
                       'source' : 'XYZ_2020',
                       'unit' : 'm'}, )

def test_interpolation():
    from datatoolbox.tools.for_datatables import interpolate
    
    
    #interpolation to with columns as years
    resTable = interpolate(df1)
    
    assert (resTable.loc['DEU',2012] == 3)
    assert np.isnan(resTable.loc['GBR',2010])
    assert np.isnan(resTable.loc['DEU',2015])
    assert np.isnan(resTable.loc['FRA',2013])
    
    
    #interpolation to with columns as years
    interpolated_table = df_datetime.interpolate()
    
    assert interpolated_table.loc['GBR', '2018-08-09 12:00:00'] == 11.
    assert interpolated_table.loc['GBR', '2018-08-09 13:00:00'] == 12.
    # test of linked method
    df1.interpolate()
   
def test_aggregation():
    
    from datatoolbox.tools.for_datatables import aggregate_region
    
    mapping= {'EU3': ['DEU', 'GBR', 'FRA']}
    res, missingCountries = aggregate_region(df1, mapping, skipna=True)
    
    npt.assert_array_almost_equal(res.loc['EU3',:].values, 
                                  np.array([2.3, 3.4, 6.4, 3.2]),
                                  decimal = 6)
    npt.assert_array_almost_equal(res.loc['GBR',:].values, 
                                  np.array([np.nan, 3.4, 2.4, 3.2]),
                                  decimal = 6)
    
    
def test_growth_rates():
    
    from datatoolbox.tools.for_datatables import growth_rate
    
    res = growth_rate(df3)
    
    exp = np.array([[np.nan,  1.2 ,  0.36363636,  0.33333333,  0.25      ],
                    [np.nan, np.nan, np.nan, np.nan, np.nan],
                    [np.nan, np.nan, np.nan, np.nan, np.nan],
                    [np.nan, np.nan, -0.29411765,  0.33333333, np.nan]])
    
    npt.assert_almost_equal(res.values, exp, decimal=8)
    assert res.meta['unit'] == 'm'
    
if __name__== '__main__':
    test_interpolation()
    test_aggregation()
    test_growth_rates()
