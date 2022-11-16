#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 18:15:56 2020

@author: ageiges
"""

import datatoolbox as dt
import os
import numpy.testing as npt

from util_for_testing import df1, df_datetime

def test_datatable__csv_io():

    
    for df in df1, df_datetime:
    
        filePath = os.path.join('/tmp','test.csv')
        
        df.to_csv(filePath)
    
        df_copy = dt.data_structures.read_csv(filePath)
        
        obs = df_copy
        exp = df
        npt.assert_array_almost_equal(obs, exp, decimal = 6)
        
        os.remove(filePath)
    
def test_datatable_excel_io():

    
    
    for df in df1, df_datetime:
    
        filePath = os.path.join('/tmp','test.xlsx')
        
        df.to_excel(filePath)
    
        df_copy = dt.data_structures.read_excel(filePath)
        
        obs = df_copy
        exp = df
        npt.assert_array_almost_equal(obs, exp, decimal = 6)
        
        os.remove(filePath)

if __name__== '__main__':
    test_datatable__csv_io()
    test_datatable_excel_io()
    