#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 14:48:08 2022

@author: ageiges
"""

import copy
import datatoolbox as dt

from util_for_testing import df, df2, sourceMeta
 
dt.admin.switch_database_to_testing()

def test_to_xdataset():
    idf = dt.findp(source='Numbers_2020').as_pyam()
    
    ds1 = dt.converters.to_xdataset(idf)
    
    tables = dt.findp(source='Numbers_2020').load()
    
    ds2 = dt.converters.to_xdataset(tables)


def test_to_pyam():
    tables = dt.findp(source='Numbers_2020').load()
    
    for table in tables:
        table.meta['model'] = 'test'
    
    ds = dt.converters.to_xdataset(tables)
    
    idf = dt.converters.to_pyam(ds)
