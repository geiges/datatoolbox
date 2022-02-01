#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 14 19:43:58 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy as np
import pandas as pd


metaDict = {'source'   : 'TEST',
            'entity'   : 'values',
            'category' : 'cat1',
            'scenario' : '#1',
            'unit'     : 'm'}
metaDict2 = {'source' : 'TEST2',
            'entity'  : 'area',
            'category': 'cat2',
            'scenario': '#2',
            'unit'    : 'km'}    
    
data = np.asarray([[1,2.2],
                   [2.3, np.nan],
                   [1.3, np.nan],
                   [np.nan, 3.4, ]])


data2 = np.asarray([[1,2.2],
               [2.3, np.nan],
               [1.1, np.nan ],
               [np.nan, 3.3,]])


df = dt.Datatable(data, meta=metaDict, columns = [2010, 2012], index = ['ARG', 'DEU', 'FRA', 'GBR'])
df2 = dt.Datatable(data2, meta=metaDict2, columns = [2009, 2012], index = ['ARG', 'DEU', 'FRA', 'GBR'])
    
    

def test_basics():
    
    
    tableSet = dt.TableSet()
    
    assert hasattr(tableSet,'inventory')
    
    
def test_add_remove():
    tableSet = dt.TableSet()
    tableSet.add(df)
    
    obs = list(tableSet.keys())
    exp = ['values|cat1__#1__TEST']
    assert obs[0] == exp[0]
    
    tableSet.add(df2)
    assert len(tableSet.inventory.index) == 2
    tableSet.remove(df.ID)
    
    obs = list(tableSet.keys())
    exp = ['area|cat2__#2__TEST2']
    assert obs == exp
    
    assert len(tableSet.inventory.index) == 1

def test_add_list():
    tableSet = dt.TableSet()
    tableSet.add([df, df2])
    obs = list(tableSet.keys())
    exp = ['values|cat1__#1__TEST', 'area|cat2__#2__TEST2']
    assert obs == exp
    
def test_to_long_table():
    
    tableSet = dt.TableSet()
    tableSet.add([df, df2])
    
    longTable = tableSet.to_LongTable()

    exp = pd.DataFrame.from_dict(
            {'variable': {0: 'values|cat1',
              1: 'values|cat1',
              2: 'values|cat1',
              3: 'values|cat1',
              4: 'area|cat2',
              5: 'area|cat2',
              6: 'area|cat2',
              7: 'area|cat2'},
            'region': {0: 'ARG',
              1: 'DEU',
              2: 'FRA',
              3: 'GBR',
              4: 'ARG',
              5: 'DEU',
              6: 'FRA',
              7: 'GBR'},
            'scenario': {0: '#1',
              1: '#1',
              2: '#1',
              3: '#1',
              4: '#2',
              5: '#2',
              6: '#2',
              7: '#2'},
            'model': {0: '', 1: '', 2: '', 3: '', 4: '', 5: '', 6: '', 7: ''},
             'unit': {0: 'm', 1: 'm', 2: 'm', 3: 'm', 4: 'km', 5: 'km', 6: 'km', 7: 'km'},
             2009: {0: np.nan, 1: np.nan, 2: np.nan, 3: np.nan, 4: 1.0, 5: 2.3, 6: 1.1, 7: np.nan},
             2010: {0: 1.0, 1: 2.3, 2: 1.3, 3: np.nan, 4: np.nan, 5: np.nan, 6: np.nan, 7: np.nan},
             2012: {0: 2.2, 1: np.nan, 2: np.nan, 3: 3.4, 4: 2.2, 5: np.nan, 6: np.nan, 7: 3.3},
             }
            )
    # exp = exp.loc[:,longTable.columns]
    
    assert (longTable.values[~pd.isna(longTable)] == exp.values[~pd.isna(exp)]).all()

def test_supplement_material():
    tableSet = dt.TableSet()
    tableSet.add([df, df2])

    assert  tableSet.scenarios() == ['#1', '#2']
    assert  tableSet.entities()  == ['values', 'area']
    assert  tableSet.sources()   == ['TEST', 'TEST2']
    
    
if __name__ == '__main__':    
    test_basics()
    test_add_remove()
    test_add_list()
    test_to_long_table()
    test_supplement_material()