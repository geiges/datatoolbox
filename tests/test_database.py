#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 28 19:56:03 2020

@author: ageiges
"""
import copy
import datatoolbox as dt
from util import df, df2, sourceMeta
 

def test_validate():
    dt.core.DB._validateRepository()
    
def test_commit_new_table():
    df.loc['ARG', 2012] = 10
    dt.commitTable(df, 'add first table', sourceMeta)
    
    
def test_validate_ID():
    assert dt.validate_ID(list(dt.find().index)[0])
    
def test_update_value_table():
    
    df.loc['ARG', 2012] = 20
    print(df.ID)
    dt.updateTable(df.ID, df, 'update value in table')

def test_update_meta():
    df.meta['unit'] = 'Mt CO2'
    df.meta['entity'] = 'Emissions|CO2|transport'
    oldID = copy.copy(df.ID)
    df.generateTableID()
    dt.updateTable(oldID, df, 'update meta data of table')
    
    assert  'Emissions|CO2|transport__Historic__XYZ_2020' in dt.core.DB.inventory.index
    
def test_delete_table():
    dt.removeTable(df.ID)
    
    assert not dt.isAvailable(df.ID)
  
def test_commit_mutliple_tables():
    dt.commitTables([df, df2], 'adding set of table', sourceMeta)

def test_delete_mutliple_tables():
    dt.removeTables([df.ID, df2.ID])
    
    assert not dt.isAvailable(df.ID)
    assert not dt.isAvailable(df.ID)
    
def test_delete_source():    
    dt.core.DB.removeSource('XYZ_2020')
    
if __name__ == '__main__':
    test_commit_new_table()
    test_validate_ID()
    test_update_value_table()
    test_update_meta()
    test_delete_table()
    test_commit_mutliple_tables()
    test_delete_mutliple_tables()
    test_delete_source()
