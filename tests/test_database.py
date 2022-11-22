#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 28 19:56:03 2020

@author: ageiges
"""
import copy
import datatoolbox as dt

from util_for_testing import df, df2, sourceMeta
 
dt.admin.switch_database_to_testing()

def test_validate():
    dt.core.DB._validateRepository()
    
def test_commit_new_table():
    df.loc['ARG', 2012] = 10
    dt.commitTable(df, 'add first table', sourceMeta)
    
    
def test_validate_ID():
    assert dt.validate_ID(list(dt.findc().index)[0])
    
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
    dt.removeTable('Emissions|CO2|transport__Historic__XYZ_2020')
    
    assert not dt.isAvailable(df.ID)
  
def test_commit_mutliple_tables():
    dt.commitTables([df, df2], 'adding set of table', sourceMeta)

def test_delete_mutliple_tables():
    dt.removeTables([df.ID, df2.ID])
    
    assert not dt.isAvailable(df.ID)
    assert not dt.isAvailable(df.ID)
    
def test_delete_source():    
    dt.core.DB.removeSource('XYZ_2020')
    
def test_find_p():
    inv = dt.findp(variable = 'Numbers|Ones',
                   source='Numbers_2020')    
    
    
    assert len(inv.variable.unique()) ==1
    
    inv = dt.findp(variable = 'Numbers**',
                   source='Numbers_2020')    
    
    
    assert len(inv.variable.unique()) ==2
    
    
def test_table_logging():
    # create tempory table in DB
    table = dt.getTable('Numbers|Ones__Historic__Numbers_2020')
    tableNew = table*2
    tableNew.meta.update(table.meta)
    tableNew.meta.update({'category':'Twos'})
    dt.commitTable(tableNew, 'test table')
    def test_analysis():

        table = dt.getTable('Numbers|Twos__Historic__Numbers_2020')
        print(table.sum())

    # run to save all required table for the analysis
    dt.core.DB.startLogTables()
    test_analysis()
    dt.core.DB.stopLogTables()
    dt.core.DB.save_logged_tables()
    
    # remove table form database
    dt.removeTable('Numbers|Twos__Historic__Numbers_2020')
    
    #run analysis with missing table, but locally stored
    test_analysis()    
    
    #cleanup 
    import shutil
    shutil.rmtree('data')
    
if __name__ == '__main__':
    test_commit_new_table()
    test_validate_ID()
    test_update_value_table()
    test_update_meta()
    test_delete_table()
    test_commit_mutliple_tables()
    test_delete_mutliple_tables()
    test_delete_source()
    test_table_logging()