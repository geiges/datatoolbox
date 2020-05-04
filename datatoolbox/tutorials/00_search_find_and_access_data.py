#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 18:18:36 2020

@author: Andreas Geiges
"""

import datatoolbox as dt

#%% dt.find usage
# Finds datatables that contain the search string

dt.find(entity='', category='', scenario ='', source='')

#%% The underlying inventory
inventory = dt.find() #returns the full inventory
print(inventory.head())

# data os strucutures in for categories : ['entity', 'category', 'scenario', 'source']
print(inventory.columns)

# Each ID is unique is contructed as the joined string of the categories above using '|'
# Each entry is representing a datatable with the index beeing the ID
print(inventory.index[0:10])

#%% List all data sources
sources = list(dt.find().source.unique())
sources.sort()
print(sources)

#%% List all scenarios within a source
res = dt.find(source='PRIMAP_2019')
print(res.scenario.unique())

#%% List all variables within a source
print(res.entity.unique())

#%% List all Emissions|KYOTOGHG data tables 
res = dt.find(entity = "Emissions|KYOTOGHG", source='PRIMAP_2019')
print(res.entity.unique())

#%% dt.findExact
# Finds datatables that match the search string exactly

# dt.find returns mutltiple matching datatables
res = dt.find(entity = "Emissions|CO2", source='IAMC')
print(res.entity.unique())

# dt.find returns mutltiple matching datatables
res = dt.findExact(entity = "Emissions|CO2", source='IAMC15_2019_R2')
print(res.entity.unique())

#%% Accessing the data

# tables are accessed by their ID, given by the inventory index returning a Datatable
table = dt.getTable(inventory.index[100])

# or given by the result dataframe
table = dt.getTable(res.index[100])

# multiple tables can be loaded at once and returned in a tableSet (being a dictionary+)
tables = dt.getTables(res.index[:10])

#%% Datatable

print(type(table))

# A Datatable is a pandas Dataframe with additonal restrictions and functionalities
#  - columns are integer years
#  - the index only consists of valied region identifiers
#  - the data is numeric
#  - each table as meta data attached with some required varariables (see config.py)
#  - each tables only consists of one variable with the same unit which 
#    allows for easy unit conversion (see unit_conversion.py as tutorial)
print(table)

# each table is stored as a csv file each inf the dedicated source folder
print(dt.core.DB._getTableFilePath(table.ID))

#%% DataSet

print(type(tables))
# A TableSet is a dictionary with minor additional functionalities

# interface to pyam
iamDataFrame = tables.to_IamDataFrame()

# interface to excel
tables.to_excel('output_test.xlsx')

