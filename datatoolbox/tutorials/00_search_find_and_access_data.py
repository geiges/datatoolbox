#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 18 09:03:50 2021

@author: ageiges
"""

"""

This tutorial covers the datatoolbox inventory and  two search functions 
for datatables

1) Inventory
2) findp "pattern"
3) findc "conains"

"""


import datatoolbox as dt

#%% tutorial data
"""
This tutoral works based on the PRIMAP emission data set. To work with this tutorial,
please import the data.
"""

# if 'PRIMAP'
    dt.import_new_source_from_remote('PRIMAP_2019')
    
    #%% Inventory
    """
    The inventory is a pandas dataframe containing all available tables, each in a row.
    The index has the tableIDs and comes with the following colums : 
    ['variable', 'entity', 'category', 'pathway', 'scenario', 'model', 'source', 'source_name', 'source_year', 'unit']
    
    All coulumns can be used for the later search requests.
    """
    dt.inventory().head()
    
    
    #%% Seach using patterns - findp
    """
    Defauls is a shell style type of search suitable to be used for categories.
    """
    
    dt.findp(source = 'PRIMAP_2019')
    
    # One star (*) is giving all results for only the following level (speparated by "|") 
    dt.findp(variable='Emissions|*',
             source = 'PRIMAP_2019') # "Emissions|KYOTOGHG_AR4|Marine" will be excluded

# Two stars (**) allows for any patter for the complete rest of the string
dt.findp(variable='Emissions|**',
         source = 'PRIMAP_2019') # "Emissions|KYOTOGHG_AR4|Marine" will be included

# One star (*) is giving all results for only the following level (speparated by "|") 
dt.findp(variable='Emissions|*|IPCM0EL',
         source = 'PRIMAP_2019') # "All kinds of IPCM0EL emissions, allows for different gases.

# several patters are Or'd together
dt.findp(variable=['Emissions|CO2|*', 'Emissions|CH4|*'],
         source = 'PRIMAP_2019')

# Example finding GHG emission for certain pathways

dt.findp(source = 'PRIMAP_2019').pathway.unique()

inv = dt.findp(entity='Emissions|KYOTOGHG_AR4', 
               pathway=['Historic|country_reported', 'Historic|third_party'],
               source = 'PRIMAP_2019')

#%% Seach using contains - findc
"""
Findc uses the panfas "contains" logic to filter the inventoy. Any findc search for a "string_pattern
can be reproduces by findp(**string_pattern**)
"""

dt.findc(variable='KYOTO', source = 'PRIMAP_2019')
#  equals
dt.findp(variable='**KYOTO**', source = 'PRIMAP_2019')

# Example finding Historic CO2 emission data sets

res = dt.find(entity='Emissions|CO2',
        scenario = 'Historic')
print(res.source.unique())
print(res.variable.unique())


res = dt.findp(entity='Emissions|CO2',
        scenario = 'Historic**')
print(res.source.unique())
print(res.variable.unique())

#%% conversions to other data structures

# load as pyam dataframe
idf = res.as_pyam()

#load as xarray 
xda = res.as_xarray()

#both allow to use the native region defintions
idf = res.as_pyam(native_regions=True)
xda = res.as_xarray(native_regions=True)

