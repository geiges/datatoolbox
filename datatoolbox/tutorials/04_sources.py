#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 19:32:13 2020

@author: Andreas Geiges

SOURCES Tutorial
"""

import datatoolbox as dt
import os

#%% Each source has its own folders  (see datashelf)
path_to_data = dt.conf.PATH_TO_DATASHELF
print(path_to_data)

# containing the raw_data
print(os.listdir(os.path.join(path_to_data, 'rawdata')))


# containing the processed data as csv files (e.g. for the WDI 2018)
for file in (os.listdir(os.path.join(path_to_data, 'database/WDI2018'))):
    print(file)

#%% all sources (should) also be organized in dt.sources

print(dt.sources)

# each containing the raw_data_read_in_class (see rawSources.py)
type(dt.sources.PRIMAP_2019)

primapReader = dt.sources.PRIMAP_2019

# the reader allows you to access the rawdata (windows not implemented, sorry)
primapReader.openRawData()

# the reader allows you to access the maping file  (windows not implemented, sorry)
primapReader.openMappingFile()

# The mapping files is doing all naming conversions as well as unit handling

#%% New data would be read in using the following steps:

# create reader instance e.g. the primap reader
primapReader = dt.sources.PRIMAP_2019

# gather data and review it
tableList, excludedTables = primapReader.gatherMappedData()

# commit tables to the dataself
dt.commitTables(tableList, 'PRIMAP 2019 update', primapReader.meta)

# same works with the other sources