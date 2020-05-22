#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
----------- DATA TOOL BOX -------------
This is a python tool box project for handling global datasets. It contains the following features:

    Augumented pandas DataFrames adding meta data,
    Automatic unit conversion and table based computations
    ID based data structure

Authors: Andreas Geiges
         Gaurav Ganti
         
@Climate Analytics gGmbH
"""

__version__ = "0.2.12"

import os
import time
from . import config
tt = time.time()
from . import core
print('{} in {:2.4f} seconds'.format('core',time.time()-tt))
tt = time.time()

tt = time.time()
from .data_structures import Datatable, TableSet, read_csv
print('{} in {:2.4f} seconds'.format('data structure',time.time()-tt))
tt = time.time()
from . import database
core.DB = database.Database()
print('{} in {:2.4f} seconds'.format('core database',time.time()-tt))
tt = time.time()
from . import mapping as mapp
print('{} in {:2.4f} seconds'.format('mapping',time.time()-tt))
tt = time.time()
from . import io_tools as io
print('{} in {:2.4f} seconds'.format('IO',time.time()-tt))
tt = time.time()
from . import interfaces
print('{} in {:2.4f} seconds'.format('Interfaces',time.time()-tt))
tt = time.time()
from . import util as util
print('{} in {:2.4f} seconds'.format('utils',time.time()-tt))
tt = time.time()
from . import admin as admin
from . import rawSources as _raw_sources
print('{} in {:2.4f} seconds'.format('raw sources',time.time()-tt))
tt = time.time()

# Predefined sets for regions and scenrarios
from datatoolbox.sets import REGIONS, SCENARIOS
print('{} in {:2.4f} seconds'.format('sets',time.time()-tt))
tt = time.time()
# unit conversion package
unitReg = core.ur

ExcelReader = io.ExcelReader # TODO make this general IO tools

commitTable  = core.DB.commitTable
commitTables = core.DB.commitTables

updateTable  = core.DB.updateTable
updateTables  = core.DB.updateTables

find         = core.DB.getInventory
findExact    = core.DB.findExact
getTable     = core.DB.getTable
getTables    = core.DB.getTables

isAvailable  = core.DB._tableExists

updateExcelInput = core.DB.updateExcelInput

insertDataIntoExcelFile = io.insertDataIntoExcelFile
sources = _raw_sources.sources

getCountryISO = util.getCountryISO

conversionFactor = core.conversionFactor

# extract data for specific countries and sources
countryDataExtract = util.getCountryExtract

executeForAll = util.forAll

#writeMAGICC6ScenFile = tools.wr

from .tools.kaya_idendentiy_decomposition import kaya_decomposion


if config.PATH_TO_DATASHELF == os.path.join(config.MODULE_PATH, 'data/SANDBOX_datashelf'):
    print("""
          ################################################################
          You are using datatoolbox with a testing database as a SANDBOX.
          This allows for testing and initial tutorial use.
          
          For switching to a excisting database use: 
              "datatoolbox.admin.change_personal_config()"
          ################################################################
          """)