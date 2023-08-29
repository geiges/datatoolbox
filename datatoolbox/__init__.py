#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
----------- DATA TOOL BOX -------------
This is a python tool box project for handling global datasets. 
It contains the following features:

    Augumented pandas DataFrames adding meta data,
    Automatic unit conversion and table based computations
    ID based data structure
    Code templates (see templates.py)
    Package specific helper functions (see: tools/)

Authors: Andreas Geiges
         Jonas Hörsch     
         Gaurav Ganti
         Matthew Giddens
         
"""
import time
all_tt = time.time()
from .version import version as __version__

import os
from . import config
from . import core


try:
    tt = time.time()
    from . import database
    if config.DEBUG:
          print("Database import in {:2.4f} seconds".format(time.time() - tt))

    core.DB = database.Database()
    db_connected = True
except:
    import traceback

    print('Database connection broken. Running without database connection.')
    traceback.print_exc()
    db_connected = False
tt = time.time()
from . import mapping as mapp
if config.DEBUG:
      print("Mapping loaded in {:2.4f} seconds".format(time.time() - tt))

tt = time.time()
from . import interfaces
if config.DEBUG:
      print("Interfaces loaded in {:2.4f} seconds".format(time.time() - tt))

tt = time.time()
from . import util as util
if config.DEBUG:
      print("Utils loaded in {:2.4f} seconds".format(time.time() - tt))

from . import admin as admin
from . import templates
tt = time.time()
from . import converters
if config.DEBUG:
      print("Converters loaded in {:2.4f} seconds".format(time.time() - tt))



tt3 = time.time()
#%% DATA STRUCTURES
from .data_structures import (
    Datatable, 
    TableSet, 
    DataSet
    )

if config.DEBUG:
      print("Data structures loaded in {:2.4f} seconds".format(time.time() - tt3))


#%% IO 
tt = time.time()
from .data_structures import read_csv, read_excel
if config.DEBUG:
      print("Read csv and escel loded in {:2.4f} seconds".format(time.time() - tt))
tt = time.time()
from . import data_readers
if config.DEBUG:
      print("data_readers loaded in {:2.4f} seconds".format(time.time() - tt))
tt = time.time()
from . import io_tools as io

if config.DEBUG:
      print("IO loaded in {:2.4f} seconds".format(time.time() - tt))

#%% SETS
# Predefined sets for regions and scenrarios
from datatoolbox.sets import REGIONS, SCENARIOS

#%% DATABASE 
if db_connected:
    db = core.DB
    commitTable = core.DB.commitTable
    commitTables = core.DB.commitTables

    updateTable = core.DB.updateTable
    updateTables = core.DB.updateTables
    updateTablesAvailable = core.DB.updateTablesAvailable

    removeTable = core.DB.removeTable
    removeTables = core.DB.removeTables

    findc = core.DB.findc
    findp = core.DB.findp
    finde = core.DB.finde
    getTable = core.DB.getTable
    getTables = core.DB.getTables
    getTablesAvailable = core.DB.getTablesAvailable

    isAvailable = core.DB._tableExists

    updateExcelInput = core.DB.updateExcelInput

    sourceInfo = core.DB.sourceInfo
    inventory = core.DB.returnInventory

    validate_ID = core.DB.validate_ID
    # writeMAGICC6ScenFile = tools.wr

    # Source management
    import_new_source_from_remote = core.DB.importSourceFromRemote
    export_new_source_to_remote = core.DB.exportSourceToRemote
    remove_source = core.DB.removeSource
    push_source_to_remote = core.DB.push_source_to_remote
    pull_source_from_remote = core.DB.pull_update_from_remote

    #show available remote data sources
    remote_sourceInfo = core.DB.remote_sourceInfo
    available_remote_data_updates = core.DB.available_remote_data_updates
    test_ssh_remote_connection = core.DB.test_ssh_remote_connection
#%% TOOLS
# Tools related to packages
if config.DEBUG:
    tt = time.time()
import datatoolbox.tools as tools
from .tools import pandas as pd
from .tools import matplotlib as plt
from .tools import xarray as xr
from .tools import excel as xl
from .tools import pyam as pyam
if config.DEBUG:
     print('Tools initialised in {:2.4f} seconds'.format(time.time() - tt))

insertDataIntoExcelFile = io.insertDataIntoExcelFile


#%% UNITS
from . import units
conversionFactor = units.conversionFactor

# get country ISO code
getCountryISO = util.getCountryISO




# convenience functions
get_time_string = core.get_time_string
get_date_string = core.get_date_string


if db_connected:
    if config.PATH_TO_DATASHELF == os.path.join(
        config.MODULE_PATH, 'data/SANDBOX_datashelf'
    ):
        print(
            """
              ################################################################
              You are using datatoolbox with a testing database as a SANDBOX.
              This allows for testing and initial tutorial use.
              
    
              For creating an empty dataase please use:
                  "datatoolbox.admin.create_empty_datashelf(pathToDatabase)"
    
              For switching to a existing database use: 
                  "datatoolbox.admin.change_personal_config()"
                  
                  
              ################################################################
              """
        )
else:
    print(
        """
          ################################################################
          
          You are using datatoolbox with no database connected
          
          Access functions and methods to database are not available.
              
          ################################################################
          """
    )


if config.DEBUG:
      print("Full datatoolbox init took {:2.4f} seconds".format(time.time() - all_tt))
