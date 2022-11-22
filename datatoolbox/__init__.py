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

from .version import version as __version__

import os
from . import config
from . import core

try:
    from . import database

    core.DB = database.Database()
    db_connected = True
except:
    import traceback

    print('Database connection broken. Running without database connection.')
    traceback.print_exc()
    db_connected = False

from . import mapping as mapp

from . import interfaces
from . import util as util
from . import admin as admin
from . import templates
from . import converters




#%% DATA STRUCTURES
from .data_structures import (
    Datatable, 
    TableSet, 
    DataSet
    )


#%% IO 
from .data_structures import read_csv, read_excel
from . import data_readers
from . import io_tools as io

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
    push_source_to_remote = core.DB.gitManager.push_to_remote_datashelf
    pull_source_from_remote = core.DB.pull_update_from_remote

    #show available remote data sources
    available_remote_data = core.DB.remote_sourceInfo
    
#%% TOOLS
# Tools related to packages
import datatoolbox.tools as tools
from .tools import pandas as pd
from .tools import matplotlib as plt
from .tools import xarray as xr
from .tools import excel as xl
from .tools import pyam as pyam

insertDataIntoExcelFile = io.insertDataIntoExcelFile


#%% UNITS
from . import units


# get country ISO code
getCountryISO = util.getCountryISO




# convenience functions
get_time_string = core.getTimeString
get_date_string = core.getDateString


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
