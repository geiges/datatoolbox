#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 12:01:06 2020

@author: ageiges
"""
import os
import pandas as pd
import platform
OS = platform.system()
from datatoolbox import config
import git
import shutil
import datatoolbox as dt

def change_personal_config():
    from .tools.install_support import create_personal_setting
    modulePath =  os.path.dirname(__file__) + '/'
    create_personal_setting(modulePath, OS)
    
    
def create_empty_datashelf(pathToDataself):
    from pathlib import Path
    import os
    path = Path(pathToDataself)
    path.mkdir(parents=True,exist_ok=True)
    
    # add subfolders database
    Path(os.path.join(pathToDataself, 'database')).mkdir()
    Path(os.path.join(pathToDataself, 'rawdata')).mkdir()
    
    #create mappings
    os.makedirs(os.path.join(pathToDataself, 'mappings'),exist_ok=True)
    shutil.copyfile(os.path.join(config.MODULE_PATH, 'data/SANDBOX_datashelf/mappings/regions.csv'),
                    os.path.join(pathToDataself, 'mappings/regions.csv'))
    shutil.copyfile(os.path.join(config.MODULE_PATH, 'data/SANDBOX_datashelf/mappings/continent.csv'),
                    os.path.join(pathToDataself, 'mappings/continent.csv'))
    shutil.copyfile(os.path.join(config.MODULE_PATH, 'data/SANDBOX_datashelf/mappings/country_codes.csv'),
                    os.path.join(pathToDataself, 'mappings/country_codes.csv'))    
    
    sourcesDf = pd.DataFrame(columns = config.SOURCE_META_FIELDS)
    filePath= os.path.join(pathToDataself, 'sources.csv')
    sourcesDf.to_csv(filePath)
    
    inventoryDf = pd.DataFrame(columns = config.INVENTORY_FIELDS)
    filePath= os.path.join(pathToDataself, 'inventory.csv')
    inventoryDf.to_csv(filePath)
    git.Repo.init(pathToDataself)

def _re_link_functions(dt):

    
    dt.commitTable = dt.core.DB.commitTable
    dt.commitTables = dt.core.DB.commitTables
    
    dt.updateTable  = dt.core.DB.updateTable
    dt.updateTables  = dt.core.DB.updateTables
    
    dt.removeTable = dt.core.DB.removeTable
    dt.removeTables = dt.core.DB.removeTables
    
    dt.find         = dt.core.DB.getInventory
    dt.findp        = dt.core.DB.findp
    dt.findExact    = dt.core.DB.findExact
    dt.getTable     = dt.core.DB.getTable
    dt.getTables    = dt.core.DB.getTables
    
    dt.isAvailable  = dt.core.DB._tableExists
    
    dt.updateExcelInput = dt.core.DB.updateExcelInput
    
def switch_database(database='default'):
    from datatoolbox.tools.install_support import create_initial_config
    
    _, sandboxPath, READ_ONLY, DEBUG = create_initial_config(config.MODULE_PATH, write_config=False)
    if database == 'sandbox':
        config.PATH_TO_DATASHELF = sandboxPath
        config.SOURCE_FILE = os.path.join(sandboxPath, 'sources.csv')
        dt.core.DB  = dt.database.Database()

        _re_link_functions(dt)