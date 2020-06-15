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

def change_personal_config():
    from .tools.install_support import create_personal_setting
    modulePath =  os.path.dirname(__file__) + '/'
    create_personal_setting(modulePath, OS)
    
    
def create_empty_datashelf(pathToDataself):
    from pathlib import Path
    path = Path(pathToDataself)
    path.mkdir(parents=True,exist_ok=True)
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
    
    