#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DATA TOOLS by Andreas Geiges

Climate Analytics
"""

import os
from . import config as conf
from . import core

from .data_structures import Datatable, read_csv
#from datatools.tools import py_magicc_tools
#from datatools.tools.py_magicc_tools import writeMagic6ScenFile
from . import database
core.DB = database.Database()
from . import mapping as mapp
try:
    from . import database
    core.DB = database.Database()
    from . import mapping as mapp
except Exception as e:
    print(e)
    print('data base not initialized')
from . import io_tools as io

from . import util as util

from . import rawSources as _raw_sources

# Predefined sets for regions and scenrarios
from datatoolbox.sets import REGIONS, SCENARIOS

#from .spatial import Mapping
#if os.path.exists(conf.MAPPING_PATH):
#    core.geoMapping = Mapping(conf.MAPPING_PATH)
#else:
#    core.geoMapping = Mapping()

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

TableSet = util.TableSet
insertDataIntoExcelFile = io.insertDataIntoExcelFile
sources = _raw_sources.sources

getCountryISO = util.getCountryISO

conversionFactor = core.conversionFactor

# extract data for specific countries and sources
countryDataExtract = util.getCountryExtract

#writeMAGICC6ScenFile = tools.wr

from .tools.kaya_idendentiy_decomposition import kaya_decomposion