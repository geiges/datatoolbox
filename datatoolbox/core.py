##!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 14:15:56 2019

@author: and
"""

import time
from . import config
import numpy as np
tt = time.time()
import pint
#%% unit handling
gases = {"CO2eq":"carbon_dioxide_equivalent",
         "CO2e" : "CO2eq",
         "NO2" : "NO2"}

from openscm_units import unit_registry as ur


try:
    ur._add_gases(gases)
    ur.define('fraction = [] = frac')
    ur.define('percent = 1e-2 frac = pct')
    ur.define('ppm = 1e-6 fraction')
    ur.define('sqkm = km * km')
    ur.define('none = dimensionless')


    ur.load_definitions(config.PATH_PINT_DEFINITIONS)
except pint.errors.DefinitionSyntaxError:
    # avoid double import of units defintions
    pass
    
import pint

#%% optional support
try:
    import xarray as xr
    config.AVAILABLE_XARRAY = True
except:
    config.AVAILABLE_XARRAY = False   
#%%

c = pint.Context('GWP_AR5')

CO2EQ_LIST = ['CO2eq', 'CO2e',]

AR4GWPDict = {'F-Gases': 1,
              'Sulfur' : 1,
              'CH4': 25,
              'HFC': 1430,
              'N2O' : 298,
              'NH3' : 1,
              'NOx' : 1,
              'VOC' : 1 ,
              'SF6' : 22800,
              'PFC' : 7390,
              'CO2' : 1,
              'BC'  :1,
              'CO' : 1,
              'OC' : 1,
              'SO2' : 1}


LOG = dict()
LOG['tableIDs'] = list()

ur.add_context(c)

def _update_meta(metaDict):
    
    for key in list(metaDict.keys()):
        if (metaDict[key] is np.nan) or metaDict[key] == '':
            del metaDict[key]
            
    for id_field in config.ID_FIELDS:
        fieldList = [ metaDict[key] for key in config.SUB_FIELDS[id_field] if key in  metaDict.keys()]
        if len(fieldList)>0:
            metaDict[id_field] =  config.SUB_SEP[id_field].join(fieldList).strip('|')
    
    return metaDict


def _createDatabaseID(metaDict):
    
    return config.ID_SEPARATOR.join([metaDict[key] for key in config.ID_FIELDS])


def osIsWindows():
    if (config.OS == 'win32') | (config.OS == "Windows"):
        return True
    else:
        return False

def getUnit(string):
    if string is None:
        string = ''
    else:
        string = string.replace('$', 'USD').replace('€','EUR').replace('%','percent')
    return ur(string)

def getUnitWindows(string):
    if string is None:
        string = ''
    else:
        string = string.replace('$', 'USD').replace('€','EUR').replace('%','percent').replace('Â','')
    return ur(string)

# re-defintion of getUnit function for windows users
if osIsWindows():
    getUnit = getUnitWindows


def getTimeString():
    return time.strftime("%Y/%m/%d-%I:%M:%S")

def getDateString():
    return time.strftime("%Y/%m/%d")

def conversionFactor(unitFrom, unitTo, context =None):
    
    if context is None:
        return getUnit(unitFrom).to(getUnit(unitTo)).m
    elif context == 'GWPAR4':
        
        return _AR4_conversionFactor(unitFrom, unitTo)
        
    else:
        raise(BaseException('unkown context'))


def _findGases(string, candidateList):
    hits = list()
    for key in candidateList:
        if key in string:
            hits.append(key)
            string = string.replace(key,'')
    return hits

def _AR4_conversionFactor(unitFrom, unitTo):
#    weirdSet = set(['CO2','CO','VOC', 'OC'])
    
    # look if unitTo is CO2eq -> conversion into co2 equivalent
    co2eqkeys = _findGases(unitTo, CO2EQ_LIST)        
    gasesToconvert = _findGases(unitFrom, list(AR4GWPDict))
    
    assert len(co2eqkeys) == 1 and len(gasesToconvert) == 1
    co2Key = co2eqkeys[0]
    gasKey = gasesToconvert[0]
    print(co2Key)
    print(gasKey)
    
    unitFrom = unitFrom.replace(gasKey, co2Key)
    conversFactor = getUnit(unitFrom).to(unitTo).m
    co2eq_factor = AR4GWPDict[gasKey]
    factor = conversFactor * co2eq_factor
    return factor
    
if config.DEBUG:
    print('core loaded in {:2.4f} seconds'.format(time.time()-tt))
