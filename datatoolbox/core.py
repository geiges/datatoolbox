##!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Core contains some very basic functions that are used within the package
in various locations and tools.

@author: Andreas Geiges
"""

import time
from datatoolbox import config
import numpy as np
tt = time.time()
import pint
#%% Pint unit handling
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

#%% optional support xarray

if config.AVAILABLE_XARRAY:
    from .tools import xarray as _xr
    to_XDataSet = _xr.to_XDataSet
    to_XDataArray = _xr.to_XDataArray

#%%

c = pint.Context('GWP_AR5')

CO2EQ_LIST = ['CO2eq', 'CO2e',]

AR4GWPDict = {'CH4': 25,
              'HFC': 1430,
              'N2O' : 298,
              'SF6' : 22800,
              'PFC' : 7390,
              'CO2' : 1}


LOG = dict()
LOG['tableIDs'] = list()

ur.add_context(c)

def _update_meta(metaDict):
    """
    Private funcion to update the meta data of a datatable
    

    Parameters
    ----------
    metaDict : dict
        new data to overwrite.

    Returns
    -------
    metaDict : dict

    """
    
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
    """
    Checkes if operating system is windows based
    

    Returns
    -------
    bool

    """
    if (config.OS == 'win32') | (config.OS == "Windows"):
        return True
    else:
        return False

def getUnit(string):
    """
    Return the pint unit for a given unit string. Compared to the original
    pint this functions replaces special characters $ € and % by a string 
    reprentation.

    Parameters
    ----------
    string : str
        unit string (e.g. "km / s" or "€  / capita")

    Returns
    -------
    unit : pint unit
    """
    if string is None:
        string = ''
    else:
        string = string.replace('$', 'USD').replace('€','EUR').replace('%','percent')
    return ur(string)

def getUnitWindows(string):
    """
    Equivalent version of getUnit  but adapted for windows system.
    
    Parameters
    ----------
    string : str
        unit string (e.g. "km / s" or "€  / capita")

    Returns
    -------
    unit : pint unit

    """
    if string is None:
        string = ''
    else:
        string = string.replace('$', 'USD').replace('€','EUR').replace('%','percent').replace('Â','')
    return ur(string)

# re-defintion of getUnit function for windows users
if osIsWindows():
    getUnit = getUnitWindows


def getTimeString():
    """
    Return formated time string.

    Returns
    -------
    time string : str

    """
    return time.strftime("%Y/%m/%d-%I:%M:%S")

def getDateString():
    """
    Return formated date string.

    Returns
    -------
    date string : str

    """
    return time.strftime("%Y/%m/%d")

def conversionFactor(unitFrom, unitTo, context =None):
    """
    Return the conversion factor from one unit to another.

    Parameters
    ----------
    unitFrom : str
        Original unit to convert.
    unitTo : str
        Unit to which it original unit should be converted.
    context : str, optional
        For some conversions, a specifice context is needed. Currently, only
        GWPAR4 is implemented. The default is None.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """ 
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
        
    if config.DEBUG:    
        print('Converting from {} to {} using GWP AR4'.format(gasKey,co2Key))
        
    
    unitFrom = unitFrom.replace(gasKey, co2Key)
    conversFactor = getUnit(unitFrom).to(unitTo).m
    co2eq_factor = AR4GWPDict[gasKey]
    factor = conversFactor * co2eq_factor
    return factor


def get_dimension_extend(table_iterable, dimensions):
    """
    This functions assesses the the unique extend for various dimensions
    given a set of datatables
    """
    fullIdx = dict()
    for dim in dimensions:
        fullIdx[dim] = set()

    for table in table_iterable:
        
#        for metaKey, metaValue in table.meta.items():
#            if metaKey not in metaDict.keys():
#                metaDict[metaKey] = set([metaValue])
#            else:
#                metaDict[metaKey].add(metaValue)
            
        for dim in dimensions:
            if dim == 'region':
                fullIdx[dim] = fullIdx[dim].union(table.index)
            elif dim == 'time':
                fullIdx[dim] = fullIdx[dim].union(table.columns)
            elif dim in table.meta.keys():
                fullIdx[dim].add(table.meta[dim])
            else:
                raise(BaseException('Dimension not available'))

    dimSize = [len(fullIdx[x]) for x in dimensions]
    dimList = [sorted(list(fullIdx[x])) for x in dimensions]
    
    return dimSize, dimList
    

def get_meta_collection(table_iterable, dimensions):
    """

    Parameters
    ----------
    table_iterable : list of tables
        DESCRIPTION.
    dimensions : list of dimentions
        DESCRIPTION.

    Returns
    -------
    metaCollection : TYPE
        DESCRIPTION.

    """

    
    metaCollection = dict()
    for table in table_iterable:
        
        for key in table.meta.keys():
            if key in dimensions  or key == 'ID':
                continue
            if key not in metaCollection.keys():
                metaCollection[key] = set()
                
            metaCollection[key].add(table.meta[key])
    
    return metaCollection
    
if config.DEBUG:
    print('core loaded in {:2.4f} seconds'.format(time.time()-tt))
