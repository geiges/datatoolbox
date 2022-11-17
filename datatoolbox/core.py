##!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Core contains some very basic functions that are used within the package
in various locations and tools.

@author: Andreas Geiges
"""

import time
import pint

import re

import numpy as np
import pandas as pd
from datatoolbox import config
from datatoolbox import naming_convention

tt = time.time()

#%% Pint unit handling
gases = {
    "CO2eq": "carbon_dioxide_equivalent",
    "CO2e": "CO2eq",
    "NO2": "NO2",
    'PM25': 'PM25',
}

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

#
#     from .tools import xarray as _xr
#     to_XDataSet = _xr.to_XDataSet
#     to_XDataArray = _xr.to_XDataArray
if config.AVAILABLE_XARRAY:
    import pint_xarray

    pint_xarray.accessors.setup_registry(ur)
    pint_xarray.unit_registry = ur
    pint_xarray.accessors.default_registry = ur

#%%

c = pint.Context('GWP_AR5')

CO2EQ_LIST = [
    'CO2eq',
    'CO2e',
]

AR4GWPDict = {'CH4': 25, 'HFC': 1430, 'N2O': 298, 'SF6': 22800, 'PFC': 7390, 'CO2': 1}


LOG = dict()
LOG['tableIDs'] = list()

ur.add_context(c)


def is_known_entity(variable):
    entity_matches = list()
    for entity in naming_convention.entities:
        if variable.startswith(entity):
            return True

    return False


def _split_variable(metaDict):
    """
    Split variable into a known entity (see naming_converntion.py) and a
    category.

    Parameters
    ----------
    metaDict : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    # Find entity
    entity_matches = list()
    for entity in naming_convention.entities:
        if metaDict['variable'].startswith(entity):
            entity_matches.append(entity)

    if len(entity_matches) > 0:
        longest_matchg = max(entity_matches, key=len)
        metaDict['entity'] = longest_matchg

    else:
        if config.DEBUG:
            print(f'Warning: Entity could not be derived from {metaDict["variable"]}')

        # exit here
        return metaDict

    # derive or check category
    if 'category' not in metaDict.keys():
        metaDict['category'] = (
            metaDict['variable'].replace(longest_matchg, '').strip('|')
        )
    else:
        if metaDict['category'] != metaDict['variable'].replace(
            longest_matchg, ''
        ).strip('|'):
            print(
                'Warming current category not fitting derived category, please review'
            )
    return metaDict


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
    if 'entity' not in metaDict.keys():

        metaDict = _split_variable(metaDict)

    for key in list(metaDict.keys()):
        if (metaDict[key] is np.nan) or metaDict[key] == '':
            if key != 'unit':
                del metaDict[key]

    for id_field in config.ID_FIELDS:
        fieldList = [
            metaDict[key]
            for key in config.SUB_FIELDS[id_field]
            if key in metaDict.keys()
        ]
        if len(fieldList) > 0:
            new_value = (
                config.SUB_SEP[id_field].join([str(x) for x in fieldList]).strip('|')
            )
            if (
                config.DEBUG
                and id_field in metaDict.keys()
                and metaDict[id_field] != new_value
            ):
                print(
                    f'Warning: {id_field} will be overritten {metaDict[id_field]} -> {new_value}'
                )
            metaDict[id_field] = new_value

    return metaDict


def _fix_filename(name, max_length=255):
    """
    Replace invalid characters on Linux/Windows/MacOS with underscores.
    List from https://stackoverflow.com/a/31976060/819417
    Trailing spaces & periods are ignored on Windows.
    >>> fix_filename("  COM1  ")
    '_ COM1 _'
    >>> fix_filename("COM10")
    'COM10'
    >>> fix_filename("COM1,")
    'COM1,'
    >>> fix_filename("COM1.txt")
    '_.txt'
    >>> all('_' == fix_filename(chr(i)) for i in list(range(32)))
    True
    """
    return re.sub(
        r'[/\\:<>"?*\0-\x1f]|^(AUX|COM[1-9]|CON|LPT[1-9]|NUL|PRN)(?![^.])|^\s|[\s.]$',
        "_",
        name[:max_length],
        flags=re.IGNORECASE,
    )


def _validate_unit(table):

    try:
        getUnit(table.meta['unit'])

        return True
    except:
        return False


def generate_table_file_name(ID):
    ID_for_filename = _fix_filename(ID)
    ID_for_filename = ID.replace('|', '-').replace('/', '-')
    return ID_for_filename + '.csv'


def _createDatabaseID(metaDict):
    ID = config.ID_SEPARATOR.join([metaDict[key] for key in config.ID_FIELDS])
    # ID = _fix_filename(ID)
    return ID


def csv_writer(filename, dataframe, meta, index=0):
    fid = open(filename, 'w', encoding='utf-8')
    fid.write(config.META_DECLARATION)

    for key, value in sorted(meta.items()):
        #            if key == 'unit':
        #                value = str(value.u)
        fid.write(key + ',' + str(value) + '\n')

    fid.write(config.DATA_DECLARATION)
    if index == 0:
        dataframe.to_csv(fid, sep=',')
    elif index is None:
        dataframe.to_csv(fid, index=None, sep=';')
    fid.close()


def excel_writer(
    writer, dataframe, meta, sheet_name="Sheet1", index=False, engine=None
):

    if isinstance(writer, pd.ExcelWriter):
        need_save = False
    else:
        writer = pd.ExcelWriter(pd.io.common.stringify_path(writer), engine=engine)
        need_save = True

    metaSeries = pd.Series(
        data=[''] + list(meta.values()) + [''],
        index=['###META###'] + list(meta.keys()) + ['###DATA###'],
    )

    metaSeries.to_excel(writer, sheet_name=sheet_name, header=None, columns=None)
    pd.DataFrame(dataframe).to_excel(
        writer, sheet_name=sheet_name, index=index, startrow=len(metaSeries)
    )

    if need_save:
        writer.save()


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
    # if not isinstance(string, str):
    #     string = str(string)
    if string is None:
        string = ''
    else:
        string = string.replace('$', 'USD').replace('€', 'EUR').replace('%', 'percent')
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
        string = (
            string.replace('$', 'USD')
            .replace('€', 'EUR')
            .replace('%', 'percent')
            .replace('Â', '')
        )
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
    return time.strftime("%Y_%m_%d")


def conversionFactor(unitFrom, unitTo, context=None):
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
        raise (BaseException('unkown context'))


def _findGases(string, candidateList):
    hits = list()
    for key in candidateList:
        if key in string:
            hits.append(key)
            string = string.replace(key, '')
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
        print('Converting from {} to {} using GWP AR4'.format(gasKey, co2Key))

    unitFrom = unitFrom.replace(gasKey, co2Key)
    conversFactor = getUnit(unitFrom).to(unitTo).m
    co2eq_factor = AR4GWPDict[gasKey]
    factor = conversFactor * co2eq_factor
    return factor


#%%
def get_dimension_extend(table_iterable, dimensions):
    """
    This functions assesses the the unique extend for various dimensions
    given a set of datatables
    """
    fullIdx = dict()
    # for dim in dimensions:
    #     fullIdx[dim] = set()

    for table in table_iterable:

        #        for metaKey, metaValue in table.meta.items():
        #            if metaKey not in metaDict.keys():
        #                metaDict[metaKey] = set([metaValue])
        #            else:
        #                metaDict[metaKey].add(metaValue)

        for dim in dimensions:

            if dim not in fullIdx.keys():
                fullIdx[dim] = set()

            if dim == 'region':
                fullIdx[dim] = fullIdx[dim].union(table.index)
            elif dim == 'time':
                fullIdx[dim] = fullIdx[dim].union(table.columns)
            elif dim in table.meta.keys():
                fullIdx[dim].add(table.meta[dim])
            else:
                raise (BaseException('Dimension not available'))

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
            if key in dimensions or key == 'ID':
                continue
            if key not in metaCollection.keys():
                metaCollection[key] = set()

            metaCollection[key].add(table.meta[key])

    return metaCollection


if config.DEBUG:
    print('core loaded in {:2.4f} seconds'.format(time.time() - tt))
