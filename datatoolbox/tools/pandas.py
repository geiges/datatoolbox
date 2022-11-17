#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 09:34:50 2020

@author: ageiges
"""
import numpy as np

# from ..util import identifyCountry
from .. import mapping as mapp


def addCountryNames(table, as_index=False):
    names = list()
    for idx in table.index:
        if idx in mapp.countries.codes.index:
            names.append(mapp.countries.codes.loc[idx, 'name'])
        else:
            names.append(idx)
    if as_index:
        table.index = names
    else:
        table.loc[:, 'country_name'] = names
    return table


def convertIndexToISO(table, iso_type='alpha3'):
    from datatoolbox.util import identifyCountry

    """
    Convert index of a dataframe into iso codes.

    Parameters
    ----------
    table : pandas.Dataframe or dt.DataTable
        Index of thos table consists of country strings.
    iso : TYPE, optional
        Either 'alpha3', alpha2 or numISO. The default is 'alpha3'.

    Returns
    -------
    table :  pandas.Dataframe or dt.DataTable
        Return old dataframe with new iso index.

    """
    replaceDict = dict()

    for idx in table.index:
        iso = identifyCountry(idx)
        if iso is not None:
            replaceDict[idx] = iso
    table.index = table.index.map(replaceDict)
    table = table.loc[~table.index.isna(), :]

    if iso_type == 'alpha2':
        table.index = mapp.countries.codes.loc[table.index, 'alpha2']
    elif iso_type == 'numISO':
        table.index = mapp.countries.codes.loc[table.index, 'numISO'].astype(int)
    return table


def add_standard(table, iso_type='alpha3'):
    """
    Convert index of a dataframe into iso codes.

    Parameters
    ----------
    table : pandas.Dataframe or dt.DataTable
        Index of thos table consists of country strings.
    iso : TYPE, optional
        Either 'alpha3', alpha2 or numISO. The default is 'alpha3'.

    Returns
    -------
    table :  pandas.Dataframe or dt.DataTable
        Return old dataframe with new iso index.

    """
    replaceDict = dict()

    for idx in table.index:
        iso = identifyCountry(idx)
        if iso is not None:
            replaceDict[idx] = iso
    table.index = table.index.map(replaceDict)
    table = table.loc[~table.index.isna(), :]

    if iso_type == 'alpha2':
        table.index = mapp.countries.codes.loc[table.index, 'alpha2']
    elif iso_type == 'numISO':
        table.index = mapp.countries.codes.loc[table.index, 'numISO'].astype(int)
    return table


def yearsColumnsOnly(index):
    """
    Extracts from any given index only the index list that can resemble
    as year

    e.g. 2001
    """

    import re

    REG_YEAR = re.compile('^[0-9]{4}$')

    newColumns = []
    for col in index:
        if REG_YEAR.search(str(col)) is not None:
            newColumns.append(col)
        else:
            try:
                if ~np.isnan(col) and REG_YEAR.search(str(int(col))) is not None:
                    #   test float string
                    newColumns.append(col)
            except:
                pass
    return newColumns


def index_availablity(dataframe, index_list):

    available_idx = dataframe.index.intersection(index_list)
    missing_idx = set(index_list).difference(available_idx)

    return available_idx, missing_idx


from operator import and_
from functools import reduce


def isin(df=None, **filters):
    """Constructs a MultiIndex selector

    Usage
    -----
    > df.loc[isin(region="World", gas=["CO2", "N2O"])]
    or with explicit df to get a boolean mask
    > isin(df, region="World", gas=["CO2", "N2O"])
    """

    def tester(df):
        tests = (df.index.isin(np.atleast_1d(v), level=k) for k, v in filters.items())
        return reduce(and_, tests, next(tests))

    return tester if df is None else tester(df)
