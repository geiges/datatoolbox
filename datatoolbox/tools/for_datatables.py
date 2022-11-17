#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:46:49 2020


toolbox for datatables

@author: ageiges
"""
from copy import copy
import numpy as np

# from . import config
import datatoolbox as dt
import pandas as pd


def interpolate(datatable, method="linear"):

    datatable = copy(datatable)

    if method == 'linear':
        from scipy import interpolate
        import numpy as np

        if pd.api.types.is_datetime64_any_dtype(datatable.columns.dtype):
            xData = (
                datatable.columns.values.astype(int) // 10**9
            )  # timestamp in seconds
        elif pd.api.types.is_integer_dtype(
            datatable.columns.dtype
        ):  # assuming year as int
            xData = datatable.columns.values.astype(float)
        else:
            raise (
                BaseException(
                    'Expecting column dtype to be integer for years or datetime'
                )
            )

        yData = datatable.values
        for row in yData:
            idxNan = np.isnan(row)
            if sum(~idxNan) < 2:
                continue
            interpolator = interpolate.interp1d(
                xData[~idxNan], row[~idxNan], kind='linear'
            )
            col_idx = xData[idxNan].astype(int)
            col_idx = col_idx[col_idx > xData[~idxNan].min()]
            col_idx = col_idx[col_idx < xData[~idxNan].max()]
            new_idx = (
                idxNan & (xData > xData[~idxNan].min()) & (xData < xData[~idxNan].max())
            )
            row[new_idx] = interpolator(col_idx)
        return datatable
    else:
        raise (NotImplementedError())


def aggregate_region(table, mapping, skipna=False):

    table = copy(table)
    missingCountryDict = dict()

    for region in mapping.keys():

        missingCountries = set(mapping[region]) - set(table.index)
        #                print('missing countries: {}'.format(missingCountries))
        missingCountryDict[region] = list(missingCountries)
        availableCountries = set(mapping[region]).intersection(table.index)
        if len(availableCountries) > 0:
            table.loc[region, :] = table.loc[list(availableCountries), :].sum(
                axis=0, skipna=skipna
            )

    return table, missingCountryDict


def growth_rate(datatable):
    """
    Computes the growth rates for the given datatable
    """
    #    tempTable = copy(datatable)
    #    years = tempTable.columns
    #    completeYears = list(range(years.min() - periods, years.max()))
    #    for year in set(completeYears).difference(tempTable.columns):
    #        tempTable.loc[:,year] = np.nan
    #    tempTable =tempTable.loc[:,completeYears]
    #    growth_rates = tempTable.diff(axis=1,periods=periods).iloc[:,periods+1:] / tempTable.iloc[:,periods:-1].values
    #    growth_rates = growth_rates.loc[~growth_rates.isnull().all(axis=1),:]
    #    growth_rates = growth_rates.loc[:,~growth_rates.isnull().all(axis=0)]
    #
    #    return growth_rates

    # t0 = tempTable.loc[:,years[1:]]
    # t1 = tempTable.loc[:,[x-period for x in years if x-period in tempTable.columns]]
    tempTable = copy(datatable)
    growth_rates = tempTable.loc[:, tempTable.columns] * np.nan
    for i_year in range(1, len(tempTable.columns)):
        t0 = tempTable.columns[i_year - 1]
        t1 = tempTable.columns[i_year]

        diff = tempTable.loc[:, t1].values - tempTable.loc[:, t0].values
        growth_rate = diff / tempTable.loc[:, t0]
        growth_rates.loc[:, t1] = growth_rate
    return growth_rates


def composite_dataframe(tablePrioryList):

    newColumns = set()
    newIndex = set()
    for table in tablePrioryList:
        newColumns = newColumns.union(table.columns)
        newIndex = newIndex.union(table.index)

    newTable = dt.Datatable(data=None, columns=newColumns, index=newIndex)

    metaCollection = dict()
    for table in tablePrioryList:
        newTable = newTable.combine_first(table)

        for key in table.meta.keys():
            if key not in metaCollection.keys():
                metaCollection[key] = set()

            metaCollection[key] = metaCollection[key].union([table.meta[key]])

    for key in metaCollection.keys():
        if len(metaCollection[key]) == 1:
            newTable.meta[key] = list(metaCollection[key])[0]
        else:
            newTable.meta[key] = 'merged - meta quantity'
    return newTable


#%%
