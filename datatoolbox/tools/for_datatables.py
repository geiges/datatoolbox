#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:46:49 2020


toolbox for datatables

@author: ageiges
"""
from copy import copy
#from . import config


def interpolate(datatable, method="linear"):
    
#    if ~isinstance(datatable, ):
#        raise(BaseException('This function is only guaranteed to work with a datatoolbox datatable'))
        
    datatable = copy(datatable)
    if method == 'linear':
        from scipy import interpolate
        import numpy as np
        xData = datatable.columns.values.astype(float)
        yData = datatable.values
        for row in yData:
            idxNan = np.isnan(row)
            if (sum(~idxNan) < 2):
                continue
            interpolator = interpolate.interp1d(xData[~idxNan], row[~idxNan], kind='linear')
            col_idx = xData[idxNan].astype(int)
            col_idx = col_idx[col_idx > xData[~idxNan].min()]
            col_idx = col_idx[col_idx < xData[~idxNan].max()]
            new_idx = idxNan & (xData > xData[~idxNan].min()) & (xData < xData[~idxNan].max())
            row[new_idx] = interpolator(col_idx)
        return datatable
    else:
        raise(NotImplementedError())
        
        
def aggregate_region(table, mapping):
    
        
    missingCountryDict = dict()
    
    for region in mapping.keys():

        
        missingCountries = set(mapping[region]) - set(table.index)
#                print('missing countries: {}'.format(missingCountries))
        missingCountryDict[region] = list(missingCountries)
        availableCountries = set(mapping[region]).intersection(table.index)
        if len(availableCountries) >0:
            table.loc[region,:] = table.loc[availableCountries,:].sum(axis=0, skipna=True)

    return table, missingCountryDict