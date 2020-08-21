#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:46:49 2020


toolbox for datatables

@author: ageiges
"""
from copy import copy


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