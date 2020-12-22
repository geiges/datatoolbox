#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains all relevant tools regarding the use of xarray


@author: ageiges
"""
import numpy as np 
import xarray as xr

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

    
    metaCollection = dict()
    for table in table_iterable:
        
        for key in table.meta.keys():
            if key in dimensions  or key == 'ID':
                continue
            if key not in metaCollection.keys():
                metaCollection[key] = set()
                
            metaCollection[key].add(table.meta[key])
    
    return metaCollection


    
def to_XDataSet(tableSet, dimensions):
    
    dimSize, dimList = get_dimension_extend(tableSet, dimensions= ['region', 'time'])
    
    dimensions= ['region', 'time']
    
    dSet = xr.Dataset(coords = {key: val for (key, val) in zip(dimensions, dimList)})
    
    for key, table in tableSet.items():
        dSet[key] = table
        dSet[key].attrs = table.meta
        
    return dSet
    
def to_XDataArray(tableSet, dimensions = ['region', 'time', 'pathway']):
    #%%
#    dimensions = ['region', 'time', 'scenario', 'model']
    
#    metaDict = dict()
    
    dimSize, dimList = get_dimension_extend(tableSet, dimensions)
    metaCollection = get_meta_collection(tableSet, dimensions)
     
    xData =  xr.DataArray(np.zeros(dimSize)*np.nan, coords=dimList, dims=dimensions)
    
    for table in tableSet:
        
        indexTuple = list()
        for dim in dimensions:
            if dim == 'region':
                indexTuple.append(list(table.index))
            elif dim == 'time':
                indexTuple.append(list(table.columns))
            else:
                indexTuple.append(table.meta[dim])
                
#        xx = (table.index,table.columns,table.meta['pathway'])
        xData.loc[tuple(indexTuple)] = table.values
        
        
    # only implemented for homgeneous physical units
    assert len(metaCollection['unit']) == 1
    xData.attrs['unit'] = list(metaCollection['unit'])[0]   
    
    for  metaKey, metaValue in metaCollection.items():
        if len(metaValue) == 1:
            xData.attrs[metaKey] = metaValue.pop()
        else:
            print('Warning, dropping meta data: ' + metaKey +  ' ' + str(metaValue))
    
    return xData