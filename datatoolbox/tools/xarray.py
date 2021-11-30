#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains all relevant tools regarding the use of xarray


@author: ageiges
"""

# from datatoolbox import core
# import datatoolbox as dt

from .. import core
import xarray as xr
import numpy as np 
import xarray as xr

import pandas as pd

#%%
def get_dimension_indices(table, dimensions):
    
    ind = list()
    for dim in dimensions:
        if isinstance(dim, tuple):
        
            index = [tuple(_get_unique_labels(table, sub_dim)[0] for sub_dim in dim)] # todo find better way
        elif dim == table.index.name:
            index = list(table.index)
        elif dim == table.columns.name:
            index = list(table.columns)
        elif dim in table.attrs.keys():
            index = [table.attrs[dim]]
        ind.append(index)
        
    return ind
    
def _get_unique_labels(table, dim):
    
    if isinstance(dim, tuple):
        unique_lables = [tuple(_get_unique_labels(table, sub_dim)[0] for sub_dim in dim)]
        # unique_lables = [tuple(d for sub_dim in dim for d in _get_unique_labels(table, sub_dim))] # todo find better way
    elif dim == table.index.name:
        unique_lables = table.index
    elif dim == table.columns.name:
        unique_lables = table.columns
    elif dim in table.attrs.keys():
        unique_lables = [table.attrs[dim]]
    else:
        #raise(Exception(f'Dimension {dim} not available'))
        unique_lables = [np.nan]
        
    return unique_lables

    
def get_dimensions(table_iterable, dimensions):
    
    
    dims = dict()
    
    for table in table_iterable:
    
        for dim in dimensions:
            
            dims[dim] = dims.get(dim,set()).union(_get_unique_labels(table, dim))
    return dims



    
def to_XDataSet(tableSet, dimensions=['region', 'time']):
    """
    Convert datatoolbox tableSet to a xarray data set

    Parameters
    ----------
    tableSet : datatoolbox.Tableset
        DESCRIPTION.
    dimensions : list of str
        Full dimensions of the xarray array. Other remaining dimensions will be
        added as dict like elements.

    Returns
    -------
    dSet : xarray.Dataset
        DESCRIPTION.

    """
    dimSize, dimList = core.get_dimension_extend(tableSet, dimensions= dimensions)
    
    # dimensions= ['region', 'time']
    
    dSet = xr.Dataset(coords = {key: val for (key, val) in zip(dimensions, dimList)})
    
    for key, table in tableSet.items():
        dSet[key] = table
        dSet[key].attrs = table.meta
        
    return dSet
    
def to_XDataArray(tableSet, dimensions = ['region', 'time', 'pathway']):
    #%%
#    dimensions = ['region', 'time', 'scenario', 'model']
    
#    metaDict = dict()
    
    dimSize, dimList = core.get_dimension_extend(tableSet, dimensions)
    metaCollection = core.get_meta_collection(tableSet, dimensions)
     
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

from time import time

def _to_xarray(tables, dimensions, stacked_dims):
    #%%
    # from datatoolbox.core import get_dimensions, get_dimension_indices
    tt = time()
    metaCollection = core.get_meta_collection(tables, dimensions)
    print(f'Meta collection: {time()-tt:2.2f}s')
    
    
    tt = time()
    final_dims = dimensions.copy()
    xdims = dimensions.copy()
    for st_dim, sub_dims in stacked_dims.items():
        [xdims.remove(dim) for dim in sub_dims]
        xdims.append(sub_dims)
        
        [final_dims.remove(dim) for dim in sub_dims]
        final_dims.append(st_dim)
        
    dims = get_dimensions(tables, xdims)
    coords = {x: sorted(list(dims[x])) for x in dims.keys()}
    labels = dict()
    for st_dim, sub_dims in stacked_dims.items():
        coords[st_dim] = range(len(coords[sub_dims]))
        sub_labels = list()
        for i_dim, sub_dim in enumerate(sub_dims):
            
            sub_labels.append(pd.Index([x[i_dim] for x in coords[sub_dims]], name=sub_dim))
        labels[st_dim]  = pd.MultiIndex.from_arrays(sub_labels, names = sub_dims)
        del coords[sub_dims]
    
    dimSize = [len(labels) for dim, labels in dims.items()]
    
    print(f'Get timension: {time()-tt:2.2f}s')
    
    tt = time()
    xData =  xr.DataArray(np.zeros(dimSize)*np.nan, coords=coords, dims=final_dims).assign_coords(labels)
    print(f'Empty xarry: {time()-tt:2.2f}s')
    tt= time()
    for table in tables:
        ind = get_dimension_indices(table,xdims)
        # xData.loc[tuple(ind)] = table.values.reshape(len(table.index),len(table.columns),1)
        
        xData.loc[tuple(ind)] = table.values.reshape(*[len(x) for x in ind])
    print(f'Data filling: {time()-tt:2.2f}s')    
    tt=time()
    metaCollection['unit'] = list(metaCollection['unit'])[0]
    xData = xData.pint.quantify(metaCollection['unit']).assign_coords(labels)
    xData.attrs = metaCollection
    print(f'quantify: {time()-tt:2.2f}s')
    #%%
    return xData


def load_as_xdataset(query,
                     dimensions = ['model', 'scenario', 'region', 'time'],
                  stacked_dims = {'pathway': ('model', 'scenario')}):
    variables = query.variable.unique()

    data = list()
    for variable in variables:
        tables = [core.DB.getTable(x) for x in query.filterp(variable = variable).index]
        
        xarray = _to_xarray(tables, dimensions, stacked_dims)
        data.append(xr.Dataset({variable : xarray}))
        
    ds = xr.merge(data) 

    return ds

if __name__ == '__main__':
    import datatoolbox as dt
    tt = time()
    # tbs = dt.getTables(dt.find(entity = 'Emissions|CO2', source='IAMC15_2019_R2').index[:])
    
    tbs = [dt.getTable(x) for x in dt.find(entity = 'Emissions|CO2', source='IAMC15_2019_R2').index]
    print(f'Load data: {time()-tt:2.2f}s')
    dimensions   = ['model', 'scenario','median warming at peak (MAGICC6)', 'region', 'time']
    stacked_dims = {'pathway': ('model', 'scenario', 'median warming at peak (MAGICC6)')}
    xData = _to_xarray(tbs, dimensions, stacked_dims)
