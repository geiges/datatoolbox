#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 10:43:55 2021

@author: ageiges
"""
import numpy as np
import pandas as pd
import xarray as xr
import pyam
from tqdm import tqdm
from . import config
from time import time
import datatoolbox as dt
# %% private functions
def _get_dimension_indices(table, dimensions):
    
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

def _yearsColumnsOnly(index):
    """
    Extracts from any given index only the index list that can resemble 
    as year 
    
    e.g. 2001
    """
    
    import re
    REG_YEAR   = re.compile('^[0-9]{4}$')
    
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

def _get_meta_collection(table_iterable, dimensions):
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

def _get_unique_labels(table, dim):
    
    if isinstance(dim, tuple):
        unique_lables = [tuple(_get_unique_labels(table, sub_dim)[0] for sub_dim in dim)]
        # unique_lables = [tuple(d for sub_dim in dim for d in _get_unique_labels(table, sub_dim))] # todo find better way
    elif dim == table.index.name:
        unique_lables = table.index
    elif dim == table.columns.name:
        unique_lables = table.columns
    elif dim in table.meta.keys():
        unique_lables = [table.meta[dim]]
    else:
        #raise(Exception(f'Dimension {dim} not available'))
        unique_lables = [np.nan]
        
    return unique_lables

def _get_dimensions(table_iterable, dimensions):
    
    
    dims = dict()
    
    for table in table_iterable:
    
        for dim in dimensions:
            
            dims[dim] = dims.get(dim,set()).union(_get_unique_labels(table, dim))
    return dims

def _to_xarray(tables, dimensions, stacked_dims):
    """ 
    Return a database query result as an xarray . This constuctor allows only for
    one unit, since the full array is quantified using pint-xarray. 
    The xarray dimensions (coordiantes) are defined
    by the provided dimensions. A multi-index for a coordinate can be created
    by using stacked_dims.
    
    Usage:
    -------
    tables : Iterable[[dt.Datatable]]
    dimensions :  Iterable[str]]
        Dimensions of the shared yarray dimensions / coordinates
    stacked_dims : Dict[str]]
        Dictionary of all mutli-index coordinates and their sub-dimensions
    
    Returns
    -------
    matches : xarray.Dataset + pint quantification
    """   
    tt = time()
    metaCollection = _get_meta_collection(tables, dimensions)
    if config.DEBUG:
        print(f'ime required for meta collection: {time()-tt:2.2f}s')
    
    
    tt = time()
    final_dims = dimensions.copy()
    xdims = dimensions.copy()
    for st_dim, sub_dims in stacked_dims.items():
        [xdims.remove(dim) for dim in sub_dims]
        xdims.append(sub_dims)
        
        [final_dims.remove(dim) for dim in sub_dims]
        final_dims.append(st_dim)
        
    dims = _get_dimensions(tables, xdims)
    if config.DEBUG:
        print(dims)
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
    
    if config.DEBUG:
        print(f'Get timension: {time()-tt:2.2f}s')
    
    tt = time()
    xData =  xr.DataArray(np.zeros(dimSize)*np.nan, coords=coords, dims=final_dims).assign_coords(labels)
    
    tt= time()
    for table in tables:
        ind = _get_dimension_indices(table,xdims)
        # xData.loc[tuple(ind)] = table.values.reshape(len(table.index),len(table.columns),1)
        
        xData.loc[tuple(ind)] = table.values.reshape(*[len(x) for x in ind])
    if config.DEBUG:
        print(f'Time required for xr data filling: {time()-tt:2.2f}s')    
    tt=time()
    metaCollection['unit'] = list(metaCollection['unit'])[0]
    xData = xData.pint.quantify(metaCollection['unit']).assign_coords(labels)
    xData.attrs = metaCollection
    
    return xData

def _key_set_to_xdataset(dict_of_data,
                        dimensions = ['model', 'scenario', 'region', 'time'],
                        stacked_dims = {'pathway': ('model', 'scenario')}):
    """ 
    Returns xarry dataset converted from a dict of pandas dataframes  or dt.TableSet. Differenty variables
    are stored as key variables. The xarray dimensions (coordiantes) are defined
    by the provided dimensions. A multi-index for a coordinate can be created
    by using stacked_dims.
    
    Usage:
    -------
    dimensions :  Iterable[str]]
        Dimensions of the shared yarray dimensions / coordinates
    stacked_dims : Dict[str]]
        Dictionary of all mutli-index coordinates and their sub-dimensions
    
    Returns
    -------
    matches : xarray.Dataset + pint quantification
    """    
    
    dim_to_sort = 'variable'
    sort_dict = dict()
    for key, table in dict_of_data.items():
        
        var = table.meta['variable']
        
        if var in sort_dict.keys():
            sort_dict[var].append(table)
        else:
            sort_dict[var] = [table]
            
    
    variables = sort_dict.keys()

    data = list()
    for variable in variables:
        tables = sort_dict[variable]
        
        xarray = _to_xarray(tables, dimensions, stacked_dims)
        data.append(xr.Dataset({variable : xarray}))
        
    ds = xr.merge(data) 

    
    return ds

def _pack_dimensions(index, **stacked_dims):
    packed_labels = {}
    packed_values = {}
    drop_levels = []
    
    for dim, levels in stacked_dims.items():
        labels = pd.MultiIndex.from_arrays([index.get_level_values(l) for l in levels])
        packed_labels[dim] = labels_u = labels.unique()
        packed_values[dim] = pd.Index(labels_u.get_indexer(labels), name=dim)
        drop_levels.extend(levels)

    return (
        pd.MultiIndex.from_arrays(
            [index.get_level_values(l) for l in index.names.difference(drop_levels)] +
            list(packed_values.values())
        ),
        packed_labels
    )


def to_dataset(data_dict,
               var_data_labels, # with different units
               packed_dimensions, # dependent dimenions
               ):
    
    
    
    pass

#%%
def _xDataSet_to_wide_dataframe(xds):
    merge_list = list()
    dims = list(xds.coords)
    assert (dt.config.DATATABLE_COLUMN_NAME in dims) and (dt.config.DATATABLE_INDEX_NAME in dims)
    df = xds.to_dataframe()
    
    for var in xds.var():
        #print(var)
        wdf = df[var]
        level = list(df.index.names).index('time')
        wdf = wdf.unstack(level=level)
        wdf['variable'] = var
        wdf['unit']  = xds[var].attrs['unit']
        wdf.set_index('variable', append=True, inplace=True)
        wdf.set_index('unit', append=True, inplace=True)
        merge_list.append(wdf)
    return pd.concat(merge_list)


def to_pyam(data):
    """
    Converts known data tyes to a tableset. Recognized data types are:
        - xarray.Dataset

    Parameters
    ----------
    data : TYPE
        DESCRIPTION.

    Returns
    -------
    pyam.IamDataFrame

    """
    if isinstance(data ,xr.Dataset):
        wdf = _xDataSet_to_wide_dataframe(data)
        return pyam.IamDataFrame(wdf)
    
    else:
        raise(Exception(f'{type(data)} is not implemented'))
        
def to_wide_dataframe(data, index_cols = ['Variable', 'Model', 'Scenario']):
    if isinstance(data, pd.DataFrame):
        wdf = data.set_index(index_cols)
        
    elif isinstance(data ,xr.Dataset):
        wdf = _xDataSet_to_wide_dataframe(data)
        # wdf = wdf.reset_index(level='region')
    
    return wdf


def to_tableset(data, additional_meta = dict()):
    """
    Converts known data tyes to a tableset. Recognized data types are:
        - xarray.Dataset

    Parameters
    ----------
    data : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    
    if isinstance(data, pd.DataFrame):
        years = _yearsColumnsOnly(data)
        meta_cols = data.columns.difference(years)
        data = data.set_index(list(meta_cols))
        data = data.reset_index(level='region')
        ts = dt.TableSet()
        for i,idx in enumerate(data.index.unique()):
            sel = data.loc[idx,:]
            if isinstance(sel, pd.Series):
                sel = pd.DataFrame(sel).T.set_index('region')
            else:
                sel = data.loc[idx,:].set_index('region')
            meta = {x:y for x,y in zip(data.index.names, idx)}
            meta.update(additional_meta)
            try :
                dt.core.getUnit(meta['unit'])
            except:
                print(f'Skipping because of unit {meta["unit"]}') 
                print(meta)
                continue
            table = dt.Datatable(data = sel,
                                 meta = meta)
            ts[i] = table
            
        return ts
    
    
    elif isinstance(data ,xr.Dataset):
        wdf = _xDataSet_to_wide_dataframe(data)
        wdf = wdf.reset_index(level='region')
        ts = dt.TableSet()
        for i,idx in enumerate(wdf.index.unique()):
            sel = wdf.loc[idx,:].set_index('region')
            meta = {x:y for x,y in zip(wdf.index.names, idx)}
            meta.update(additional_meta)
            table = dt.Datatable(data = sel,
                                 meta = meta)
            ts[i] = table
        return ts
    
    elif isinstance(data, pyam.IamDataFrame):
        
        wdf = idf.timeseries().reset_index()
        idx_cols = ['variable','model', 'scenario', 'unit']
        wdf = wdf.set_index(idx_cols)
        tables = dt.Tableset()
        for idx, df in tqdm(wdf.groupby(idx_cols)):
            meta = {key: value for key, value in zip(idx_cols, idx)}
            meta = dt.core._split_variable(meta)
            meta.update(additional_meta)
            try:
                dt.core.ur(meta['unit'])
            except:
                print(f"Skiping table due to unit {meta['unit']}")
                continue
            table = dt.Datatable(df.set_index('region'), meta = meta).clean()
            tables.add(table)
    
    
    else:
        raise(Exception(f'{type(data)} is not implemented'))
       
     
       
def to_xdataset(data, 
                dimensions = ['model', 'scenario', 'region', 'time'],
                stacked_dims = {'pathway': ('model', 'scenario')}):
        
    
     if isinstance(data, pd.DataFrame):
         # wide dataframe
         for dim in dimensions:
             if dim not in data.columns:
                 print(f'Dimension {dim} not found in index names')
             
         #convert to table set
         print('convert to table set')
         data = to_tableset(data)
         
         ds = _key_set_to_xdataset(data)
    
     if isinstance(data, pyam.IamDataFrame):
        
         ds = dt.data_structures.DataSet(data)
    
     return ds
        
 
    #%%
    
    #%%
if __name__ == '__main__':
    
    #%test
    import datatoolbox as dt

    # local_data_file = '/media/sf_Documents/python/ca_data_management/data/IPCC_AR6/AR6_Scenarios_Database_World_v1.0.csv'#,
    # data = pd.read_csv(local_data_file)
    # dimensions = ['Model', 'Scenario', 'Region', 'year']
    # stacked_dims = {'Pathway': ('Model', 'Scenario'),
    #                 'varunit' : ("Variable", "Unit")}
    # data = data.set_index(data.columns[:5].tolist()).rename_axis(columns= 'year').stack()
    
    # from datatoolbox.tools.pyam import idf_to_xarray
    
    # xds = idf_to_xarray(data, stacked_dims= stacked_dims)
    
    # tbs = dt.getTables(dt.find(source='IAMC15_2019_R2').index[:10])
    # idf = tbs.to_IamDataFrame()
    # # xda = idf_to_xarray(idf)
    # stacked_dims= {'pathway': ('model', 'scenario')}
    
    # index, labels = _pack_dimensions(idf.index, **stacked_dims)
    xdata = dt.findp(variable=['Emissions|CO2|Energy|Supply|Electricity',
                              'Secondary Energy|Electricity'],
                    source ='IPCC_SR15', pathway='**SSP1-19**').as_xarray()
    
    ts = to_tableset(xdata)
    idf = to_pyam(xdata)
    
    wdf = to_wide_dataframe(xdata)
    idf2 = dt.findp(variable=['Emissions|CO2|Energy|Supply|Electricity',
                              'Secondary Energy|Electricity'],
                    source ='IPCC_SR15', pathway='**SSP1-19**').as_pyam()
    
    ds = to_xdataset(wdf.reset_index())

#%% 
# res = 