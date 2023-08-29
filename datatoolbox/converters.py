#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 10:43:55 2021

@author: ageiges
"""
import numpy as np
import pandas as pd
import xarray as xr
#import pyam
from tqdm import tqdm
from datatoolbox import config
from time import time
import datatoolbox as dt

# %% private functions
def _get_dimension_indices(table, dimensions):

    ind = list()
    for dim in dimensions:
        if isinstance(dim, tuple):

            index = [
                tuple(_get_unique_labels(table, sub_dim)[0] for sub_dim in dim)
            ]  # todo find better way
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
            if key in dimensions or key == 'ID':
                continue
            if key not in metaCollection.keys():
                metaCollection[key] = set()

            metaCollection[key].add(table.meta[key])

    return metaCollection


def _get_unique_labels(table, dim):

    if isinstance(dim, tuple):
        unique_lables = [
            tuple(_get_unique_labels(table, sub_dim)[0] for sub_dim in dim)
        ]
        # unique_lables = [tuple(d for sub_dim in dim for d in _get_unique_labels(table, sub_dim))] # todo find better way
    elif dim == table.index.name:
        unique_lables = table.index
    elif dim == table.columns.name:
        unique_lables = table.columns
    elif dim in table.meta.keys():
        unique_lables = [table.meta[dim]]
    else:
        # raise(Exception(f'Dimension {dim} not available'))
        unique_lables = [np.nan]

    return unique_lables


def _get_dimensions(table_iterable, dimensions):

    dims = dict()

    for table in table_iterable:

        for dim in dimensions:

            dims[dim] = dims.get(dim, set()).union(_get_unique_labels(table, dim))
    return dims

def _get_stacked_levels(index, st_dim, sub_dims):
    df = index.to_frame()
    # for st_dim, sub_dims in stacked_dims.items():
    df[st_dim] = df.loc[:, sub_dims].agg('__'.join, axis=1)
    df = df.set_index([st_dim] + list(sub_dims))
    return df.index.codes[0], df.index.levels[0]


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

            sub_labels.append(
                pd.Index([x[i_dim] for x in coords[sub_dims]], name=sub_dim)
            )
        labels[st_dim] = pd.MultiIndex.from_arrays(sub_labels, names=sub_dims)
        del coords[sub_dims]

    dimSize = [len(labels) for dim, labels in dims.items()]

    if config.DEBUG:
        print(f'Get timension: {time()-tt:2.2f}s')

    tt = time()
    xData = xr.DataArray(
        np.zeros(dimSize) * np.nan, coords=coords, dims=final_dims
    ).assign_coords(labels)

    tt = time()
    for table in tables:
        ind = _get_dimension_indices(table, xdims)
        # xData.loc[tuple(ind)] = table.values.reshape(len(table.index),len(table.columns),1)

        xData.loc[tuple(ind)] = table.values.reshape(*[len(x) for x in ind])
    if config.DEBUG:
        print(f'Time required for xr data filling: {time()-tt:2.2f}s')
    tt = time()
    metaCollection['unit'] = list(metaCollection['unit'])[0]
    xData = xData.pint.quantify(metaCollection['unit']).assign_coords(labels)
    xData.attrs = metaCollection

    return xData


def _key_set_to_xdataset(
    dict_of_data,
    dimensions=['model', 'scenario', 'region', 'time'],
    stacked_dims={'pathway': ('model', 'scenario')},
):
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
        data.append(xr.Dataset({variable: xarray}))

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
            [index.get_level_values(l) for l in index.names.difference(drop_levels)]
            + list(packed_values.values())
        ),
        packed_labels,
    )

def _xDataSet_to_wide_dataframe(xds):
    merge_list = list()
    dims = list(xds.coords)
    assert (dt.config.DATATABLE_COLUMN_NAME in dims) and (
        dt.config.DATATABLE_INDEX_NAME in dims
    )
    df = xds.to_dataframe()

    for var in xds.var():
        # print(var)
        wdf = df[var]
        level = list(df.index.names).index('time')
        wdf = wdf.unstack(level=level)
        wdf['variable'] = var
        wdf['unit'] = xds[var].attrs['unit']
        wdf.set_index('variable', append=True, inplace=True)
        wdf.set_index('unit', append=True, inplace=True)
        merge_list.append(wdf)
    return pd.concat(merge_list)


def _xDataArray_to_wide_df(xarr):
    df = xarr.to_dataframe()
    # find name of time column
    for time_col in ['time', 'year']:
        if time_col in df.index.names:
            break
    level = list(df.index.names).index(time_col)
    wdf = df.unstack(level=level)
    wdf['variable'] = xarr.name
    wdf['unit'] = str(xarr.pint.units)
    wdf.set_index('variable', append=True, inplace=True)
    wdf.set_index('unit', append=True, inplace=True)
    wdf.columns = wdf.columns.droplevel(0)
    return wdf

#%% Public functions

def multi_index_df_to_datatables(df, index_col_name = 'region'):
    #%%
    if not index_col_name in df.index.names:
        raise(Exception(f'New index columns name {index_col_name} must be in mult-index'))
    groupby_levels = [x for x in df.index.names if x != index_col_name]
    
    tables =  list()
    for idx, sub_df in df.groupby(groupby_levels):
        
        table = dt.Datatable.from_multi_indexed_dataframe(sub_df)
        tables.append(table)
    return tables
    #%%

def datatables_to_multi_index_df(tables,
                                 exclude_meta =['ID', 'creator', 'source_name','source_year', 'modified'],
                                 use_index_names = None):
    
    dfs = list()
    for table in tables:
        dfs.append(table.to_multi_index_dataframe(exclude_meta=exclude_meta,
                                                  use_index_names=use_index_names))
    index_names = set([x.index.names for x in dfs])
    if len(index_names) > 1:
        raise(Exception(f'Index level names do not aligen, please check: {index_names}'))
    return pd.concat(dfs)
    
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
    import pyam
    if isinstance(data, xr.Dataset):
        wdf = _xDataSet_to_wide_dataframe(data)
        return pyam.IamDataFrame(wdf)

    elif isinstance(data, xr.DataArray):
        wdf = _xDataArray_to_wide_df(data)
        return pyam.IamDataFrame(wdf)

    elif isinstance(data, (dict, dt.TableSet)):
        xds = _key_set_to_xdataset(data)
        wdf = _xDataSet_to_wide_dataframe(xds)

        # ugly fix #TODO
        if 'region' not in wdf.index.names:
            if None in wdf.index.names:
                new_names = [x if x is not None else 'region' for x in wdf.index.names]
                wdf.index = wdf.index.set_names(new_names)
        return pyam.IamDataFrame(wdf)

    else:
        raise (Exception(f'{type(data)} is not implemented'))


def to_wide_dataframe(data, index_cols=['Variable', 'Model', 'Scenario']):
    if isinstance(data, pd.DataFrame):
        wdf = data.set_index(index_cols)

    elif isinstance(data, xr.Dataset):
        wdf = _xDataSet_to_wide_dataframe(data)
        # wdf = wdf.reset_index(level='region')

    return wdf


def to_tableset(data, additional_meta=dict()):
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
    import pyam
    if isinstance(data, pd.DataFrame):
        years = _yearsColumnsOnly(data)
        meta_cols = data.columns.difference(years)
        data = data.set_index(list(meta_cols))
        data = data.reset_index(level='region')
        ts = dt.TableSet()
        for i, idx in enumerate(data.index.unique()):
            sel = data.loc[idx, :]
            if isinstance(sel, pd.Series):
                sel = pd.DataFrame(sel).T.set_index('region')
            else:
                sel = data.loc[idx, :].set_index('region')
            meta = {x: y for x, y in zip(data.index.names, idx)}
            meta.update(additional_meta)
            try:
                dt.core.getUnit(meta['unit'])
            except:
                print(f'Skipping because of unit {meta["unit"]}')
                print(meta)
                continue
            table = dt.Datatable(data=sel, meta=meta)
            table.columns = table.columns.astype(int)
            ts[i] = table

        return ts

    elif isinstance(data, xr.Dataset):
        wdf = _xDataSet_to_wide_dataframe(data)
        wdf = wdf.reset_index(level='region')
        ts = dt.TableSet()
        for i, idx in enumerate(wdf.index.unique()):
            sel = wdf.loc[idx, :].set_index('region')
            meta = {x: y for x, y in zip(wdf.index.names, idx)}
            meta.update(additional_meta)
            table = dt.Datatable(data=sel, meta=meta)
            ts[i] = table
        return ts

    elif isinstance(data, pyam.IamDataFrame):

        wdf = data.timeseries().reset_index()
        idx_cols = ['variable', 'model', 'scenario', 'unit']
        wdf = wdf.set_index(idx_cols)
        tables = dt.TableSet()
        for idx, df in tqdm(wdf.groupby(idx_cols)):
            meta = {key: value for key, value in zip(idx_cols, idx)}
            meta = dt.core._split_variable(meta)
            meta.update(additional_meta)

            year_columns = dt.util.yearsColumnsOnly(df.columns)
            remaining_cols = df.columns.difference(set(year_columns + ['region']))
            for col in remaining_cols:
                if len(df[col].unique()) == 1 and (
                    isinstance(df.loc[df.index[0], col], (str, float, int))
                ):
                    meta[col] = df.loc[df.index[0], col]
                else:
                    print(
                        f'Warning addition column information in {col} will be dropped'
                    )
            df = df.drop(remaining_cols, axis=1)
            try:
                dt.core.ur(meta['unit'])
            except:
                print(f"Skiping table due to unit {meta['unit']}")
                continue
            table = dt.Datatable(df.set_index('region'), meta=meta).clean()
            tables.add(table)

        return tables
    else:
        raise (Exception(f'{type(data)} is not implemented'))




def idf_to_xdset(idf, stacked_dims={'pathway': ('model', 'scenario')}):

    df_ = idf._data.unstack(level=['variable', 'unit'])
    # df_.drop([])
    coords_flatten = []
    coords_flatten = sum([list(x) for x in stacked_dims.values()], [])
    coords = {
        x: sorted(list(df_.index.get_level_values(level=x))) for x in coords_flatten
    }
    coords['pathway'] = list(range(len(df_.index)))
    sub_labels = list()
    labels = dict()

    new_index_names = set(df_.index.names)
    cols_to_drop = list()
    for st_dim, sub_dims in stacked_dims.items():
        coords[st_dim] = range(len(df_.index))
        sub_labels = list()
        new_index_names = new_index_names.difference(sub_dims).union([st_dim])
        cols_to_drop.extend(sub_dims)
        # codes, levels = _get_stacked_levels(df_.index, st_dim, sub_dims)
        index, labels = _pack_dimensions(df_.index, **stacked_dims)

        for i, name in enumerate(index.names):
            if name == st_dim:
                break

        df_[st_dim] = index.codes[i]
        for i_dim, sub_dim in enumerate(sub_dims):

            sub_labels.append(pd.Index(coords[sub_dim], name=sub_dim))
        # labels[st_dim]  = levels
        # del coords[sub_dims]
    df_ = df_.reset_index().set_index(list(new_index_names))
    df_ = df_.drop(cols_to_drop, axis=1)
    ds = xr.Dataset.from_dataframe(df_)

    # ds = dt.DataSet.from_pyam(idf_dt)
    xds = list()

    def adapt_units(unit):
        unit = (
            unit.replace('ktU', 'kt U')
            .replace('Mt CO2-equiv/yr', 'Mt CO2eq/yr')
            .replace('kt CF4-equiv/y', 'kt CF4eq/y')
            .replace('kt HFC134a-equiv/yr', 'kt HFC134aeq/yr')
            .replace('Index (2005 = 1)', 'dimensionless')  # ?? TODO
            .replace('US$', 'USD')
            .replace('kt HFC43-10/yr', 'kt HFC43_10/yr')
            .replace('m2', 'm**2')
        )
        return unit

    for variable, unit in ds.keys():
        xData = ds[variable, unit]
        xData = xData.pint.quantify(adapt_units(unit))
        xData.name = variable
        xds.append(xData)
    ds = xr.merge(xds)
    sub_dims = ['model', 'scenario']
    # sub_lables = [pd.Index(pd.Index(ds[coord]), name=coord) for coord in sub_dims]
    # labels = {'pathway' : pd.MultiIndex.from_arrays(sub_labels, names = sub_dims)}
    ds = ds.assign_coords(labels)

    return ds


def to_xdataset(
    data,
    dimensions=['model', 'scenario', 'region', 'time'],
    stacked_dims={'pathway': ('model', 'scenario')},
):
    import pyam
    if isinstance(data, pd.DataFrame):
        # wide dataframe
        data = data.reset_index()
        for dim in dimensions:
            if dim not in data.columns:
                print(f'Dimension {dim} not found in index names')

        # convert to table set
        print('convert to table set')
        data = to_tableset(data)

        ds = _key_set_to_xdataset(data)
        return ds
    elif isinstance(data, dt.TableSet):
        ds = _key_set_to_xdataset(data)
        return ds
    elif isinstance(data, pyam.IamDataFrame):

        # ds = dt.data_structures.DataSet.from_pyam(data,
        #                                           dimensions,
        #                                           stacked_dims)
        # wdf = data.timeseries()
        # ds  = to_xdataset(wdf)
        ds = idf_to_xdset(data, stacked_dims)
        return ds

    else:
        raise (Exception(f'Data type {type(data)} conversion not implemented'))




#%%
if __name__ == '__main__':

    
    import datatoolbox as dt

    xdata = dt.findp(
        variable=[
            'Emissions|CO2|Energy|Supply|Electricity',
            'Secondary Energy|Electricity',
        ],
        source='IPCC_SR15',
        pathway='**SSP1-19**',
    ).as_xarray()

    ts = to_tableset(xdata)
    idf = to_pyam(xdata)

    wdf = to_wide_dataframe(xdata)
    idf2 = dt.findp(
        variable=[
            'Emissions|CO2|Energy|Supply|Electricity',
            'Secondary Energy|Electricity',
        ],
        source='IPCC_SR15',
        pathway='**SSP1-19**',
    ).as_pyam()

    ds = to_xdataset(wdf.reset_index())

#%%
# res =
