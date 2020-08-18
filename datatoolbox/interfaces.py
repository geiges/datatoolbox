#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 09:58:27 2019

@author: Andreas Geiges
"""

def load_matlab_mat_file_as_dict(file_path):
    """
    Function to load a complex mat file as a dictionary
    Source for code: 

    Parameters
    ----------
    file_path : str
        Path to the .mat file to load

    Returns
    -------
    None.

    """
    from scipy.io import loadmat, matlab
    
    def _check_vars(d):
        """
        Checks if entries in dictionary are mat-objects. If yes
        todict is called to change them to nested dictionaries
        """
        for key in d:
            if isinstance(d[key], matlab.mio5_params.mat_struct):
                d[key] = _todict(d[key])
            elif isinstance(d[key], np.ndarray):
                d[key] = _toarray(d[key])
        return d

    def _todict(matobj):
        """
        A recursive function which constructs from matobjects nested dictionaries
        """
        d = {}
        for strg in matobj._fieldnames:
            elem = matobj.__dict__[strg]
            if isinstance(elem, matlab.mio5_params.mat_struct):
                d[strg] = _todict(elem)
            elif isinstance(elem, np.ndarray):
                d[strg] = _toarray(elem)
            else:
                d[strg] = elem
        return d

    def _toarray(ndarray):
        """
        A recursive function which constructs ndarray from cellarrays
        (which are loaded as numpy ndarrays), recursing into the elements
        if they contain matobjects.
        """
        if ndarray.dtype != 'float64':
            elem_list = []
            for sub_elem in ndarray:
                if isinstance(sub_elem, matlab.mio5_params.mat_struct):
                    elem_list.append(_todict(sub_elem))
                elif isinstance(sub_elem, np.ndarray):
                    elem_list.append(_toarray(sub_elem))
                else:
                    elem_list.append(sub_elem)
            return np.array(elem_list)
        else:
            return ndarray

    data = loadmat(file_path, struct_as_record=False, squeeze_me=True)
    return _check_vars(data)


def read_IAMC_table(iamcData, relationList):
    import datatoolbox as dt
    import pandas as pd
    
    """
    Class to help convert iamcData input to homogeneous data tables
    
    Tables are split according the column variable of the iamcData table.
    The following colums are expected ['model', 'scenario', 'region', 'variable', 'unit']
    and a variable amount of year columns.
    The relationList maps which variable name is mapped to which output table.
    """
    from types import SimpleNamespace
    import re
    
    YEAR_EXP = re.compile('^[0-9]{4}$')
    dataColumnsIds = [int(x) for x in iamcData.columns if YEAR_EXP.search(str(x)) is not None] 
    
    outTables = list()
    
    for varName in relationList:
        
        if varName not in list(iamcData.loc[:,'variable']):
            raise(BaseException('Required variable "{}" not found in input table'.format(varName)))
        ids = iamcData.loc[:,'variable'] == varName
        idx0 = iamcData.index[ids][0]
        dataExtract = iamcData.loc[ids, dataColumnsIds]
        dataExtract.meta = SimpleNamespace()
        dataExtract.index= iamcData.region[ids]
        
        # asserting that the unit, scenario and model data is only containing
        #  the same value
        assert iamcData.unit[ids].nunique() ==1
        assert iamcData.scenario[ids].nunique() ==1
        assert iamcData.model[ids].nunique() ==1
        
        meta = dict()
        meta['entity'] = varName
        meta['model']    = iamcData.loc[idx0,'model']
        meta['scenario'] = iamcData.loc[idx0,'scenario']
        meta['unit']     = iamcData.loc[idx0,'unit']
    
        outTables.append(dt.Datatable(dataExtract, meta= meta))
    
    return outTables

def read_long_table(longDf, relationList):

    """
    Function to  convert long table input to homogeneous data tables
    
    Tables are split according the column variable of the iamcData table.
    The following colums are expected ['model', 'scenario', 'region', 'variable', 'unit']
    and a variable amount of year columns.
    The relationList maps which variable name is mapped to which output table.
    """
    import datatoolbox as dt
    import pandas as pd
    
    requiredColumns = set(['model', 'scenario','region','variable','unit', 'value', 'year'])

    outTables = list()
    
    if not set(longDf.columns) == requiredColumns:
        raise(BaseException('Input data must have this columns' + str(requiredColumns)))
    
    for varName in relationList:
        
        if varName not in list(longDf.loc[:,'variable']):
            raise(BaseException('Required variable "{}" not found in input table'.format(varName)))
        ids = longDf.loc[:,'variable'] == varName
        idx0 = longDf.index[ids][0]
        dataExtract = longDf.loc[ids, :].pivot(index='region', columns='year', values='value')
        #dataExtract.index= longDf.region[ids]
        dataExtract.columns = dataExtract.columns.astype(int)
        
        # asserting that the unit, scenario and model data is only containing
        #  the same value
        assert longDf.unit[ids].nunique() ==1
        assert longDf.scenario[ids].nunique() ==1
        assert longDf.model[ids].nunique() ==1
        
        meta = dict()
        meta['entity'] = varName
        meta['model']    = longDf.loc[idx0,'model']
        meta['scenario'] = longDf.loc[idx0,'scenario']
        meta['unit']     = longDf.loc[idx0,'unit']
    
        outTables.append(dt.Datatable(dataExtract, meta= meta))
    
    return outTables


if __name__ == '__main__':
    import pandas as pd
    longDf = pd.read_csv('data/long_test_data.csv')
    outTables = read_long_table(longDf, ['CH4|AGR', 'CH4|DOM'])
