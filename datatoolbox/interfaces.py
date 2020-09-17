#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 09:58:27 2019

@author: Andreas Geiges
"""
import numpy as np
from .data_structures import Datatable, TableSet
from . import config
import os
import pandas as pd
from . import core

class _MAGGIC6():
    def read_MAGICC6_ScenFile(fileName, **kwargs):
        VALID_MASS_UNITS= {
                'Pt': 1e18,
                'Gt': 1e15,
                'Mt': 1e12,
                'kt': 1e9,
                't' : 1e6,
                'Pg': 1e15,
                'Tg': 1e12,
                'Gg': 1e9,
                'Mg': 1e6,
                'kg': 1e3,
                'g' : 1}
        fid = open(fileName,'r')
        nDataRows    = int(fid.readline().replace('/n',''))
        
        while True:
        #for i, line in enumerate(fid.readlines()):
            line = fid.readline()
            if line[:11] == '{0: >11}'.format('YEARS'):
                break
        #get first header line
        entities  = line.split()[1:]
        
        
        #reading units
        unitLine = fid.readline().split()[1:]
        #print(unitLine)
        
        # find correct component
        components = [GHG_data.findEntryIdx(entity) for entity in entities]
    
        units = [unit for unit in unitLine if unit[:2] in VALID_MASS_UNITS]        
        
        
        replacementDict = {'MtN2O-N' : 'Mt N'}
    
        units = [replacementDict.get(unit, unit) for unit in units]
    
        columns = [(x,y,z) for x,y,z in zip(entities, components, units)]
        entityFrame =  pd.DataFrame(columns=entities)
        entityFrame.columns = pd.MultiIndex.from_tuples(columns)
        
        entityFrame.columns.names=['NAME', 'COMP','UNIT']
    
        for i, line in enumerate(fid.readlines()):
            
            if i == nDataRows:
                break
            data = line.split()
            entityFrame.loc[int(data[0])] = np.asarray(data[1:])
    
        # TODO: CHange to a results list of datatables
        return entityFrame


    def read_MAGICC6_MATLAB_bulkout(pathName):
        
        temp_offset = 0.61
        df = pd.read_table(pathName, skiprows=23, header=0, delim_whitespace=True, index_col=0)
        df = df + temp_offset
        df.index = df.index.astype(int)
        return df
        #df.values[df.values<0] = np.nan
    #    
    #    meta = dict()
    #    meta['entity'] = 'global_temp'
    #    meta['unit']   = '°C'
    #    return Datatable(df, meta=meta)
    
    def read_MAGICC6_BINOUT(filePath):
        import pymagicc as pym
        reader = pym.io._BinaryOutReader(filePath)
    
        metaData, df = reader.read()
        
        data = df.pivot(index='region',columns='time', values='value')
        
        metaDict = dict()
        metaDict['entity'] = df.variable[0]
        metaDict['source'] = 'MAGICC6_calculation'
        metaDict['unit']   = None
    
        return Datatable(data, meta=metaDict)

class _primap():
    def read_PRIMAP_csv(fileName):
        
        metaMapping = {
                'entity': 'SHEET_ENTITY',
                'unit':'SHEET_UNIT',
                'category':'SHEET_NAME_CATEGORY',
                'scenario': 'SHEET_SCENARIO',
                'model' : 'SHEET_SOURCE'
                }
        
        allDf = pd.read_csv(fileName, usecols=[0,1], index_col=0)
        #print(os.path.basename(fileName))
    
        firstDataRow = allDf.loc['SHEET_FIRSTDATAROW','Unnamed: 1']
        
        #bugfix of old wrong formated PRIMAP files
        try:
            int(firstDataRow)
        except:
            firstDataRow = pd.np.where(allDf.index == "Countries\Years")[0][0]+3
       
        firstMetaRow = pd.np.where(allDf.index == "&SHEET_SPECIFICATIONS")[0][0]+1
        
        metaPrimap = dict()
        for row in range(firstMetaRow,firstDataRow):
            key = allDf.index[row]
            value = allDf.iloc[row,0]
            if key =='/':
                break
            metaPrimap[key] = value
        
        data = pd.read_csv(fileName, header=firstDataRow-2, index_col=0)
        
        meta = dict()
        for metaKey in metaMapping:
            
            if isinstance(metaMapping[metaKey], list):
                value = '_'.join(metaPrimap[x] for x in metaMapping[metaKey])
            else:
                value = metaPrimap[metaMapping[metaKey]]
            meta[metaKey] = value
        
        
        
        table = Datatable(data, meta=meta)
        table = table.loc[:, util.yearsColumnsOnly(table)]
        table.columns = table.columns.astype(int)
        return table#, data
    
    def read_PRIMAP_Excel(fileName, sheet_name= 0, xlsFile=None):
        if xlsFile is not None:
            xlsFile = xlsFile
        else:   
            xlsFile = pd.ExcelFile(fileName)
        allDf = pd.read_excel(xlsFile, sheet_name = sheet_name, usecols=[0,1], index_col=0)
        #print(os.path.basename(fileName))
    
        firstDataRow = allDf.loc['SHEET_FIRSTDATAROW','Unnamed: 1']
        
        #bugfix of old wrong formated PRIMAP files
        try:
            int(firstDataRow)
        except:
            firstDataRow = pd.np.where(allDf.index == "Countries\Years")[0][0]+3
            
        #print(firstDataRow)
        setup = dict()
        setup['filePath']  = os.path.dirname(fileName) +'/'
        setup['fileName']  = os.path.basename(fileName)
        setup['sheetName'] = sheet_name
        setup['timeIdxList']  = ('B' + str(firstDataRow-1), 'XX'+ str(firstDataRow-1))
        setup['spaceIdxList'] = ('A'+ str(firstDataRow), 'A1000')
        #print(setup)
        ex = ExcelReader(setup, xlsFile=xlsFile)
        data = ex.gatherData().astype(float)
        #return data
        meta = {'source': '',
                'entity': allDf.loc['SHEET_ENTITY','Unnamed: 1'],
                'unit': allDf.loc['SHEET_UNIT','Unnamed: 1'],
                'category': allDf.loc['SHEET_NAME_CATEGORY','Unnamed: 1'],
                'scenario': allDf.loc['SHEET_SCENARIO','Unnamed: 1'] + '|' + allDf.loc['SHEET_SOURCE','Unnamed: 1']}
        REG_ton = re.compile('^[GM]t')
        xx = REG_ton.search(meta['unit'])
        
        if xx:
            meta['unit'] = meta['unit'].replace(xx.group(0), xx.group(0) + ' ')
        
        table = Datatable(data, meta=meta)
        try:
            table.columns = table.columns.astype(int)
        except:
            print('warning: Columns could not be converted to int')
        return table

    def write_PRIMAP_Excel(data, fileName):
        
        if ~isinstance(data, TableSet):
            
            if isinstance(data, list):
                tableSet = TableSet(data)
                
            elif isinstance(data, (Datatable, pd.DataFrame)):
                tableSet = TableSet([data])
                
            else:
                raise(BaseException('Could not identifiy compatible data'))
                
            
            

class _matlab():
    def load_mat_file_as_dict(file_path):
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

class _EmissionModulePIK():
    
    
    N_ROWS_TO_ADD = 1
    ID_PARTS = ['SHEET_ENTITY', 
                'SHEET_CATEGORY', 
                'SHEET_CLASS', 
                'SHEET_TYPE', 
                'SHEET_SCENARIO', 
                'SHEET_SOURCE']
    metaDict = {'&SHEET_SPECIFICATIONS': '',
                'SHEET_CODE': '',
                'SHEET_CATEGORY': '',
                'SHEET_NAME_CATEGORY': '',
                'SHEET_ENTITY': '',
                'SHEET_TYPE': 'NET',
                'SHEET_CLASS': 'TOTAL',
                'SHEET_DESCR': '',
                'SHEET_NOTE': '',
                'SHEET_SOURCE': '',
                'SHEET_SCENARIO': '',
                'SHEET_FIRSTDATAROW': '',
                'SHEET_UNIT': '',
                'SHEET_DATATYPE': '',
                'SHEET_SUBSOURCE':''}
    
    categoryNameMapping = {'CATM1A' : 'Aviation',
                           'CATM1B' : 'Marine'}
            
    def write_tables(self, tables, sheet_names, filePath):
        #%%
        
        if not isinstance(tables, list):
            tables = list(tables)
            sheet_names = list(sheet_names)
        
        
        def _validLettersAndNumbers(string):
            return string.isalpha() or string.isnumeric()
        def validStrings(input):
            return  ''.join(filter(_validLettersAndNumbers, input))
    #%%
        writer = pd.ExcelWriter(filePath, engine='xlsxwriter')
        
        for table, sheet_name in zip(tables, sheet_names):
            header = pd.DataFrame(data=list(self.metaDict.values()), index= self.metaDict.keys(), columns=['values'])
            
            header.loc['SHEET_ENTITY'] = validStrings(table.meta['entity']).upper().replace('EMISSIONS','')
            header.loc['SHEET_CATEGORY'] = validStrings(table.meta['category']).upper()
            header.loc['SHEET_NAME_CATEGORY'] = self.categoryNameMapping[validStrings(table.meta['category']).upper()]
            header.loc['SHEET_SCENARIO'] = validStrings(table.meta['pathway']).upper()
            header.loc['SHEET_UNIT'] = table.meta['unit']
            header.loc['SHEET_SOURCE'] = validStrings(table.meta['source']).upper()
            firstDataRow = len(header) + self.N_ROWS_TO_ADD
            header.loc['SHEET_FIRSTDATAROW'] = firstDataRow +2
            header.loc['SHEET_CODE']   = '_'.join([header.loc[x,'values'] for x in self.ID_PARTS])
            
        
#            sheet_name = header.loc['SHEET_CODE'][0][:31]
            header.to_excel(writer, header=None, sheet_name= sheet_name)
            pd.DataFrame(table).to_excel(writer, startrow=firstDataRow, sheet_name= sheet_name)
        writer.close()
#        Excel.open('pandas_positioning.xlsx')
       #%%
class Excel():
    
    def open(filePath):
        if config.OS == 'Linux':
            os.system('libreoffice ' + filePath )
        elif config.OS == 'Darwin':
            os.system('open -a "Microsoft Excel" ' + filePath )
            #%%
class _Xarray():

    to_Xarray = core.to_XDataArray
        
    
    
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

matlab = _matlab()
emission_module = _EmissionModulePIK()
if config.AVAILABLE_XARRAY:
    xarray = _Xarray()





if __name__ == '__main__':
    import pandas as pd
    longDf = pd.read_csv('data/long_test_data.csv')
    outTables = read_long_table(longDf, ['CH4|AGR', 'CH4|DOM'])
