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
from .util import isin


class _MAGGIC6:
    def read_MAGICC6_ScenFile(fileName, **kwargs):
        VALID_MASS_UNITS = {
            'Pt': 1e18,
            'Gt': 1e15,
            'Mt': 1e12,
            'kt': 1e9,
            't': 1e6,
            'Pg': 1e15,
            'Tg': 1e12,
            'Gg': 1e9,
            'Mg': 1e6,
            'kg': 1e3,
            'g': 1,
        }
        fid = open(fileName, 'r')
        nDataRows = int(fid.readline().replace('/n', ''))

        while True:
            # for i, line in enumerate(fid.readlines()):
            line = fid.readline()
            if line[:11] == '{0: >11}'.format('YEARS'):
                break
        # get first header line
        entities = line.split()[1:]

        # reading units
        unitLine = fid.readline().split()[1:]
        # print(unitLine)

        # find correct component
        components = [GHG_data.findEntryIdx(entity) for entity in entities]

        units = [unit for unit in unitLine if unit[:2] in VALID_MASS_UNITS]

        replacementDict = {'MtN2O-N': 'Mt N'}

        units = [replacementDict.get(unit, unit) for unit in units]

        columns = [(x, y, z) for x, y, z in zip(entities, components, units)]
        entityFrame = pd.DataFrame(columns=entities)
        entityFrame.columns = pd.MultiIndex.from_tuples(columns)

        entityFrame.columns.names = ['NAME', 'COMP', 'UNIT']

        for i, line in enumerate(fid.readlines()):

            if i == nDataRows:
                break
            data = line.split()
            entityFrame.loc[int(data[0])] = np.asarray(data[1:])

        # TODO: CHange to a results list of datatables
        return entityFrame

    def read_MAGICC6_MATLAB_bulkout(pathName):

        temp_offset = 0.61
        df = pd.read_table(
            pathName, skiprows=23, header=0, delim_whitespace=True, index_col=0
        )
        df = df + temp_offset
        df.index = df.index.astype(int)
        return df
        # df.values[df.values<0] = np.nan

    #
    #    meta = dict()
    #    meta['entity'] = 'global_temp'
    #    meta['unit']   = '°C'
    #    return Datatable(df, meta=meta)

    def read_MAGICC6_BINOUT(filePath):
        import pymagicc as pym

        reader = pym.io._BinaryOutReader(filePath)

        metaData, df = reader.read()

        data = df.pivot(index='region', columns='time', values='value')

        metaDict = dict()
        metaDict['entity'] = df.variable[0]
        metaDict['source'] = 'MAGICC6_calculation'
        metaDict['unit'] = None

        return Datatable(data, meta=metaDict)


class _primap:
    def read_PRIMAP_csv(fileName):

        metaMapping = {
            'entity': 'SHEET_ENTITY',
            'unit': 'SHEET_UNIT',
            'category': 'SHEET_NAME_CATEGORY',
            'scenario': 'SHEET_SCENARIO',
            'model': 'SHEET_SOURCE',
        }

        allDf = pd.read_csv(fileName, usecols=[0, 1], index_col=0)
        # print(os.path.basename(fileName))

        firstDataRow = allDf.loc['SHEET_FIRSTDATAROW', 'Unnamed: 1']

        # bugfix of old wrong formated PRIMAP files
        try:
            int(firstDataRow)
        except:
            firstDataRow = pd.np.where(allDf.index == "Countries\Years")[0][0] + 3

        firstMetaRow = pd.np.where(allDf.index == "&SHEET_SPECIFICATIONS")[0][0] + 1

        metaPrimap = dict()
        for row in range(firstMetaRow, firstDataRow):
            key = allDf.index[row]
            value = allDf.iloc[row, 0]
            if key == '/':
                break
            metaPrimap[key] = value

        data = pd.read_csv(fileName, header=firstDataRow - 2, index_col=0)

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
        return table  # , data

    def read_PRIMAP_Excel(fileName, sheet_name=0, xlsFile=None):
        if xlsFile is not None:
            xlsFile = xlsFile
        else:
            xlsFile = pd.ExcelFile(fileName)
        allDf = pd.read_excel(
            xlsFile, sheet_name=sheet_name, usecols=[0, 1], index_col=0
        )
        # print(os.path.basename(fileName))

        firstDataRow = allDf.loc['SHEET_FIRSTDATAROW', 'Unnamed: 1']

        # bugfix of old wrong formated PRIMAP files
        try:
            int(firstDataRow)
        except:
            firstDataRow = pd.np.where(allDf.index == "Countries\Years")[0][0] + 3

        # print(firstDataRow)
        setup = dict()
        setup['filePath'] = os.path.dirname(fileName) + '/'
        setup['fileName'] = os.path.basename(fileName)
        setup['sheetName'] = sheet_name
        setup['timeIdxList'] = (
            'B' + str(firstDataRow - 1),
            'XX' + str(firstDataRow - 1),
        )
        setup['spaceIdxList'] = ('A' + str(firstDataRow), 'A1000')
        # print(setup)
        ex = ExcelReader(setup, xlsFile=xlsFile)
        data = ex.gatherData().astype(float)
        # return data
        meta = {
            'source': '',
            'entity': allDf.loc['SHEET_ENTITY', 'Unnamed: 1'],
            'unit': allDf.loc['SHEET_UNIT', 'Unnamed: 1'],
            'category': allDf.loc['SHEET_NAME_CATEGORY', 'Unnamed: 1'],
            'scenario': allDf.loc['SHEET_SCENARIO', 'Unnamed: 1']
            + '|'
            + allDf.loc['SHEET_SOURCE', 'Unnamed: 1'],
        }
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
                raise (BaseException('Could not identifiy compatible data'))


class _matlab:
    def load_mat_file_as_dict(self, file_path):
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


class _EmissionModulePIK:

    N_ROWS_TO_ADD = 1
    ID_PARTS = [
        'SHEET_ENTITY',
        'SHEET_CATEGORY',
        'SHEET_CLASS',
        'SHEET_TYPE',
        'SHEET_SCENARIO',
        'SHEET_SOURCE',
    ]
    metaDict = {
        '&SHEET_SPECIFICATIONS': '',
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
        'SHEET_SUBSOURCE': '',
    }

    categoryNameMapping = {
        'CATM1A': 'Aviation',
        'CATM1B': 'Marine',
        'CATM0EL': 'National_total_exlucding_LULUCF',
    }

    def write_tables(self, file, tables, sheet_names):
        #%%

        if isinstance(file, str):
            close_writer = True
            writer = pd.ExcelWriter(file, engine='xlsxwriter')
        else:
            close_writer = False
            # assume pandas excel writer
            writer = file

        if not isinstance(tables, list):
            tables = list(tables)
            sheet_names = list(sheet_names)

        def _validLettersAndNumbers(string):
            return string.isalpha() or string.isnumeric()

        def validStrings(input):
            return ''.join(filter(_validLettersAndNumbers, input))

        #%%

        for table, sheet_name in zip(tables, sheet_names):
            header = pd.DataFrame(
                data=list(self.metaDict.values()),
                index=self.metaDict.keys(),
                columns=['values'],
            )

            header.loc['SHEET_ENTITY'] = (
                validStrings(table.meta['entity']).upper().replace('EMISSIONS', '')
            )
            header.loc['SHEET_CATEGORY'] = validStrings(table.meta['category']).upper()
            header.loc['SHEET_NAME_CATEGORY'] = self.categoryNameMapping[
                validStrings(table.meta['category']).upper()
            ]
            header.loc['SHEET_SCENARIO'] = validStrings(table.meta['pathway']).upper()
            header.loc['SHEET_UNIT'] = table.meta['unit']
            header.loc['SHEET_SOURCE'] = validStrings(table.meta['source']).upper()
            firstDataRow = len(header) + self.N_ROWS_TO_ADD
            header.loc['SHEET_FIRSTDATAROW'] = firstDataRow + 2
            header.loc['SHEET_CODE'] = '_'.join(
                [header.loc[x, 'values'] for x in self.ID_PARTS]
            )

            #            sheet_name = header.loc['SHEET_CODE'][0][:31]
            header.to_excel(writer, header=None, sheet_name=sheet_name)
            pd.DataFrame(table).to_excel(
                writer, startrow=firstDataRow, sheet_name=sheet_name
            )

        if close_writer:
            writer.close()


#        Excel.open('pandas_positioning.xlsx')


class Excel:
    def open(filePath):
        if config.OS == 'Linux':
            os.system('libreoffice ' + filePath)
        elif config.OS == 'Darwin':
            os.system('open -a "Microsoft Excel" ' + filePath)


# if config.AVAILABLE_XARRAY:
from datatoolbox.tools import xarray as _xr


class _Xarray:

    to_Xarray = _xr.to_XDataArray


#%%


class IAMC_PYAM:
    def __init__(self):

        self.entityDict = self._mapped_entities()

    def _mapped_entities(self):
        """
        Entities that are commonly used. Keys are the original code and
        values are the datatools equivalent
        """
        eDict = {
            # Emissions
            'Emissions|CO2': 'Emissions|CO2',
            'Emissions|CH4': 'Emissions|CH4',
            'Emissions|NOx': 'Emissions|NOx',
            'Emissions|NH3': 'Emissions|NH3',
            'Emissions|CO': 'Emissions|CO',
            'Emissions|BC': 'Emissions|BC',
            'Emissions|N2O': 'Emissions|N2O',
            'Emissions|KYOTOGHG_AR4': 'Emissions|KYOTOGHG_AR4',
            'Emissions|KYOTOGHGAR4': 'Emissions|KYOTOGHG_AR4',
            'Emissions|CO|AFOLU': 'Emissions|CO|AFOLU',
            'Emissions|SF6': 'Emissions|SF6',
            'Emissions|OC': 'Emissions|OC',
            'Emissions|HFC': 'Emissions|HFC',
            'Emissions|KYOTOGHG_AR5': 'Emissions|KYOTOGHG_AR5',
            'Emissions|Sulfur': 'Emissions|Sulfur',
            'Emissions|CO|Energy': 'Emissions|CO|Energy',
            'Emissions|F-Gases': 'Emissions|F-Gases',
            'Emissions|Sulfur|AFOLU': 'Emissions|Sulfur|AFOLU',
            'Emissions|VOC': 'Emissions|VOC',
            'Emissions|PFC': 'Emissions|PFC',
            # Energy
            'Primary_Energy': 'Primary_Energy',
            'Secondary_Energy': 'Secondary_Energy',
            'Final Energy'
            # Temperature assessment
            'Global_mean_temperature': 'Global_mean_temperature',
            # Misc
            'GDP|MER': 'GDP|MER',
            'GDP|PPP': 'GDP|PPP',
            'Subsidies': 'Subsidies',
            'Investment': 'Investment',
            'Population': 'Population',
            'Price|Carbon': 'Price|Carbon',
            'Carbon_Sequestration': 'Carbon_Sequestration',
            # Testing
            'Numbers': 'Numbers',
        }

        return eDict

    def _split_variable(self, variable):
        for key in self.entityDict.keys():
            if key in variable:
                category = variable.replace(key, '').lstrip('_').lstrip('|')
                entity = self.entityDict[key]
                return entity, category
        else:
            return None, None

    def from_IamDataFrame(self, idf):
        """Extracts a consistent time-series for a unique variable from ``idf``

        Parameters
        ----------
        idf : pyam.IamDataFrame
            pyam data frame from which to extract

        Returns
        -------
        TableSet
        Returns the extracted data as tableset

        """

        timeseries = idf.timeseries()
        meta_columns = set(timeseries.index.names).difference({"region"})

        datatables = TableSet()
        for model, scenario in idf.index:
            pathway = f"{scenario}|{model}"
            extra_meta = idf.meta.loc[model, scenario].to_dict()

            for variable in idf.variable:
                data = timeseries.loc[
                    isin(model=model, scenario=scenario, variable=variable)
                ]
                if data.empty:
                    continue
                print(variable)
                meta = {
                    "pathway": pathway,
                    **extra_meta,
                    **{
                        k: v
                        for k, v in zip(data.index.names, data.index[0])
                        if k in meta_columns
                    },
                }
                entity, category = self._split_variable(variable)
                if entity is not None:
                    meta['entity'] = entity
                    meta['category'] = category

                table = Datatable(data.droplevel(list(meta_columns)), meta=meta)

                try:
                    tableKey = table.generateTableID()
                except:
                    tableKey = '__'.join([variable, pathway])

                datatables[tableKey] = table.clean()

        return datatables

    def to_IamDataFrame(tableSet):

        import pyam

        long_table = tableSet.to_LongTable()
        long_table.index.name = None
        idf = pyam.IamDataFrame(pd.DataFrame(long_table))

        meta = pd.DataFrame([df.meta for df in self.values()])
        if 'model' not in meta:
            meta['model'] = ""
        if 'scenario' not in meta:
            meta['scenario'] = ""
        meta = (
            meta[
                pd.Index(['model', 'scenario', 'pathway']).append(
                    meta.columns[meta.columns.str.startswith('source')]
                )
            ]
            .set_index(['model', 'scenario'])
            .drop_duplicates()
        )

        idf.meta = meta
        idf.reset_exclude()

        return idf

    def read_IAMC_table(self, iamcData):
        """
        Class to help convert iamcData input to homogeneous data tables

        Tables are split according the column variable of the iamcData table.
        The following colums are expected ['model', 'scenario', 'region', 'variable', 'unit']
        and a variable amount of year columns.
        The relationList maps which variable name is mapped to which output table.

        RelationDict Example:
        relationDict = {'Emissions|KYOTOGHGAR4|IPCM0EL' : {'entitiy' : 'Emissions|KYOTOGHGAR4',
                                                           'category': 'IPCM0EL'}}
        """

        print('this function is decribcated, please use from_idf')
        from types import SimpleNamespace

        #    import re

        #    YEAR_EXP = re.compile('^[0-9]{4}$')
        #    dataColumnsIds = [int(x) for x in iamcData.columns if YEAR_EXP.search(str(x)) is not None]

        dataColumnsIds = iamcData.year

        outTables = list()

        for varName, mappDict in relationDict.items():

            if varName not in set(iamcData.variable):
                raise (
                    BaseException(
                        'Required variable "{}" not found in input table'.format(
                            varName
                        )
                    )
                )
            ids = iamcData.data.loc[:, 'variable'] == varName
            idx0 = iamcData.data.index[ids][0]
            mask
            dataExtract = iamcData.data.loc[ids, dataColumnsIds]
            dataExtract.meta = SimpleNamespace()
            dataExtract.index = iamcData.data.region[ids]

            # asserting that the unit, scenario and model data is only containing
            #  the same value
            assert iamcData.data.unit[ids].nunique() == 1
            assert iamcData.data.scenario[ids].nunique() == 1
            assert iamcData.data.model[ids].nunique() == 1

            meta = dict()
            for key in mappDict.keys():
                meta[key] = mappDict[key]
            #        meta['entity'] = varName
            meta['model'] = iamcData.loc[idx0, 'model']
            meta['scenario'] = iamcData.loc[idx0, 'scenario']
            meta['unit'] = iamcData.loc[idx0, 'unit']

            outTables.append(Datatable(dataExtract, meta=meta))

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

    requiredColumns = set(
        ['model', 'scenario', 'region', 'variable', 'unit', 'value', 'year']
    )

    outTables = list()

    if not set(longDf.columns) == requiredColumns:
        raise (
            BaseException('Input data must have this columns' + str(requiredColumns))
        )

    for varName in relationList:

        if varName not in list(longDf.loc[:, 'variable']):
            raise (
                BaseException(
                    'Required variable "{}" not found in input table'.format(varName)
                )
            )
        ids = longDf.loc[:, 'variable'] == varName
        idx0 = longDf.index[ids][0]
        dataExtract = longDf.loc[ids, :].pivot(
            index='region', columns='year', values='value'
        )
        # dataExtract.index= longDf.region[ids]
        dataExtract.columns = dataExtract.columns.astype(int)

        # asserting that the unit, scenario and model data is only containing
        #  the same value
        assert longDf.unit[ids].nunique() == 1
        assert longDf.scenario[ids].nunique() == 1
        assert longDf.model[ids].nunique() == 1

        meta = dict()
        meta['entity'] = varName
        meta['model'] = longDf.loc[idx0, 'model']
        meta['scenario'] = longDf.loc[idx0, 'scenario']
        meta['unit'] = longDf.loc[idx0, 'unit']

        outTables.append(dt.Datatable(dataExtract, meta=meta))

    return outTables


matlab = _matlab()
emission_module = _EmissionModulePIK()
if config.AVAILABLE_XARRAY:
    xarray = _Xarray()

pyam = IAMC_PYAM()


if __name__ == '__main__':
    import pandas as pd

    longDf = pd.read_csv('data/long_test_data.csv')
    outTables = read_long_table(longDf, ['CH4|AGR', 'CH4|DOM'])
