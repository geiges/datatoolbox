#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 11:20:42 2019

@author: andreas geiges
"""

from collections import defaultdict
import datatoolbox as dt
from datatoolbox import config
from datatoolbox.data_structures import Datatable
from datatoolbox.util import isUnit, identifyCountry
from datatoolbox.tools.pandas import yearsColumnsOnly
from datatoolbox import mapping
from collections import defaultdict
from functools import reduce
import pandas as pd
import numpy as np
import os
import time
import pint
import re

tt = time.time()

MAPPING_COLUMNS = (
    list(config.ID_FIELDS) + config.OPTIONAL_META_FIELDS + ['unit', 'unitTo']
)
VAR_MAPPING_SHEET = 'variable_mapping'
SPATIAL_MAPPING_SHEET = 'spatial_mapping'


def IEA_mapping_generation():
    path = '/media/sf_Documents/datashelf/rawdata/IEA2018/'
    os.chdir(path)
    df = pd.read_csv(
        'World_Energy_Balances_2018_clean.csv',
        encoding="ISO-8859-1",
        engine='python',
        index_col=None,
        header=0,
        na_values='..',
    )


def highlight_column(s, col):
    return ['background-color: #d42a2a' if s.name in col else '' for v in s.index]


#%% Import Reader Classes:


class setupStruct(object):
    pass


class sourcesStruct(object):
    def __init__(self):
        self.inventory = list()

    def __setitem__(self, name, obj):
        self.__setattr__(name, obj)
        self.inventory.append(name)

    def getAll(self):
        return self.inventory

    def __str__(self):
        return str([x for x in self.inventory])


class BaseImportTool:
    def __init__(self):
        self.setup = setupStruct()

    def add_standard_region(self, table, mapping):

        # for idx in table.index:
        #     if idx not in mapping.keys():
        #         iso = identifyCountry(idx)
        #         if iso is not None:
        #             mapping[idx] = iso
        #         else:
        #             mapping[idx] = np.nan

        table['standard_region'] = table.index.map(mapping)
        # table.meta['_index_names'] = ['region', 'standard_region']

        return table

    def loadData(self):
        pass

    def loadVariableMapping(self):
        self.mapping = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET
        )

    def gatherMappedData(self):
        pass

    def openMappingFile(self):
        if dt.config.OS == 'Linux':
            os.system('libreoffice ' + self.setup.MAPPING_FILE)
        elif dt.config.OS == 'Darwin':
            os.system('open -a "Microsoft Excel" ' + self.setup.MAPPING_FILE)

    def openRawData(self):
        if dt.config.OS == 'Linux':
            os.system('libreoffice ' + self.setup.DATA_FILE)
        elif dt.config.OS == 'Darwin':
            os.system('open -a "Microsoft Excel" ' + self.setup.DATA_FILE)

    def open_source_website(self):
        import webbrowser

        webbrowser.open(self.meta['source_url'])

    def createSourceMeta(self):
        self.meta = {
            'SOURCE_ID': self.setup.SOURCE_ID,
            'collected_by': config.CRUNCHER,
            'date': dt.core.getDateString(),
            'source_url': self.setup.URL,
            'licence': self.setup.LICENCE,
        }

    def update_database(
        self,
        tableList,
        updateContent=False,
        append_data=False,
        overwrite=False,
        clean_Tables=True,
    ):
        # tableList = self.gatherMappedData(updateTables=updateContent)
        # dt.commitTables(tableList, f'update {self.__class__.__name__} data', self.meta, update=updateContent)
        self.createSourceMeta()

        if hasattr(self.setup, 'MAPPING_FILE'):
            dt.core.DB.update_mapping_file(
                self.setup.SOURCE_ID, self.setup.MAPPING_FILE, self.meta
            )
        else:
            print('No mapping file saved')

        dt.commitTables(
            tableList,
            f'update {self.__class__.__name__} data at {dt.getDateString()} by {dt.config.CRUNCHER}',
            sourceMetaDict=self.meta,
            update=updateContent,
            append_data=append_data,
            overwrite=overwrite,
            cleanTables=True,
        )


class WDI(BaseImportTool):
    def __init__(self, year, data_path=None):

        self.setup = setupStruct()

        if data_path is None:
            data_path = os.path.join(config.PATH_TO_DATASHELF, 'raw_data')

        self.setup.SOURCE_ID = f"WDI_{year}"
        self.setup.SOURCE_NAME = "WDI"
        self.setup.SOURCE_YEAR = f"{year}"
        self.setup.SOURCE_PATH = os.path.join(data_path)
        self.setup.DATA_FILE = 'WDIData.csv'
        self.setup.MAPPING_FILE = os.path.join(data_path, 'mapping.xlsx')
        self.setup.LICENCE = 'CC BY-4.0'
        self.setup.URL = (
            'https://datacatalog.worldbank.org/dataset/world-development-indicators'
        )

        self.setup.INDEX_COLUMN_NAME = 'Indicator Code'
        self.setup.SPATIAL_COLUM_NAME = 'Country Code'

        self.setup.COLUMNS_TO_DROP = ['Country Name', 'Indicator Name']

        self.createSourceMeta()

        if not (os.path.exists(self.setup.MAPPING_FILE)):

            self.createVariableMapping()

        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET
            )

    def createVariableMapping(self):

        fullFilePath = os.path.join(self.setup.SOURCE_PATH, self.setup.DATA_FILE)
        self.availableSeries = pd.read_csv(fullFilePath, index_col=None, header=0)
        print(self.availableSeries.index)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index,
            columns=list(config.REQUIRED_META_FIELDS) + ['unitTo'],
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping.scenario = 'Historic'

        self.mapping.to_excel(
            self.setup.MAPPING_FILE, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET
        )

    def loadData(self):
        fullFilePath = os.path.join(self.setup.SOURCE_PATH, self.setup.DATA_FILE)
        self.data = pd.read_csv(
            fullFilePath, index_col=self.setup.INDEX_COLUMN_NAME, header=0
        )

    def gatherMappedData(self, spatialSubSet=None):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        indexDataToCollect = self.mapping.index[~pd.isnull(self.mapping['entity'])]

        tablesToCommit = []
        for idx in indexDataToCollect:
            metaDf = self.mapping.loc[idx]

            # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
            # print(metaData[self.setup.INDEX_COLUMN_NAME])
            metaDf['timeformat'] = '%Y'
            metaDf['source_name'] = self.setup.SOURCE_NAME
            metaDf['source_year'] = self.setup.SOURCE_YEAR
            metaDict = {
                key: metaDf[key]
                for key in config.REQUIRED_META_FIELDS.union({'category', 'unitTo'})
            }
            #            metaDict['unitTo'] = self.mappingEntity.loc[entity]['unitTo']
            seriesIdx = metaDf['Series Code']
            metaDict['original code'] = metaDf['Series Code']
            metaDict['original name'] = metaDf['Indicator Name']

            if pd.isnull(metaDict['category']):
                metaDict['category'] = ''

            dataframe = self.data.loc[seriesIdx]

            if spatialSubSet:
                spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(spatialSubSet)
                dataframe = dataframe.loc[spatIdx]

            dataframe = dataframe.set_index(self.setup.SPATIAL_COLUM_NAME).drop(
                self.setup.COLUMNS_TO_DROP, axis=1
            )
            dataframe = dataframe.dropna(axis=1, how='all')

            dataTable = Datatable(dataframe, meta=metaDict)

            dataTable.loc['EU28', :] = dataTable.loc['EUU', :]

            if 'unitTo' in metaDict.keys() and (not pd.isna(metaDict['unitTo'])):
                dataTable = dataTable.convert(metaDict['unitTo'])

            tablesToCommit.append(dataTable)
        return tablesToCommit


class EEA_DATA(BaseImportTool):
    def __init__(self, year):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = f"EEA_{year}"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, f'rawdata/EEA_{year}'
        )
        self.setup.DATA_FILE = f'EU members targets 13_07_21.xlsx'
        # self.setup.MAPPING_FILE = self.setup.SOURCE_PATH + 'mapping_detailed.xlsx'
        self.setup.LICENCE = 'restricted'
        self.setup.URL = 'https://www.iea.org/statistics/co2emissions/'
        self.year = year

    def gatherMappedData(self):

        tables = list()

        fullFilePath = os.path.join(self.setup.SOURCE_PATH, self.setup.DATA_FILE)
        # loading data if necessary
        target_data = pd.read_excel(
            fullFilePath,
            index_col=0,
            header=1,
            sheet_name='Data all countries',
            usecols=[0, 3, 4],
        )

        target_data = dt.tools.pandas.convertIndexToISO(target_data)

        meta = {
            'entity': 'Emissions|KYOTOGHG_AR4',
            'category': 'Total_excl_LULUCF',
            'scenario': 'National_targets|Min',
            'source': f'EEA_{self.year}',
            'unit': 'Mt CO2eq',
        }

        target_data_min = dt.Datatable(
            target_data.loc[:, ['in MtCO2eq (min)']], meta=meta
        )
        target_data_min.columns = [2030]

        meta = {
            'entity': 'Emissions|KYOTOGHG_AR4',
            'category': 'Total_excl_LULUCF',
            'scenario': 'National_targets|Max',
            'source': f'EEA_{self.year}',
            'unit': 'Mt CO2eq',
        }

        target_data_max = dt.Datatable(
            target_data.loc[:, ['in MtCO2eq (max)']], meta=meta
        )
        target_data_max.columns = [2030]

        tables.append(target_data_min)
        tables.append(target_data_max)

        hist_data = pd.read_excel(
            fullFilePath,
            index_col=0,
            header=1,
            sheet_name='Data all countries',
            usecols=[0] + list(range(14, 43)),
        )

        hist_data = dt.tools.pandas.convertIndexToISO(hist_data)

        meta = {
            'entity': 'Emissions|KYOTOGHG_AR4',
            'category': 'Total_excl_LULUCF',
            'scenario': 'Historic',
            'source': f'EEA_{self.year}',
            'unit': 'kt CO2eq',
        }

        hist_data = dt.Datatable(hist_data, meta=meta).convert('Mt CO2eq')

        tables.append(hist_data)

        # CPP
        cpp_wem_data = pd.read_excel(
            fullFilePath,
            index_col=0,
            header=1,
            sheet_name='Data all countries',
            usecols=[0] + list(range(43, 55)),
        )

        cpp_wem_data = dt.tools.pandas.convertIndexToISO(cpp_wem_data)

        meta = {
            'entity': 'Emissions|KYOTOGHG_AR4',
            'category': 'Total_excl_LULUCF',
            'scenario': 'Current_policies',
            'source': f'EEA_{self.year}',
            'unit': 'kt CO2eq',
        }

        cpp_wem_data = dt.Datatable(cpp_wem_data, meta=meta).convert('Mt CO2eq')

        tables.append(cpp_wem_data)

        cpp_wam_data = pd.read_excel(
            fullFilePath,
            index_col=0,
            header=1,
            sheet_name='Data all countries',
            usecols=[0] + list(range(56, 68)),
        )
        cpp_wam_data.columns = [int(x[:4]) for x in cpp_wam_data.columns]
        cpp_wam_data = dt.tools.pandas.convertIndexToISO(cpp_wam_data)

        meta = {
            'entity': 'Emissions|KYOTOGHG_AR4',
            'category': 'Total_excl_LULUCF',
            'scenario': 'Additional_policies',
            'source': f'EEA_{self.year}',
            'unit': 'kt CO2eq',
        }

        cpp_wam_data = dt.Datatable(cpp_wam_data, meta=meta).convert('Mt CO2eq')

        tables.append(cpp_wam_data)

        return tables, None


class IEA_GHG_FUEL_DETAILED(BaseImportTool):
    """
    IEA World fuel emissions detail version
    """

    def __init__(
        self,
        year,
        raw_data_folder=None,
    ):

        if raw_data_folder is None:
            raw_data_folder = os.path.join(config.PATH_TO_DATASHELF, 'rawdata')
        self.setup = setupStruct()
        self.setup.SOURCE_ID = f"IEA_GHG_FUEL_DETAILED_{year}"
        self.setup.SOURCE_PATH = os.path.join(raw_data_folder, f'IEA_GHG_FUEL_{year}')
        self.setup.DATA_FILE = os.path.join(self.setup.SOURCE_PATH, f'World_BigCO2.csv')
        self.setup.MAPPING_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'mapping_detailed.xlsx'
        )
        self.setup.LICENCE = 'restricted'
        self.setup.URL = 'https://www.iea.org/statistics/co2emissions/'

        self.setup.INDEX_COLUMN_NAME = ['FLOW (kt of CO2)', 'PRODUCT']
        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        self.setup.COLUMNS_TO_DROP = ['PRODUCT', 'FLOW (kt of CO2)']

        self.IEA_WEB_SOURCE = f"IEA_WEB_DETAILED_{year}"

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = self.loadVariableMapping()
            self.spatialMapping = self.loadSpatialMapping()

        self.createSourceMeta()

    def loadVariableMapping(
        self,
    ):
        mapping = dict()

        for variableName in self.setup.INDEX_COLUMN_NAME + ["COMPUTED"]:
            mapping[variableName] = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=variableName, index_col=0
            )
            notNullIndex = mapping[variableName].index[
                ~mapping[variableName].isna().mapping
            ]
            mapping[variableName] = mapping[variableName].loc[notNullIndex]

        return mapping

    def loadSpatialMapping(
        self,
    ):
        return pd.read_excel(self.setup.MAPPING_FILE, sheet_name='spatial', index_col=0)

    def loadData(self):
        cols = pd.read_csv(
            self.setup.DATA_FILE, encoding="ISO-8859-1", header=0, skiprows=1, nrows=0
        ).columns
        self.data = pd.read_csv(
            self.setup.DATA_FILE,
            encoding="ISO-8859-1",
            header=0,
            skiprows=[1],
            na_values=["x", "..", "c"],
        )
        self.data.columns = cols[:3].append(self.data.columns[3:])

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        for column in self.setup.INDEX_COLUMN_NAME:
            # models
            index = self.data.loc[:, column].unique()

            self.availableSeries = pd.DataFrame(index=index)
            self.mapping = pd.DataFrame(index=index, columns=['mapping'])

            self.mapping.to_excel(writer, engine='openpyxl', sheet_name=column)

        # spatial mapping
        column = self.setup.SPATIAL_COLUM_NAME

        index = self.data.loc[:, column].unique()

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['mapping'])

        for region in self.mapping.index:
            coISO = dt.mapp.getSpatialID(region)

            if coISO is not None:
                self.mapping.loc[region, 'mapping'] = coISO

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='spatial')

        writer.close()

    @staticmethod
    def interpolateAndSum(tables):
        unit = tables[0].meta["unit"]
        prepared_tables = (t.interpolate().convert(unit) for t in tables)
        df = reduce(
            lambda x, y: x.add(y, fill_value=0.0),
            prepared_tables,
            next(prepared_tables),
        )
        df.meta["unit"] = unit
        return df

    def computeElectricityAndHeat(self, intermediateTables, addTable):
        mapping = self.mapping["COMPUTED"]

        entities = set()

        product_flow = defaultdict(dict)
        for table in intermediateTables:
            entity = table.meta["entity"][1:]
            product_flow[table.meta.get("category", "")][entity] = table
            entities.add(entity)

        # entities = set(entity for entity in entities if "MAIN" in entity)

        prefixed_entities = [
            f"{prefix}{entity}" for entity in entities for prefix in ["EL", "HE"]
        ]
        inv = dt.findp(source=self.IEA_WEB_SOURCE, entity=prefixed_entities).fillna(
            {"category": ""}
        )
        print(
            f"Computing electricity and heat shares for {', '.join(entities)} "
            f"with {', '.join(inv.entity.unique())} from {self.IEA_WEB_SOURCE}"
        )

        def getDefaultTable(prefix, product, entity):
            _inv = inv.loc[
                (inv.category == product) & (inv.entity == f"{prefix}{entity}")
            ]
            if _inv.empty:
                return None
            return dt.getTable(_inv.index.item())

        def getElectricityHeatShare(product, entity):
            el = getDefaultTable("EL", product, entity)
            he = getDefaultTable("HE", product, entity)
            if el is None and he is None:
                return None, None
            if el is None:
                return None, 1
            if he is None:
                return 1, None

            tot = el + he
            elshare = (el / tot).where(tot > 0, 0).convert("")
            heshare = (he / tot).where(tot > 0, 0).convert("")
            return elshare, heshare

        for product, flows in product_flow.items():
            electricity_list = []
            heat_list = []

            for entity, table in flows.items():
                # compute electricity and heat shares
                elshare, heshare = getElectricityHeatShare(product, entity)
                if elshare is not None:
                    electricity_list.append(elshare * table)
                if heshare is not None:
                    heat_list.append(heshare * table)

            if electricity_list:
                electricity = sum(electricity_list)
                unitTo = mapping.at["Electricity", "unitTo"]
                addTable(
                    electricity,
                    entity=mapping.at["Electricity", "mapping"],
                    category=product,
                    unit=electricity.meta["unit"],
                    unitTo=electricity.meta["unit"] if pd.isna(unitTo) else unitTo,
                )

            if heat_list:
                heat = sum(heat_list)
                unitTo = mapping.at["Heat", "unitTo"]
                addTable(
                    heat,
                    entity=mapping.at["Heat", "mapping"],
                    category=product,
                    unit=heat.meta["unit"],
                    unitTo=heat.meta["unit"] if pd.isna(unitTo) else unitTo,
                )

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        intermediateTables = []
        tablesToCommit = []
        excludedTables = dict(empty=[], error=[], exists=[])

        def addTable(dataframe, entity, category, unit, unitTo, **additionalMeta):
            metaDict = dict(
                source=self.setup.SOURCE_ID,
                entity=entity,
                category=category if category != "Total" else "",
                scenario="Historic",
                unit=unit,
                unitTo=unitTo,
                **additionalMeta,
            )

            metaDict = dt.core._update_meta(metaDict)
            tableID = dt.core._createDatabaseID(metaDict)

            if dataframe.empty:
                excludedTables['empty'].append(tableID)
                return

            dataTable = Datatable(pd.DataFrame(dataframe), meta=metaDict)
            # possible required unit conversion
            if 'unitTo' in metaDict:
                dataTable = dataTable.convert(metaDict['unitTo'])

            if entity.startswith("_"):
                intermediateTables.append(dataTable)
            elif not updateTables and dt.core.DB.tableExist(tableID):
                excludedTables['exists'].append(tableID)
            else:
                tablesToCommit.append(dataTable)

            return dataTable

        flow_map = self.mapping["FLOW (kt of CO2)"]
        product_map = self.mapping["PRODUCT"]

        for flow in flow_map.index:
            mask = self.data['FLOW (kt of CO2)'] == flow
            tempDataMo = self.data.loc[mask]

            flow_name, unit, unitTo = flow_map.loc[flow, ["mapping", "unit", "unitTo"]]

            product_tables = {}

            for product in product_map.index:
                mask = tempDataMo['PRODUCT'] == product
                tempDataMoSc = tempDataMo.loc[mask]
                product_name = product_map.loc[product, "mapping"]

                if tempDataMoSc.empty:
                    addTable(
                        tempDataMoSc,
                        entity=flow_name,
                        category=product_name,
                        unit=unit,
                        unitTo=unitTo,
                    )
                    continue

                dataframe = tempDataMoSc.set_index(self.setup.SPATIAL_COLUM_NAME)
                if spatialSubSet:
                    spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                        spatialSubSet
                    )
                    dataframe = tempDataMoSc.loc[spatIdx]

                dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                validSpatialRegions = self.spatialMapping.index[
                    ~self.spatialMapping.mapping.isnull()
                ]
                dataframe = dataframe.loc[validSpatialRegions, :]
                dataframe.index = self.spatialMapping.mapping[
                    ~self.spatialMapping.mapping.isnull()
                ]
                dataframe.columns = dataframe.columns.astype(int)

                dataTable = addTable(
                    dataframe,
                    entity=flow_name,
                    category=product_name,
                    unit=unit,
                    unitTo=unitTo,
                )

                product_tables[product] = dataTable

            aggregate_col = next(
                (col for col in product_map.columns if flow_name.startswith(col)),
                "aggregate",
            )
            aggregate = product_map.get(aggregate_col)
            if aggregate is not None:
                for agg_name in aggregate.dropna().unique():
                    components = [
                        c
                        for c in aggregate.index[aggregate == agg_name]
                        if c in product_tables
                    ]
                    df = self.interpolateAndSum([product_tables[c] for c in components])

                    addTable(
                        df,
                        entity=flow_name,
                        category=agg_name,
                        aggregation=", ".join(product_map.loc[components, "mapping"]),
                        unit=unit if pd.isna(unitTo) else unitTo,
                        unitTo=unitTo,
                    )

        self.computeElectricityAndHeat(intermediateTables, addTable)

        return tablesToCommit, excludedTables


class IEA_CO2_FUEL_DETAILED(BaseImportTool):
    """
    IEA World fuel emissions detail version
    """

    def __init__(self, year):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = f"IEA_CO2_FUEL_DETAILED_{year}"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, f'rawdata/IEA_CO2_FUEL_{year}'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, f'WOLRD_CO2_emissions_fuel_{year}_detailed.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'mapping_detailed.xlsx'
        )
        self.setup.LICENCE = 'restricted'
        self.setup.URL = 'https://www.iea.org/statistics/co2emissions/'

        self.setup.INDEX_COLUMN_NAME = ['FLOW (kt of CO2)', 'PRODUCT']
        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        self.setup.COLUMNS_TO_DROP = ['PRODUCT', 'FLOW (kt of CO2)', 'combined']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = self.loadVariableMapping()
            self.spatialMapping = self.loadSpatialMapping()

        self.createSourceMeta()

    def loadVariableMapping(
        self,
    ):
        mapping = dict()

        for variableName in self.setup.INDEX_COLUMN_NAME:
            mapping[variableName] = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=variableName, index_col=0
            )
            notNullIndex = mapping[variableName].index[
                ~mapping[variableName].isna().mapping
            ]
            mapping[variableName] = mapping[variableName].loc[notNullIndex]

        return mapping

    def loadSpatialMapping(
        self,
    ):
        return pd.read_excel(self.setup.MAPPING_FILE, sheet_name='spatial', index_col=0)

    def loadData(self):
        self.data = pd.read_csv(
            self.setup.DATA_FILE,
            encoding='utf8',
            engine='python',
            index_col=None,
            header=0,
            na_values=['..', 'c', 'x', 'nan'],
        )
        self.data['combined'] = self.data[self.setup.INDEX_COLUMN_NAME].apply(
            lambda x: '_'.join(x), axis=1
        )

        self.data = self.data.set_index(
            self.data['combined']
        )  # .drop('combined',axis=1)

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        for column in self.setup.INDEX_COLUMN_NAME:
            # models
            index = self.data.loc[:, column].unique()

            self.availableSeries = pd.DataFrame(index=index)
            self.mapping = pd.DataFrame(index=index, columns=['mapping'])

            self.mapping.to_excel(writer, engine='openpyxl', sheet_name=column)

        # spatial mapping
        column = self.setup.SPATIAL_COLUM_NAME

        index = self.data.loc[:, column].unique()

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['mapping'])

        for region in self.mapping.index:
            coISO = dt.mapp.getSpatialID(region)

            if coISO is not None:
                self.mapping.loc[region, 'mapping'] = coISO

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='spatial')

        writer.close()

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        for product in self.mapping['PRODUCT'].index:
            mask = self.data['PRODUCT'] == product
            tempDataMo = self.data.loc[mask]

            for flow in self.mapping['FLOW (kt of CO2)'].index:
                #                metaDict['scenario'] = scenario + '|' + model
                mask = tempDataMo['FLOW (kt of CO2)'] == flow
                tempDataMoSc = tempDataMo.loc[mask]

                metaDict['entity'] = '|'.join(
                    [
                        self.mapping['FLOW (kt of CO2)'].mapping.loc[flow],
                        self.mapping['PRODUCT'].mapping.loc[product],
                    ]
                )
                metaDict['scenario'] = 'Historic'
                metaDict['category'] = ''

                for key in ['unit', 'unitTo']:
                    metaDict[key] = self.mapping['FLOW (kt of CO2)'].loc[flow, key]

                # print(metaDict)
                tableID = dt.core._createDatabaseID(dt.core._update_meta(metaDict))
                # print(tableID)
                if not updateTables:
                    if dt.core.DB.tableExist(tableID):
                        excludedTables['exists'].append(tableID)
                        continue

                if len(tempDataMoSc.index) > 0:

                    dataframe = tempDataMoSc.set_index(self.setup.SPATIAL_COLUM_NAME)
                    if spatialSubSet:
                        spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                            spatialSubSet
                        )
                        dataframe = tempDataMoSc.loc[spatIdx]

                    dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                    dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                    validSpatialRegions = self.spatialMapping.index[
                        ~self.spatialMapping.mapping.isnull()
                    ]
                    dataframe = dataframe.loc[validSpatialRegions, :]
                    dataframe.index = self.spatialMapping.mapping[
                        ~self.spatialMapping.mapping.isnull()
                    ]

                    dataTable = Datatable(dataframe, meta=metaDict)
                    # possible required unit conversion
                    if not pd.isna(metaDict['unitTo']):
                        dataTable = dataTable.convert(metaDict['unitTo'])
                    tablesToCommit.append(dataTable)
                else:
                    excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class IEA_FUEL(BaseImportTool):
    """
    IEA World fuel emissions
    """

    def __init__(self, year):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = f"IEA_CO2_FUEL_{year}"
        self.setup.SOURCE_NAME = "IEA_CO2_FUEL"
        self.setup.SOURCE_YEAR = f"{year}"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, f'rawdata/IEA_CO2_FUEL_{year}'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, f'World_CO2_emissions_fuel_{year}.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = 'restricted'
        self.setup.URL = 'https://www.iea.org/statistics/co2emissions/'

        self.setup.INDEX_COLUMN_NAME = ['FLOW (kt of CO2)', 'PRODUCT']
        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        self.setup.COLUMNS_TO_DROP = ['PRODUCT', 'FLOW (kt of CO2)', 'combined']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET
            )
            self.spatialMapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=SPATIAL_MAPPING_SHEET
            )

        self.createSourceMeta()

    def loadData(self):
        self.data = pd.read_csv(
            self.setup.DATA_FILE,
            encoding='utf8',
            engine='python',
            index_col=None,
            header=0,
            na_values=['..', 'c', 'x', 'nan'],
        )
        self.data['combined'] = self.data[self.setup.INDEX_COLUMN_NAME].apply(
            lambda x: '_'.join(x), axis=1
        )

        self.data = self.data.set_index(
            self.data['combined']
        )  # .drop('combined',axis=1)

    def createVariableMapping(self):

        productMapping = {
            'Total': 'Total',
            'Coal, peat and oil shale': 'Coal_peat_oil_shale',
            'Oil': 'Oil',
            'Natural gas': 'Natural_gas',
            'Other': 'Other',
        }

        entity = "Emissions|CO2|Combustion"
        flowMapping = {
            'CO2 fuel combustion': 'Total',
            'Electricity and heat production': 'Electricity&heat',
            #                     'Other energy industry own use': 'Other_energy_industry_own_use',
            #                     'Manufacturing industries and construction': 'Manufacturing_industries_and_construction',
            'Transport': 'Transport',
            ' of which: road': 'Transports|Road',
            'Residential': 'Residential',
        }
        #                      'Commercial and public services': 'Commercial&public_services',
        #                     ' Agriculture/forestry': 'Agriculture/forestry',
        #                     ' Fishing': '_Fishing',
        #                     'Memo: International marine bunkers': 'Memo:_International_marine_bunkers',
        #                     'Memo: International aviation bunkers': 'Memo:_International_aviation_bunkers',
        #                     'Memo: Total final consumption': 'Memo:_Total_final_consumption'}

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        if not hasattr(self, 'data'):
            #            self.data = pd.read_csv(self.setup.DATA_FILE, encoding='utf8', engine='python', index_col = None, header =0, na_values='..')
            #            self.data['combined'] = self.data[self.setup.INDEX_COLUMN_NAME].apply(lambda x: '_'.join(x), axis=1)
            self.loadData()

        index = self.data['combined'].unique()

        # spatial mapping
        self.spatialMapping = dict()
        spatialIDList = self.data[self.setup.SPATIAL_COLUM_NAME].unique()
        from hdx.location.country import Country

        for spatID in spatialIDList:
            ISO_ID = dt.mapp.getSpatialID(spatID)
            ISO_ID = Country.get_iso3_country_code_fuzzy(spatID)[0]
            if ISO_ID is not None:
                self.spatialMapping[spatID] = ISO_ID
            else:
                print('not found: ' + spatID)

        # adding regions
        self.spatialMapping['World'] = "World"

        dataFrame = pd.DataFrame(data=[], columns=['alternative'])
        for key, item in self.spatialMapping.items():
            dataFrame.loc[key] = item
        dataFrame.to_excel(writer, sheet_name=SPATIAL_MAPPING_SHEET)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=MAPPING_COLUMNS)
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping.scenario = 'Historic'

        self.mapping['flow'] = self.mapping.index
        self.mapping['flow'] = self.mapping['flow'].apply(lambda x: x[0 : x.rfind('_')])
        self.mapping['product'] = self.mapping.index
        self.mapping['product'] = self.mapping['product'].apply(
            lambda x: x[x.rfind('_') + 1 :]
        )

        for key, value in flowMapping.items():
            mask = self.mapping['flow'].str.match(key)
            for pKey, pValue in productMapping.items():
                pMask = self.mapping['product'].str.match(pKey)
                self.mapping['entity'][mask & pMask] = '|'.join([value, pValue])
                self.mapping['unit'][mask & pMask] = 'kt CO2'
                self.mapping['unitTo'][mask & pMask] = 'Mt CO2'

        # self.mapping = self.mapping.drop(['product','flow'],axis=1)
        self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)
        writer.close()

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        indexDataToCollect = self.mapping.index[~pd.isnull(self.mapping['entity'])]

        tablesToCommit = []
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()
        for idx in indexDataToCollect:
            metaDf = self.mapping.loc[idx]
            print(idx)

            # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
            # print(metaData[self.setup.INDEX_COLUMN_NAME])
            metaDf['source_name'] = self.setup.SOURCE_NAME
            metaDf['source_year'] = self.setup.SOURCE_YEAR

            metaDict = {
                key: metaDf[key]
                for key in config.REQUIRED_META_FIELDS.union({"unit", "category"})
            }
            #            if pd.isna(metaDict['category']):
            #            metaDict['unit'] = metaDf['unit']
            metaDict['original code'] = idx
            # metaDict['original name'] = metaDf['Indicator Name']

            seriesIdx = idx

            print(metaDict)

            if not updateTables:
                tableID = dt.core._createDatabaseID(metaDict)
                print(tableID)
                if not updateTables:
                    if dt.core.DB.tableExist(tableID):
                        excludedTables['exists'].append(tableID)
                        continue

            dataframe = self.data.loc[seriesIdx]

            newData = list()
            for iRow in range(len(dataframe)):
                if dataframe.COUNTRY.iloc[iRow] in self.spatialMapping.index:
                    newData.append(
                        self.spatialMapping.alternative.loc[
                            dataframe.COUNTRY.iloc[iRow]
                        ]
                    )
                else:
                    newData.append(pd.np.nan)
            dataframe.loc[:, 'COUNTRY'] = newData

            if spatialSubSet:
                spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(spatialSubSet)
                dataframe = dataframe.loc[spatIdx]

            dataframe = dataframe.set_index(self.setup.SPATIAL_COLUM_NAME).drop(
                self.setup.COLUMNS_TO_DROP, axis=1
            )

            dataframe = dataframe.dropna(axis=1, how='all')
            dataframe = dataframe.loc[~pd.isna(dataframe.index)]
            dataTable = Datatable(dataframe, meta=metaDict)

            if not pd.isna(metaDf['unitTo']):
                dataTable = dataTable.convert(metaDf['unitTo'])

            tablesToCommit.append(dataTable)

        return tablesToCommit, excludedTables


#    def addSpatialMapping(self):
#        #EU
#        mappingToCountries  = dict()
#        EU_countryList = ['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN',
#       'FRA', 'GBR', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LIE', 'LTU', 'LUX',
#       'LVA', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE']
#        self.spatialMapping.loc['Memo: European Union-28'] = 'EU28'
#        mappingToCountries['EU28'] =  EU_countryList
#
#        LATMER_countryList = ['ABW', 'ARG', 'ATG', 'BHS', 'BLZ', 'BMU', 'BOL', 'BRA', 'BRB', 'COL',
#       'CRI', 'CUB', 'CYM', 'DMA', 'DOM', 'ECU', 'FLK', 'GLP', 'GRD', 'GTM',
#       'GUF', 'GUY', 'HND', 'HTI', 'JAM', 'KNA', 'LCA', 'MSR', 'MTQ', 'NIC',
#       'PAN', 'PER', 'PRI', 'PRY', 'SLV', 'SPM', 'SUR', 'TCA', 'TTO', 'URY',
#       'VCT', 'VEN', 'VGB']
#        self.spatialMapping.loc['Non-OECD Americas'] = 'LATAMER'
#        mappingToCountries['LATAMER'] =  LATMER_countryList
#
#        AFRICA_countryList = ['AGO', 'BDI', 'BEN', 'BFA', 'BWA', 'CAF', 'CIV', 'CMR', 'COD', 'COG',
#       'COM', 'CPV', 'DJI', 'DZA', 'EGY', 'ERI', 'ESH', 'ETH', 'GAB', 'GHA',
#       'GIN', 'GMB', 'GNB', 'GNQ', 'KEN', 'LBR', 'LBY', 'LSO', 'MAR', 'MDG',
#       'MLI', 'MOZ', 'MRT', 'MUS', 'MWI', 'NAM', 'NER', 'NGA', 'REU', 'RWA',
#       'SDN', 'SEN', 'SLE', 'SOM', 'SSD', 'STP', 'SWZ', 'SYC', 'TCD', 'TGO',
#       'TUN', 'TZA', 'UGA', 'ZAF', 'ZMB', 'ZWE']
#        self.spatialMapping.loc['Africa'] = 'AFRICA'
#        mappingToCountries['AFRICA'] =  AFRICA_countryList
#
#        MIDEAST_countryList = ['ARE', 'BHR', 'IRN', 'IRQ', 'JOR', 'KWT', 'LBN', 'OMN', 'QAT', 'SAU',
#       'SYR', 'YEM']
#        self.spatialMapping.loc['Middle East'] = 'MIDEAST'
#        mappingToCountries['MIDEAST'] =  MIDEAST_countryList
#
#        from hdx.location.country import Country
#        OECDTOT_countryList = [Country.get_iso3_country_code_fuzzy(x)[0] for x in 'Australia, Austria, Belgium, Canada, Chile, the Czech Republic, \
#         Denmark, Estonia, Finland, France, Germany, Greece, Hungary, Iceland, \
#         Ireland, Israel, Italy, Japan, Korea, Latvia, Lithuania , Luxembourg, \
#         Mexico, the Netherlands, New Zealand, Norway, Poland, Portugal, the Slovak Republic, \
#         Slovenia, Spain, Sweden, Switzerland, Turkey, the United Kingdom, United States'.split(',')]
#
#        self.spatialMapping.loc['Memo: OECD Total'] = 'OECDTOT'
#        mappingToCountries['OECDTOT'] =  OECDTOT_countryList
#        dt.mapp.regions.addRegionToContext('IEA',mappingToCountries)


class IEA_World_Energy_Balance(BaseImportTool):
    """
    IEA World balance data import
    """

    def __init__(self, year=2020, detailed=False):
        self.detailed = detailed

        self.setup = setupStruct()
        if not self.detailed:
            self.setup.SOURCE_ID = "IEA_WEB_" + str(year)
            self.setup.SOURCE_PATH = os.path.join(
                config.PATH_TO_DATASHELF, f'rawdata/IEA_WEB_{year}'
            )
            self.setup.DATA_FILE = os.path.join(
                self.setup.SOURCE_PATH, f'World_Energy_Balances_{year}_clean.csv'
            )
            self.setup.MAPPING_FILE = os.path.join(
                self.setup.SOURCE_PATH, 'mapping.xlsx'
            )
        else:
            self.setup.SOURCE_ID = "IEA_WEB_DETAILED_" + str(year)
            self.setup.SOURCE_PATH = os.path.join(
                config.PATH_TO_DATASHELF, f'rawdata/IEA_WEB_{year}'
            )
            self.setup.DATA_FILE = os.path.join(
                self.setup.SOURCE_PATH, f'WEB_{year}_detailed.csv'
            )
            self.setup.MAPPING_FILE = os.path.join(
                self.setup.SOURCE_PATH, 'mapping_detailed.xlsx'
            )

        self.setup.LICENCE = 'restricted'
        self.setup.URL = 'https://webstore.iea.org/world-energy-balances-' + str(year)

        self.setup.INDEX_COLUMN_NAME = ['FLOW', 'PRODUCT']
        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        self.setup.COLUMNS_TO_DROP = ['PRODUCT', 'FLOW']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = self.loadVariableMapping()
            self.spatialMapping = self.loadSpatialMapping()

        self.createSourceMeta()

    def loadData(self):
        if not self.detailed:
            self.data = pd.read_csv(
                self.setup.DATA_FILE,
                encoding='utf8',
                engine='python',
                index_col=None,
                header=0,
                na_values=['x', 'c', '..'],
                sep=';',
            )
        else:
            cols = pd.read_csv(
                self.setup.DATA_FILE,
                encoding="ISO-8859-1",
                header=0,
                skiprows=1,
                nrows=0,
            ).columns
            self.data = pd.read_csv(
                self.setup.DATA_FILE,
                encoding="ISO-8859-1",
                header=0,
                skiprows=[1],
                na_values=["x", "..", "c"],
            )
            self.data.columns = cols[:3].append(self.data.columns[3:])

    def loadSpatialMapping(
        self,
    ):
        return pd.read_excel(self.setup.MAPPING_FILE, sheet_name='spatial', index_col=0)

    def loadVariableMapping(
        self,
    ):
        mapping = dict()

        for variableName in self.setup.INDEX_COLUMN_NAME:
            mapping[variableName] = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=variableName, index_col=0
            ).dropna(subset=["mapping"])

        return mapping

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        for column in self.setup.INDEX_COLUMN_NAME:
            # models
            index = self.data.loc[:, column].unique()

            self.availableSeries = pd.DataFrame(index=index)
            self.mapping = pd.DataFrame(index=index, columns=['mapping'])

            self.mapping.to_excel(writer, sheet_name=column)

        # spatial mapping
        column = self.setup.SPATIAL_COLUM_NAME

        index = self.data.loc[:, column].unique()

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['mapping'])

        for region in self.mapping.index:
            coISO = dt.mapp.getSpatialID(region)

            if coISO is not None:
                self.mapping.loc[region, 'mapping'] = coISO

        self.mapping.to_excel(writer, sheet_name='spatial')

        writer.close()

    def gatherMappedData(
        self, spatialSubSet=None, updateTables=False, onlyAggregations=False
    ):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        flow_map = self.mapping["FLOW"]
        product_map = self.mapping["PRODUCT"]

        for flow in flow_map.index:
            mask = self.data['FLOW'] == flow
            tempDataMo = self.data.loc[mask]

            flow_name, unit, unitTo = self.mapping['FLOW'].loc[
                flow, ["mapping", "unit", "unitTo"]
            ]

            product_tables = {}

            for product in product_map.index:
                mask = tempDataMo['PRODUCT'] == product
                tempDataMoSc = tempDataMo.loc[mask]

                product_name = product_map.loc[product, "mapping"]
                metaDict = dict(
                    source=self.setup.SOURCE_ID,
                    entity=flow_name,
                    category=product_name if product_name != "Total" else "",
                    scenario="Historic",
                    unit=unit,
                    unitTo=unitTo,
                )

                metaDict = dt.core._update_meta(metaDict)
                tableID = dt.core._createDatabaseID(metaDict)

                if len(tempDataMoSc.index) == 0:
                    excludedTables['empty'].append(tableID)
                    continue

                dataframe = tempDataMoSc.set_index(self.setup.SPATIAL_COLUM_NAME)
                if spatialSubSet:
                    spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                        spatialSubSet
                    )
                    dataframe = tempDataMoSc.loc[spatIdx]

                dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                validSpatialRegions = self.spatialMapping.index[
                    ~self.spatialMapping.mapping.isnull()
                ]
                dataframe = dataframe.loc[validSpatialRegions, :]
                dataframe.index = self.spatialMapping.mapping[
                    ~self.spatialMapping.mapping.isnull()
                ]
                dataframe.columns = dataframe.columns.astype(int)

                dataTable = Datatable(dataframe, meta=metaDict)
                # possible required unit conversion
                if 'unitTo' in metaDict:
                    dataTable = dataTable.convert(metaDict['unitTo'])

                product_tables[product] = dataTable

                if not updateTables and dt.core.DB.tableExist(tableID):
                    excludedTables['exists'].append(tableID)
                    continue

                if not onlyAggregations:
                    tablesToCommit.append(dataTable)

            aggregate_col = next(
                (col for col in product_map.columns if flow_name.startswith(col)),
                "aggregate",
            )
            aggregate = product_map.get(aggregate_col)
            removes = None
            if aggregate is not None:
                for agg_name in aggregate.dropna().unique():
                    components = [
                        c
                        for c in aggregate.index[aggregate == agg_name]
                        if c in product_tables
                    ]
                    interpolated_tables = (
                        product_tables[c].interpolate() for c in components
                    )
                    df = reduce(
                        lambda x, y: x.add(y, fill_value=0.0),
                        interpolated_tables,
                        next(interpolated_tables),
                    )

                    if agg_name == "__remove__":
                        removes = df
                        continue
                    elif agg_name == "Total":
                        if removes is not None:
                            df = df.sub(removes, fill_value=0.0)

                    metaDict = dict(
                        source=self.setup.SOURCE_ID,
                        entity=flow_name,
                        category=agg_name if agg_name != "Total" else "",
                        scenario="Historic",
                        aggregation=", ".join(product_map.loc[components, "mapping"]),
                        unit=unit if pd.isna(unitTo) else unitTo,
                        unitTo=unitTo,
                    )

                    metaDict = dt.core._update_meta(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    if not updateTables and dt.core.DB.tableExist(tableID):
                        excludedTables['exists'].append(tableID)
                    else:
                        dataTable = dt.Datatable(pd.DataFrame(df), meta=metaDict)
                        tablesToCommit.append(dataTable)

        return tablesToCommit, excludedTables


class ADVANCE_DB(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "ADVANCE_2016"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/ADVANCE_DB'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH,
            'ADVANCE_Synthesis_version101_compare_20190619-143200.csv',
        )
        # self.setup.META_FILE    = self.setup.SOURCE_PATH + 'sr15_metadata_indicators_r1.1.xlsx'
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = ' CC-BY 4.0'
        self.setup.URL = (
            'https://db1.ene.iiasa.ac.at/ADVANCEDB/dsd?Action=htmlpage&page=about'
        )

        self.setup.VARIABLE_COLUMN_NAME = ['VARIABLE']
        self.setup.MODEL_COLUMN_NAME = ['MODEL']
        self.setup.SCENARIO_COLUMN_NAME = ['SCENARIO']

        self.setup.SPATIAL_COLUM_NAME = ['REGION']
        self.setup.COLUMNS_TO_DROP = ["MODEL", "SCENARIO", "VARIABLE", "UNIT"]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping'
        ).set_index('model')
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping'
        ).set_index('scenario')
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    def createVariableMapping(self):
        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates(
            self.setup.VARIABLE_COLUMN_NAME
        ).set_index(self.setup.VARIABLE_COLUMN_NAME)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=MAPPING_COLUMNS
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping = self.mapping.loc[:, MAPPING_COLUMNS]
        self.mapping.unit = self.availableSeries.UNIT
        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name=VAR_MAPPING_SHEET,
            index_label="original variable",
        )

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=self.setup.MODEL_COLUMN_NAME)
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='model_mapping',
            index_label="original model",
        )

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=self.setup.SCENARIO_COLUMN_NAME
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='scenario_mapping',
            index_label="original scenario",
        )
        writer.close()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        self.createSourceMeta()
        #        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()
        for model in self.mappingModel.index:
            mask = self.data['MODEL'] == self.mappingModel.loc[model]['original_model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = scenario + '|' + model
                mask = (
                    tempDataMo['SCENARIO']
                    == self.mappingScenario.loc[scenario]['original_scenario']
                )
                tempDataMoSc = tempDataMo.loc[mask]

                for entity in self.mappingEntity.index:
                    metaDf = self.mappingEntity.loc[entity]
                    metaDict['entity'] = self.mappingEntity.loc[entity]['entity']
                    metaDict['model'] = model

                    for key in ['category', 'unit', 'unitTo']:
                        metaDict[key] = metaDf[key]
                    if pd.isnull(metaDict['category']):
                        metaDict['category'] = ''
                    tableID = dt.core._createDatabaseID(metaDict)
                    print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['VARIABLE'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        dataTable = Datatable(dataframe, meta=metaDict)

                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])

                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class AR5_DATABASE(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "AR5_DB"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/AR5_database'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH,
            'ar5_public_version102_compare_compare_20150629-130000.csv',
        )
        # self.setup.META_FILE    = self.setup.SOURCE_PATH + 'sr15_metadata_indicators_r1.1.xlsx'
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = ' CC-BY 4.0'
        self.setup.URL = (
            'https://db1.ene.iiasa.ac.at/ADVANCEDB/dsd?Action=htmlpage&page=about'
        )

        self.setup.VARIABLE_COLUMN_NAME = ['VARIABLE']
        self.setup.MODEL_COLUMN_NAME = ['MODEL']
        self.setup.SCENARIO_COLUMN_NAME = ['SCENARIO']

        self.setup.SPATIAL_COLUM_NAME = ['REGION']
        self.setup.COLUMNS_TO_DROP = ["MODEL", "SCENARIO", "VARIABLE", "UNIT"]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping'
        ).set_index('model')
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping'
        ).set_index('scenario')
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    def createVariableMapping(self):
        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates(
            self.setup.VARIABLE_COLUMN_NAME
        ).set_index(self.setup.VARIABLE_COLUMN_NAME)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=MAPPING_COLUMNS
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping = self.mapping.loc[:, MAPPING_COLUMNS]
        self.mapping.unit = self.availableSeries.UNIT
        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name=VAR_MAPPING_SHEET,
            index_label="original variable",
        )

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=self.setup.MODEL_COLUMN_NAME)
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='model_mapping',
            index_label="original model",
        )

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=self.setup.SCENARIO_COLUMN_NAME
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='scenario_mapping',
            index_label="original scenario",
        )
        writer.close()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        self.createSourceMeta()
        #        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()
        for model in self.mappingModel.index:
            mask = self.data['MODEL'] == self.mappingModel.loc[model]['original_model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = scenario + '|' + model
                mask = (
                    tempDataMo['SCENARIO']
                    == self.mappingScenario.loc[scenario]['original_scenario']
                )
                tempDataMoSc = tempDataMo.loc[mask]

                for entity in self.mappingEntity.index:
                    metaDf = self.mappingEntity.loc[entity]
                    metaDict['entity'] = self.mappingEntity.loc[entity]['entity']
                    metaDict['model'] = model
                    metaDict['unitTo'] = self.mappingEntity.loc[entity]['unitTo']

                    for key in ['category', 'unit']:
                        metaDict[key] = metaDf[key]
                    metaDict = dt.core._update_meta(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['VARIABLE'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        dataTable = Datatable(dataframe, meta=metaDict)

                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])

                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class IAMC15_2019(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "IAMC15_2019_R2"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/IAMC15_2019b'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'iamc15_scenario_data_all_regions_r2.0.xlsx'
        )
        self.setup.META_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'sr15_metadata_indicators_r2.0.xlsx'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = ' CC-BY 4.0'
        self.setup.URL = 'https://data.ene.iiasa.ac.at/iamc-1.5c-explorer'

        self.setup.VARIABLE_COLUMN_NAME = ['Variable']
        self.setup.MODEL_COLUMN_NAME = ['Model']
        self.setup.SCENARIO_COLUMN_NAME = ['Scenario']

        self.setup.SPATIAL_COLUM_NAME = ['Region']
        self.setup.COLUMNS_TO_DROP = [
            'Model',
            'Scenario',
            'Variable',
            'Unit',
            'combined',
        ]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()

        self.metaList = [
            'climate_category',
            'baseline',
            'marker',
            'project',
            'median warming at peak (MAGICC6)',
            'year of peak warming (MAGICC6)',
            'cumulative CO2 emissions (2016-2100, Gt CO2)',
            'cumulative CCS (2016-2100, Gt CO2)',
            'cumulative sequestration land-use (2016-2100, Gt CO2)',
            'year of netzero CO2 emissions',
        ]

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping'
        ).set_index('model')
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping'
        ).set_index('scenario')
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    def createSourceMeta(self):
        self.meta = {
            'SOURCE_ID': self.setup.SOURCE_ID,
            'collected_by': config.CRUNCHER,
            'date': dt.core.getDateString(),
            'source_url': self.setup.URL,
            'licence': self.setup.LICENCE,
        }

    def loadData(self):
        self.data = pd.read_excel(
            self.setup.DATA_FILE,
            sheet_name='data',
            index_col=None,
            header=0,
            na_values='..',
        )
        self.data['combined'] = self.data[['Scenario', 'Model']].apply(
            lambda x: '|'.join(x), axis=1
        )
        self.data['combined'] = (
            self.data['combined']
            .apply(lambda x: x.replace(' ', '_'))
            .apply(lambda x: x.replace('/', '_'))
        )

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates('Variable').set_index(
            'Variable'
        )['Unit']
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=MAPPING_COLUMNS
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['model'])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # models
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['model'])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')
        writer.close()

    def loadMetaData(self):
        # meta data
        self.createSourceMeta()
        self.metaDataDf = pd.read_excel(
            self.setup.META_FILE, header=0, sheet_name='meta'
        )
        self.metaDataDf['climate_category'] = self.metaDataDf['category']
        self.metaDataDf['combined'] = self.metaDataDf[['scenario', 'model']].apply(
            lambda x: '|'.join(x), axis=1
        )
        self.metaDataDf['combined'] = (
            self.metaDataDf['combined']
            .apply(lambda x: x.replace(' ', '_'))
            .apply(lambda x: x.replace('/', '_'))
        )
        self.metaDataDf = self.metaDataDf.set_index(self.metaDataDf['combined'])

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        import tqdm

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        # meta data
        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        availableCombitions = list(self.data.combined)
        #        for scenModel in tqdm.tqdm(self.data.combined.unique()):
        #            mask = self.data['combined'] == scenModel
        #            tempDataMoSc = self.data.loc[mask,:]
        #            metaDict['scenario'] = tempDataMoSc.loc[:,'Scenario'].iloc[0] + '|' + tempDataMoSc.loc[:,'Model'].iloc[0]
        #            metaDict['model'] = tempDataMoSc.loc[:,'Model'].iloc[0]
        for model in self.mappingModel.index:
            mask = self.data['Model'] == self.mappingModel.loc[model]['original_model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = scenario + '|' + model
                mask = (
                    tempDataMo['Scenario']
                    == self.mappingScenario.loc[scenario]['original_scenario']
                )
                tempDataMoSc = tempDataMo.loc[mask]

                if metaDict['scenario'] not in availableCombitions:
                    continue

                #            if True:

                #                for metaTag in self.metaList:
                #                    try:
                #                        metaDict[metaTag] = self.metaDataDf.loc[scenModel, metaTag]
                #
                #                    except:
                #                        pass

                for entity in self.mappingEntity.index:
                    metaDf = self.mappingEntity.loc[entity]
                    metaDict['entity'] = self.mappingEntity.loc[entity]['entity']
                    metaDict['model'] = model

                    for key in ['category', 'unit', 'unitTo']:
                        metaDict[key] = metaDf[key]
                    if pd.isna(metaDict['category']):
                        metaDict['category'] = ''
                    # print(metaDict)
                    metaDict = dt.core._update_meta(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    # print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['Variable'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        dataTable = Datatable(dataframe, meta=metaDict)
                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])
                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class CDLINKS_2018(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "CDLINKS_2018"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/CDLINKS_2018'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH,
            'cdlinks_public_version101_compare__20181010-142000.csv',
        )
        #        self.setup.META_FILE    = self.setup.SOURCE_PATH + 'sr15_metadata_indicators_r2.0.xlsx'
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = ' CC-BY 4.0'
        self.setup.URL = 'https://db1.ene.iiasa.ac.at/CDLINKSDB'

        self.setup.VARIABLE_COLUMN_NAME = ['VARIABLE']
        self.setup.MODEL_COLUMN_NAME = ['MODEL']
        self.setup.SCENARIO_COLUMN_NAME = ['SCENARIO']

        self.setup.SPATIAL_COLUM_NAME = ['REGION']
        self.setup.COLUMNS_TO_DROP = ["MODEL", "SCENARIO", "VARIABLE", "UNIT"]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping'
        ).set_index('model')
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping'
        ).set_index('scenario')
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    def createVariableMapping(self):
        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates(
            self.setup.VARIABLE_COLUMN_NAME
        ).set_index(self.setup.VARIABLE_COLUMN_NAME)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=MAPPING_COLUMNS
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping = self.mapping.loc[:, MAPPING_COLUMNS]
        self.mapping.unit = self.availableSeries.UNIT
        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name=VAR_MAPPING_SHEET,
            index_label="original variable",
        )

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=self.setup.MODEL_COLUMN_NAME)
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='model_mapping',
            index_label="original model",
        )

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=self.setup.SCENARIO_COLUMN_NAME
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='scenario_mapping',
            index_label="original scenario",
        )
        writer.close()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        self.createSourceMeta()
        #        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()
        for model in self.mappingModel.index:
            mask = self.data['MODEL'] == self.mappingModel.loc[model]['original model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = scenario + '|' + model
                mask = (
                    tempDataMo['SCENARIO']
                    == self.mappingScenario.loc[scenario]['original scenario']
                )
                tempDataMoSc = tempDataMo.loc[mask]

                for entity in self.mappingEntity.index:
                    metaDf = self.mappingEntity.loc[entity]
                    metaDict['entity'] = self.mappingEntity.loc[entity]['entity']
                    metaDict['model'] = model

                    for key in ['category', 'unit', 'unitTo']:
                        metaDict[key] = metaDf[key]
                    if pd.isnull(metaDict['category']):
                        metaDict['category'] = ''
                    metaDict = dt.core._update_meta(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['VARIABLE'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        dataTable = Datatable(dataframe, meta=metaDict)

                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])

                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class IAMC_CMIP6(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "IAMC_CMIP6_2020"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata', self.setup.SOURCE_ID
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'SSP_CMIP6_201811.csv'
        )
        #        self.setup.META_FILE    = self.setup.SOURCE_PATH + 'sr15_metadata_indicators_r2.0.xlsx'
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = ' CC-BY 4.0'
        self.setup.URL = 'https://tntcat.iiasa.ac.at/SspDb/dsd?Action=htmlpage&page=50'

        #        self.setup.VARIABLE_COLUMN_NAME = ['VARIABLE']
        #        self.setup.MODEL_COLUMN_NAME = ['MODEL']
        #        self.setup.SCENARIO_COLUMN_NAME = ['SCENARIO']

        self.entryMapping = {
            'VARIABLE': ['entity', 'category', 'unit', 'unitTo'],
            'MODEL': ['model'],
            'SCENARIO': ['scenario'],
        }
        self.setup.SPATIAL_COLUM_NAME = ['REGION']
        self.setup.COLUMNS_TO_DROP = ["MODEL", "SCENARIO", "VARIABLE", "UNIT"]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping', index_col=0
        )
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping', index_col=0
        )
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    def createVariableMapping(self):
        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        entry = 'VARIABLE'
        self.availableSeries = self.data.drop_duplicates([entry]).set_index(entry)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=self.entryMapping[entry]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping = self.mapping.loc[:, self.entryMapping[entry]]

        xx = self.mapping.index.map(
            lambda x: str(x).replace(' ', '_').replace('CMIP6_', '')
        )
        self.mapping.entity = xx.map(lambda x: '|'.join(str(x).split('|')[:2]))
        self.mapping.category = xx.map(lambda x: '|'.join(str(x).split('|')[2:]))

        self.mapping.unit = self.availableSeries.UNIT
        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name=VAR_MAPPING_SHEET,
            index_label="original variable",
        )

        # models
        entry = 'MODEL'
        self.availableSeries = self.data.drop_duplicates([entry]).set_index(entry)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=self.entryMapping[entry]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping = self.mapping.loc[:, self.entryMapping[entry]]

        xx = self.mapping.index.map(
            lambda x: str(x).replace(' ', '_').replace('/', '_')
        )
        self.mapping.model = xx
        #        self.mapping.category = xx.map(lambda x : '|'.join(str(x).split('|')[2:]))

        #        self.mapping.unit = self.availableSeries.UNIT
        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='model_mapping',
            index_label="original model",
        )

        # scenarios
        entry = 'SCENARIO'
        self.availableSeries = self.data.drop_duplicates([entry]).set_index(entry)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=self.entryMapping[entry]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping = self.mapping.loc[:, self.entryMapping[entry]]
        xx = self.mapping.index.map(
            lambda x: str(x)
            .replace(' ', '_')
            .replace('/', '_')
            .replace('(', '')
            .replace(')', '')
        )
        self.mapping.scenario = xx

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='scenario_mapping',
            index_label="original scenario",
        )

        writer.close()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        self.createSourceMeta()
        #        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()

        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        for model in self.mappingModel.index:
            mask = self.data['MODEL'] == model
            metaDict['model'] = self.mappingModel.loc[model, 'model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = self.mappingScenario.loc[scenario, 'scenario']
                mask = tempDataMo['SCENARIO'] == scenario
                tempDataMoSc = tempDataMo.loc[mask]

                for entity in self.mappingEntity.index:

                    for key in ['entity', 'category', 'unit', 'unitTo']:
                        metaDict[key] = self.mappingEntity.loc[entity, key]

                    if pd.isnull(metaDict['category']):
                        metaDict['category'] = ''
                    metaDict = dt.core._update_meta(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['VARIABLE'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        dataTable = Datatable(dataframe, meta=metaDict)

                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])

                        if 'unitTo' in dataTable.meta.keys():
                            del dataTable.meta['unitTo']
                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class AIM_SSP_DATA_2019(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "AIM_SSPx_DATA_2019"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/AIM_SSP_scenarios'
        )
        self.setup.DATA_FILE = [
            os.path.join(self.setup.SOURCE_PATH, 'data/ssp' + str(x) + '.csv')
            for x in range(1, 5)
        ]
        # self.setup.META_FILE    = self.setup.SOURCE_PATH + 'sr15_metadata_indicators_r2.0.xlsx'
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = ''
        self.setup.URL = 'https://github.com/JGCRI/ssp-data'

        self.setup.VARIABLE_COLUMN_NAME = ['Variable']
        self.setup.MODEL_COLUMN_NAME = ['model']
        self.setup.SCENARIO_COLUMN_NAME = ['scenario']

        self.setup.SPATIAL_COLUM_NAME = 'region'
        self.setup.COLUMNS_TO_DROP = ['model', 'scenario', 'Variable', 'Unit']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()
            self.spatialMapping = self.loadSpatialMapping()

    def loadSpatialMapping(
        self,
    ):
        return pd.read_excel(self.setup.MAPPING_FILE, sheet_name='spatial_mapping')

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping'
        ).set_index('model')
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping'
        ).set_index('scenario')
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    #    def createSourceMeta(self):
    #        self.meta = {'SOURCE_ID': self.setup.SOURCE_ID,
    #                      'collected_by' : config.CRUNCHER,
    #                      'date': dt.core.getDateString(),
    #                      'source_url' : self.setup.URL,
    #                      'licence': self.setup.LICENCE }

    def loadData(self):
        datafiles = list()
        for file in self.setup.DATA_FILE:

            datafiles.append(pd.read_csv(file, index_col=0, header=0, na_values='..'))
        self.data = pd.concat(datafiles)
        newColumns = [x.replace('X', '') for x in self.data.columns]
        self.data.columns = newColumns

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates('Variable').set_index(
            'Variable'
        )['Unit']
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=MAPPING_COLUMNS
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['model'])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # models
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['model'])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')

        # spatial mapping
        column = self.setup.SPATIAL_COLUM_NAME

        index = self.data.loc[:, column].unique()

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['mapping'])

        for region in self.mapping.index:
            coISO = dt.mapp.getSpatialID(region)

            if coISO is not None:
                self.mapping.loc[region, 'mapping'] = coISO

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='spatial')

        writer.close()

    #    def loadMetaData(self):
    # meta data
    #        self.createSourceMeta()
    #        self.metaDataDf = pd.read_excel(self.setup.META_FILE, header=0, sheet_name='meta')
    #        self.metaDataDf['combined'] = self.metaDataDf[['model','scenario']].apply(lambda x: '_'.join(x), axis=1)
    #
    #        self.metaDataDf = self.metaDataDf.set_index(self.metaDataDf['combined'])

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()
        for model in self.mappingModel.index:
            mask = self.data['model'] == self.mappingModel.loc[model]['original_model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = scenario + '|' + model
                mask = (
                    tempDataMo['scenario']
                    == self.mappingScenario.loc[scenario]['original_scenario']
                )
                tempDataMoSc = tempDataMo.loc[mask]

                for entity in self.mappingEntity.index:
                    metaDf = self.mappingEntity.loc[entity]
                    metaDict['entity'] = self.mappingEntity.loc[entity]['entity']
                    metaDict['model'] = model

                    for key in ['category', 'unit', 'unitTo']:
                        metaDict[key] = metaDf[key]
                    if pd.isna(metaDict['category']):
                        metaDict['category'] = ''
                    # print(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    # print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['Variable'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        # spatial
                        validSpatialRegions = self.spatialMapping.index[
                            ~self.spatialMapping.mapping.isnull()
                        ]
                        dataframe = dataframe.loc[validSpatialRegions, :]
                        dataframe.index = self.spatialMapping.mapping[
                            ~self.spatialMapping.mapping.isnull()
                        ]

                        dataTable = Datatable(dataframe, meta=metaDict)
                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])
                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class IRENA2019(BaseImportTool):

    """
    IRENA data import tool
    """

    def __init__(self, year=2019):

        self.setup = setupStruct()
        self.setup.SOURCE_ID = "IRENA_" + str(year)
        self.setup.year = year
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/IRENA_' + str(year)
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'IRENA_RE_electricity_statistics.xlsx'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = 'open source'
        self.setup.URL = 'http://www.irena.org/IRENADocuments/IRENA_RE_electricity_statistics_-_Query_tool.xlsm'

        #        self.setup.INDEX_COLUMN_NAME = ['FLOW', 'PRODUCT']
        #        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        #        self.setup.COLUMNS_TO_DROP = [ 'PRODUCT','FLOW','combined']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET
            )
            self.spatialMapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=SPATIAL_MAPPING_SHEET
            )

        self.createSourceMeta()

    def loadData(self):

        # from datatoolbox.io import ExcelReader
        setup = dict()
        setup['filePath'] = self.setup.SOURCE_PATH
        setup['fileName'] = 'IRENA_RE_electricity_statistics.xlsx'

        self.dataDf = pd.DataFrame([], columns=['meta', 'dataDf'])

        sheetList = list(range(1, 4)) + list(range(6, 22))
        for sheetNum in sheetList:

            # Capacity
            setup['sheetName'] = str(sheetNum)
            setup['timeIdxList'] = ('B5', 'AL5')
            setup['spaceIdxList'] = ('A7', 'A198')
            ex = dt.io.ExcelReader(setup)
            df = ex.gatherData()
            df = df.iloc[:, ~pd.isna(df.columns)]
            df = df.iloc[~pd.isna(df.index), :]
            df.columns = df.columns.astype(int)
            entity = 'Capacity ' + ex.gatherValue('A1')
            unit = ex.gatherValue('A5')
            if "MW" in unit:
                unit = "MW"
            elif "GWh" in unit:
                unit = "GWh"

            metaDict = {'unit': unit, 'entity': entity}
            self.dataDf.loc[entity] = [metaDict, df]

            # Production
            setup['timeIdxList'] = ('AO5', 'BU5')
            setup['spaceIdxList'] = ('A7', 'A198')
            ex = dt.io.ExcelReader(setup)
            df = ex.gatherData()
            df = df.iloc[:, ~pd.isna(df.columns)]
            df = df.iloc[~pd.isna(df.index), :]
            df.columns = df.columns.astype(int)
            entity = 'Production ' + ex.gatherValue('A1')
            unit = ex.gatherValue('A5')
            if "MW" in unit:
                unit = "MW"
            elif "GWh" in unit:
                unit = "GWh"

            metaDict = {'unit': unit, 'entity': entity}
            self.dataDf.loc[entity] = [metaDict, df]

    #        self.data = pd.read_csv(self.setup.DATA_FILE, encoding='utf8', engine='python', index_col = None, header =0, na_values='c')
    #        self.data['combined'] = self.data[self.setup.INDEX_COLUMN_NAME].apply(lambda x: '_'.join(x), axis=1)
    #
    #        self.data = self.data.set_index(self.data['combined'])#.drop('combined',axis=1)

    def createVariableMapping(self):

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variable mapping
        self.mapping = pd.DataFrame(
            [], columns=MAPPING_COLUMNS, index=self.dataDf.index.sort_values()
        )

        self.mapping.source = self.setup.SOURCE_ID
        self.mapping.scenario = 'historic'
        self.mapping.entity = self.mapping.index.str.split(' ').str.join('_')
        self.mapping.category = ''

        for idx in self.mapping.index:
            self.mapping.loc[idx, ['unit']] = self.dataDf.loc[idx, ['meta']][0]['unit']

        self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)

        # spatial mapping
        self.spatialMapping = dict()
        spatialIDList = list()
        for i in range(len(self.dataDf)):
            spatialIDList = spatialIDList + list(self.dataDf.iloc[i, 1].index.unique())
        spatialIDList = list(set(spatialIDList))
        for spatID in spatialIDList:
            ISO_ID = dt.getCountryISO(spatID)
            if ISO_ID is not None:
                self.spatialMapping[spatID] = ISO_ID
            else:
                print('not found: ' + spatID)

        # adding regions
        self.spatialMapping['World'] = "World"
        self.spatialMapping['UK'] = "GBR"
        self.spatialMapping['European Union'] = "EU28"

        dataFrame = pd.DataFrame(data=[], columns=['alternative'])
        for key, item in self.spatialMapping.items():
            dataFrame.loc[key] = item
        dataFrame.to_excel(writer, sheet_name=SPATIAL_MAPPING_SHEET)
        self.spatialMapping = dataFrame
        writer.close()

    def gatherMappedData(self, spatialSubSet=None):
        # loading data if necessary
        if not hasattr(self, 'dataDf'):
            self.loadData()

        indexDataToCollect = self.mapping.index[~pd.isnull(self.mapping['entity'])]

        tablesToCommit = []
        for idx in indexDataToCollect:
            metaDf = self.mapping.loc[idx]
            print(idx)

            # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
            # print(metaData[self.setup.INDEX_COLUMN_NAME])

            metaDict = {
                key: metaDf[key]
                for key in {'entity', 'scenario', 'source_name', 'unit'}
            }
            metaDict['source_year'] = self.setup.year
            metaDict['original code'] = idx
            metaDict['unitTo'] = metaDf['unitTo']
            # metaDict['original name'] = metaDf['Indicator Name']

            seriesIdx = idx

            dataframe = self.dataDf.loc[seriesIdx]['dataDf'].copy()
            dataframe = dataframe.astype(float)

            newData = list()
            for iRow in range(len(dataframe)):
                if dataframe.index[iRow] in self.spatialMapping.index:
                    newData.append(
                        self.spatialMapping.alternative.loc[dataframe.index[iRow]]
                    )
                else:
                    newData.append(pd.np.nan)
                    print('ignoring: ' + dataframe.index[iRow])
            dataframe.index = newData

            if spatialSubSet:
                spatIdx = dataframe.index.isin(spatialSubSet)
                dataframe = dataframe.loc[spatIdx]

            dataframe = dataframe.dropna(axis=1, how='all')
            dataframe = dataframe.loc[~pd.isna(dataframe.index)]
            dataTable = Datatable(dataframe, meta=metaDict)
            # possible required unit conversion
            if not pd.isna(metaDict['unitTo']):
                dataTable = dataTable.convert(metaDict['unitTo'])
            tablesToCommit.append(dataTable)

        return tablesToCommit


class SSP_DATA(BaseImportTool):
    def __init__(self):

        self.setup = setupStruct()

        self.setup.SOURCE_ID = "SSP_DB_2013"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/SSP_DB'
        )
        self.setup.DATA_FILE = [
            os.path.join(self.setup.SOURCE_PATH, 'SspDb_country_data_2013-06-12.csv'),
            os.path.join(
                self.setup.SOURCE_PATH, 'SspDb_compare_regions_2013-06-12.csv'
            ),
        ]
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = 'open access'
        self.setup.URL = 'tntcat.iiasa.ac.at/SspWorkDb'

        self.setup.VARIABLE_COLUMN_NAME = ['VARIABLE']
        self.setup.MODEL_COLUMN_NAME = ['MODEL']
        self.setup.SCENARIO_COLUMN_NAME = ['Scenario']

        self.setup.SPATIAL_COLUM_NAME = ['REGION']
        self.setup.COLUMNS_TO_DROP = ["MODEL", "SCENARIO", "VARIABLE", "UNIT"]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.loadMapping()

        self.createSourceMeta()

    def createSourceMeta(self):
        self.meta = {
            'SOURCE_ID': self.setup.SOURCE_ID,
            'collected_by': config.CRUNCHER,
            'date': dt.core.getDateString(),
            'source_url': self.setup.URL,
            'licence': self.setup.LICENCE,
        }

    def loadMapping(
        self,
    ):
        self.mappingEntity = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
        )
        self.mappingEntity = self.mappingEntity.loc[self.mappingEntity.entity.notnull()]

        self.mappingModel = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='model_mapping'
        ).set_index('model')
        self.mappingModel = self.mappingModel.loc[self.mappingModel.index.notnull()]

        self.mappingScenario = pd.read_excel(
            self.setup.MAPPING_FILE, sheet_name='scenario_mapping'
        ).set_index('scenario')
        self.mappingScenario = self.mappingScenario.loc[
            self.mappingScenario.index.notnull()
        ]

    def loadData(self):
        datafiles = [pd.read_csv(dataFile) for dataFile in self.setup.DATA_FILE]
        self.data = pd.concat(datafiles)

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates(
            self.setup.VARIABLE_COLUMN_NAME
        ).set_index(self.setup.VARIABLE_COLUMN_NAME)
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=MAPPING_COLUMNS
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name=VAR_MAPPING_SHEET,
            index_label="original variable",
        )

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=self.setup.MODEL_COLUMN_NAME)
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='model_mapping',
            index_label="original model",
        )

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=self.setup.SCENARIO_COLUMN_NAME
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping.source = self.setup.SOURCE_ID

        self.mapping.to_excel(
            writer,
            engine='openpyxl',
            sheet_name='scenario_mapping',
            index_label="original scenario",
        )
        writer.close()

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        self.createSourceMeta()
        #        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()
        for model in self.mappingModel.index:
            mask = self.data['MODEL'] == self.mappingModel.loc[model]['original_model']
            tempDataMo = self.data.loc[mask]

            for scenario in self.mappingScenario.index:
                metaDict['scenario'] = scenario
                mask = (
                    tempDataMo['SCENARIO']
                    == self.mappingScenario.loc[scenario]['original_scenario']
                )
                tempDataMoSc = tempDataMo.loc[mask]

                for entity in self.mappingEntity.index:
                    metaDf = self.mappingEntity.loc[entity]
                    metaDict['entity'] = self.mappingEntity.loc[entity]['entity']
                    metaDict['model'] = model

                    for key in ['category', 'unit', 'unitTo']:
                        metaDict[key] = metaDf[key]

                    metaDict = dt.core._update_meta(metaDict)
                    tableID = dt.core._createDatabaseID(metaDict)
                    print(tableID)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                            continue

                    mask = tempDataMoSc['VARIABLE'] == entity
                    tempDataMoScEn = tempDataMoSc.loc[mask]

                    if len(tempDataMoScEn.index) > 0:

                        dataframe = tempDataMoScEn.set_index(
                            self.setup.SPATIAL_COLUM_NAME
                        )
                        if spatialSubSet:
                            spatIdx = dataframe[self.setup.SPATIAL_COLUM_NAME].isin(
                                spatialSubSet
                            )
                            dataframe = tempDataMoScEn.loc[spatIdx]

                        dataframe = dataframe.drop(self.setup.COLUMNS_TO_DROP, axis=1)
                        dataframe = dataframe.dropna(axis=1, how='all').astype(float)

                        dataTable = Datatable(dataframe, meta=metaDict)
                        # possible required unit conversion
                        if not pd.isna(metaDict['unitTo']):
                            dataTable = dataTable.convert(metaDict['unitTo'])
                        tablesToCommit.append(dataTable)
                    else:
                        excludedTables['empty'].append(tableID)

        return tablesToCommit, excludedTables


class PRIMAP_DOWNSCALE(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "PRIMAP_DOWN_2020"
        self.setup.SOURCE_YEAR = "2020"
        self.setup.SOURCE_NAME = "PRIMAP_DOWN"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/PRIMAP_DOWNSCALE'
        )

        self.setup.DATA_FILES = [
            os.path.join(self.setup.SOURCE_PATH, x)
            for x in os.listdir(self.setup.SOURCE_PATH)
            if x.endswith('.csv')
        ]
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')

        self.setup.LICENCE = 'Creative Commons Attribution 4.0 International'
        self.setup.URL = 'https://zenodo.org/record/3638137#.XxcBWiFR0Wp'

        self.entryMapping = {
            'entity': ['entity', 'unit', 'unitTo'],
            'source': ['model', 'downscaling'],
            'scenario': ['scenario'],
            'category': ['category'],
        }

        self.setup.SPATIAL_COLUM_NAME = ['country']
        self.setup.COLUMNS_TO_DROP = ["entity", "source", "scenario", "category"]

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = dict()
            for var in self.entryMapping.keys():
                df = pd.read_excel(self.setup.MAPPING_FILE, sheet_name=var, index_col=0)
                df = df.loc[~df.loc[:, self.entryMapping[var][0]].isna()]

                self.mapping[var] = df.T.to_dict()

    def createVariableMapping(self):
        # loading data if necessary
        #        if not hasattr(self, 'data'):
        #            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )
        self.mapping = dict()
        for data in self.loadData():
            for columnKey in self.entryMapping.keys():
                if not isinstance(self.entryMapping[columnKey], list):
                    self.entryMapping[columnKey] = [self.entryMapping[columnKey]]
                availableSeries = data.drop_duplicates(columnKey).set_index(columnKey)

                if columnKey not in self.mapping.keys():
                    self.mapping[columnKey] = pd.DataFrame(
                        index=availableSeries.index,
                        columns=self.entryMapping[columnKey],
                    )
                #                if "unit" in self.entryMapping[columnKey]:
                #                    self.mapping.unit = availableSeries.unit
                else:
                    newEntries = pd.DataFrame(
                        index=availableSeries.index,
                        columns=self.entryMapping[columnKey],
                    )

                    for idx in newEntries.index:
                        if idx not in self.mapping[columnKey].index:
                            self.mapping[columnKey].loc[idx, :] = newEntries.loc[idx, :]

                if "unit" in self.entryMapping[columnKey]:
                    self.mapping[columnKey].loc[
                        availableSeries.index, "unit"
                    ] = availableSeries.unit

        for columnKey in self.entryMapping.keys():
            self.mapping[columnKey].to_excel(
                writer,
                engine='openpyxl',
                sheet_name=columnKey,
                index_label="original variable",
            )

        writer.close()

    def loadData(self):
        for dataFile in self.setup.DATA_FILES[:]:
            yield pd.read_csv(dataFile, index_col=None, header=0)

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        self.createSourceMeta()
        #        # meta data
        #        self.loadMetaData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['erro'] = list()
        excludedTables['exists'] = list()

        #        datafilter = list()
        #        datafilter.append(self.data)
        #        filterList = list(self.entryMapping.keys())
        #        tableList = list()
        #        metaDict = dict()
        #        metaDict['source_name'] = [self.setup.SOURCE_ID]
        #        metaDict['source_year'] = [self.setup.SOURCE_YEAR]
        import copy
        import numpy as np

        def process_and_filter(self, datafilter, filterList, tableList, metaDict):

            if len(filterList) == 0:
                yearColumns = dt.util.yearsColumnsOnly(datafilter[-1])
                for key in metaDict.keys():
                    metaDict[key] = '|'.join(metaDict[key])
                dataTable = dt.Datatable(
                    datafilter[-1].loc[:, yearColumns], meta=metaDict
                )
                dataTable.index = datafilter[-1].loc[:, 'country']
                if 'unitTo' in dataTable.meta and (
                    isinstance(dataTable.meta['unitTo'], str)
                ):
                    # convert if required
                    dataTable = dataTable.convert(metaDict['unitTo'])
                    del dataTable.meta['unitTo']

                tableList.append(dataTable)
                del yearColumns
                del dataTable
                del key
                del datafilter, filterList, metaDict

                return tableList
            columnKey = filterList.pop()
            print(columnKey)
            for mappFrom, mappTo in self.mapping[columnKey].items():
                print(mappFrom, mappTo)
                mask = datafilter[-1].loc[:, columnKey] == mappFrom
                datafilter.append(datafilter[-1].loc[mask, :])
                for key in mappTo.keys():
                    if not isinstance(mappTo[key], str):
                        continue
                    if key in metaDict.keys():
                        print(metaDict[key])
                        print(mappTo[key])
                        metaDict[key].append(mappTo[key])

                    else:
                        metaDict[key] = [mappTo[key]]
                #                metaDict.update(mappTo)
                if len(datafilter[-1]) == 0:
                    # break loop if empty filtered set

                    # clean
                    _ = datafilter.pop()
                    print('Meta: ' + str(metaDict))
                    for key in mappTo.keys():
                        if not isinstance(mappTo[key], str):
                            continue
                        metaDict[key].remove(mappTo[key])
                    del _
                    continue

                #                sdf
                tableList = process_and_filter(
                    self,
                    copy.copy(datafilter),
                    copy.copy(filterList),
                    tableList,
                    copy.copy(metaDict),
                )

                # clean
                datafilter.pop()
                print('Meta: ' + str(metaDict))
                for key in mappTo.keys():
                    if not isinstance(mappTo[key], str):
                        continue
                    metaDict[key].remove(mappTo[key])

            del key

            del datafilter, filterList, metaDict
            return tableList

        for data in self.loadData():
            filterList = list(self.entryMapping.keys())
            tableList = list()
            metaDict = dict()
            metaDict['source_name'] = [self.setup.SOURCE_NAME]
            metaDict['source_year'] = [self.setup.SOURCE_YEAR]
            datafilter = [data]
            filteredData = process_and_filter(
                self, datafilter, filterList, tableList, metaDict
            )
            #            adsfr
            tablesToCommit.extend(filteredData)

        return tablesToCommit, excludedTables


class AIM15_2020(BaseImportTool):
    def __init__(self):

        self.setup = setupStruct()

        self.setup.SOURCE_ID = "AIM15_2020"
        self.setup.SOURCE_NAME = "AIM15"
        self.setup.SOURCE_YEAR = "2020"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/AIM15_2020'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, '201029_SSP12_19_26_base_AIM.xlsx'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = '?'  # TODO
        self.setup.URL = '?'  # TODO

        self.setup.COLUMNS_TO_DROP = ['model', 'scenario', 'variable', 'unit']

        self.createSourceMeta()

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
            )

    def createVariableMapping(self):
        with pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        ) as writer:

            if not hasattr(self, 'data'):
                self.loadData()

            meta = self.data.loc[
                ~self.data.index.duplicated(keep='first'),
                ['model', 'scenario', 'variable', 'unit'],
            ]

            variable = (
                meta['variable']
                .str.replace(" ", "_")
                .replace({"Emissions|Kyoto Gases": "Emissions|KYOTOGHG"})
            )
            variable_components = variable.str.split('|')
            two_component_entity = variable_components.str[0].isin(["Emissions", "GDP"])
            entity = (
                variable_components.str[:2]
                .str.join('|')
                .where(two_component_entity, variable_components.str[0])
            )
            category = (
                variable_components.str[2:]
                .str.join('|')
                .where(two_component_entity, variable_components.str[1:].str.join('|'))
            )

            self.mapping = meta.assign(
                source=self.setup.SOURCE_ID,
                variable=variable,
                entity=entity,
                category=category,
                unit=(
                    # ['Mt CO2/yr', 'EJ/yr', 'billion US$2005/yr', 'million',
                    #  'US$2005/t CO2', 'US$2005/GJ', 'Mt CH4/yr', 'Mt CO2-equiv/yr',
                    #  'kt N2O/yr', 'million Ha', 'Index (2005 = 1)', 'Mt BC/yr',
                    #  'Mt CO/yr', 'Mt NO2/yr', 'Mt OC/yr', 'Mt SO2/yr', 'EJ',
                    #  't DM/ha/yr', 'million t DM/yr', 'kcal/cap/day', 'million m3/yr',
                    #  'Mt NH3/yr', 'Mt VOC/yr', 'US$2005/MWh', 'GW', '1000 ha', '1000 t',
                    #  't/ha', 'billion US$2005', 1, 'Percentage (1=1%)', 'million Ha/yr',
                    #  'Mt/yr', 'Index (2010 = 1)', 'US$2005/kW', 'years',
                    #  'US$2005/kW/yr', 'ppm', 'W/m2', ' °C', 'ppb', 'Tg N/yr']
                    meta['unit']
                    .replace(
                        {
                            'Mt CO2-equiv/yr': 'Mt CO2eq/yr',
                            'MtCO2eq/year': 'Mt CO2eq/yr',
                            'kt HFC134a-equiv/yr': 'kt HFC134aeq/yr',
                            'EJ_final': 'EJ',
                            'EJ_primary': 'EJ',
                            'Index (2005 = 1)': '1',
                            'Index (2010 = 1)': '1',
                            'Percentage (1=1%)': '%',
                        }
                    )
                    .str.replace('US$2005', 'USD2005', regex=False)
                    .str.replace('m2', 'm**2', regex=False)
                    .str.replace('m3', 'm**3', regex=False)
                    .str.replace('million Ha', 'million ha', regex=False)
                    .str.replace(
                        ' OR local currency.*$', '', flags=re.IGNORECASE
                    )  # ?? TODO
                ),
            ).dropna()  # drops some AGMIP variables

            problematic_units = pd.Series(self.mapping.unit.unique())[
                lambda s: ~s.map(isUnit)
            ]
            if not problematic_units.empty:
                print("problematic units:", ", ".join(problematic_units))

            self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)

    def loadData(self):
        self.data = pd.read_excel(self.setup.DATA_FILE, sheet_name="Sheet1")
        self.data.set_axis(
            self.data.columns[:5].str.lower().append(self.data.columns[5:]),
            axis=1,
            inplace=True,
        )
        self.data.set_index(
            self.data['model']
            + '_'
            + self.data['scenario']
            + '_'
            + self.data['variable'],
            inplace=True,
        )

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):
        excludedTables = defaultdict(list)

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        for idx, metaDf in self.mapping.iterrows():
            metaDict = metaDf.to_dict()
            metaDict.update(
                source_name=self.setup.SOURCE_NAME,
                source_year=self.setup.SOURCE_YEAR,
            )

            metaDict = dt.core._update_meta(metaDict)
            tableID = dt.core._createDatabaseID(metaDict)

            if not updateTables and dt.core.DB.tableExist(tableID):
                excludedTables['exists'].append(tableID)
                print('table exists')
                print(tableID)
                continue

            dataframe = (
                self.data.loc[[idx], :]
                .set_index('region')
                .drop(self.setup.COLUMNS_TO_DROP, axis=1)
                .astype(float)
                .dropna(axis=1, how='all')
            )

            if not dataframe.index.is_unique:
                print(f"Table {tableID} has non-unique regions, skipping")
                continue

            try:
                dataTable = Datatable(dataframe, meta=metaDict)
            except pint.errors.UndefinedUnitError as exc:
                print(f"Undefined unit `{exc.args[0]}` for table {tableID}, skipping")
                excludedTables['error'].append(tableID)
                continue

            # possible required unit conversion
            if not pd.isna(metaDict.get('unitTo')):
                dataTable = dataTable.convert(metaDict['unitTo'])
            tablesToCommit.append(dataTable)

        return tablesToCommit, excludedTables


class LED_2019(BaseImportTool):
    def __init__(self):

        self.setup = setupStruct()

        self.setup.SOURCE_ID = "LED_2019"
        self.setup.SOURCE_NAME = "LED"
        self.setup.SOURCE_YEAR = "2019"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/LED_2019/'
        )
        self.setup.DATA_FILE = os.path.join(self.setup.SOURCE_PATH, 'led.csv')
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = '?'  # TODO
        self.setup.URL = '?'  # TODO

        self.setup.COLUMNS_TO_DROP = ['model', 'scenario', 'variable', 'unit']

        self.createSourceMeta()

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
            )

    def createVariableMapping(self):
        with pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        ) as writer:

            if not hasattr(self, 'data'):
                self.loadData()

            meta = self.data.loc[
                ~self.data.index.duplicated(keep='first'),
                ['model', 'scenario', 'variable', 'unit'],
            ]

            variable = (
                meta['variable']
                .str.replace(" ", "_")
                .replace({"Emissions|Kyoto Gases": "Emissions|KYOTOGHG"})
            )
            variable_components = variable.str.split('|')
            two_component_entity = variable_components.str[0].isin(["Emissions", "GDP"])
            entity = (
                variable_components.str[:2]
                .str.join('|')
                .where(two_component_entity, variable_components.str[0])
            )
            category = (
                variable_components.str[2:]
                .str.join('|')
                .where(two_component_entity, variable_components.str[1:].str.join('|'))
            )

            self.mapping = meta.assign(
                source=self.setup.SOURCE_ID,
                variable=variable,
                entity=entity,
                category=category,
                unit=(
                    # units ['million t DM/yr', 'GW', 'US$2010/kW', 'Mt CO2/yr',
                    #        'billion US$2010/yr', '%', 'Mt BC/yr', 'kt CF4/yr', 'Mt CH4/yr',
                    #        'Mt CO/yr', 'Mt CO2-equiv/yr', 'kt HFC134a-equiv/yr',
                    #        'kt HFC125/yr', 'kt HFC134a/yr', 'kt HFC143a/yr', 'kt HFC227ea/yr',
                    #        'kt HFC23/yr', 'kt HFC245fa/yr', 'kt HFC32/yr', 'kt HFC43-10/yr',
                    #        'kt N2O/yr', 'Mt NH3/yr', 'Mt NOx/yr', 'Mt OC/yr', 'kt SF6/yr',
                    #        'Mt SO2/yr', 'Mt VOC/yr', 'Tg N/yr', 't/ha/yr', 'Tg P/yr', 'EJ/yr',
                    #        'kcal/cap/day', 'million m3/yr', 'MtCO2eq/year', 'EJ_final', 'Mha',
                    #        'EJ_primary', 'Mm3', 'million ha', 'years', 'US$2010/kW/yr',
                    #        'US$2010/kWh', 'billion US$2010/yr OR local currency/yr',
                    #        'million', 'Index (2005 = 1)',
                    #        'US$2010/t CO2 or local currency/t CO2',
                    #        'US$2010/GJ or local currency/GJ', 'ZJ', 'km3/yr', 't DM/ha/yr']
                    meta['unit']
                    .replace(
                        {
                            'Mt CO2-equiv/yr': 'Mt CO2eq/yr',
                            'MtCO2eq/year': 'Mt CO2eq/yr',
                            'kt HFC134a-equiv/yr': 'kt HFC134aeq/yr',
                            'EJ_final': 'EJ',
                            'EJ_primary': 'EJ',
                            'Index (2005 = 1)': '1',  # ?? TODO
                        }
                    )
                    .str.replace('US$2010', 'USD2010', regex=False)
                    .str.replace('m3', 'm**3', regex=False)
                    .str.replace(
                        ' OR local currency.*$', '', flags=re.IGNORECASE
                    )  # ?? TODO
                ),
            )

            problematic_units = pd.Series(self.mapping.unit.unique())[
                lambda s: ~s.map(isUnit)
            ]
            if not problematic_units.empty:
                print("problematic units:", ", ".join(problematic_units))

            self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE)
        self.data.set_axis(self.data.columns.str.lower(), axis=1, inplace=True)
        self.data.set_index(
            self.data['model']
            + '_'
            + self.data['scenario']
            + '_'
            + self.data['variable'],
            inplace=True,
        )

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):
        excludedTables = defaultdict(list)

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        for idx, metaDf in self.mapping.iterrows():
            metaDict = metaDf.to_dict()
            metaDict.update(
                source_name=self.setup.SOURCE_NAME,
                source_year=self.setup.SOURCE_YEAR,
            )

            metaDict = dt.core._update_meta(metaDict)
            tableID = dt.core._createDatabaseID(metaDict)

            if not updateTables and dt.core.DB.tableExist(tableID):
                excludedTables['exists'].append(tableID)
                print('table exists')
                print(tableID)
                continue

            dataframe = (
                self.data.loc[idx, :]
                .set_index('region')
                .drop(self.setup.COLUMNS_TO_DROP, axis=1)
                .astype(float)
                .dropna(axis=1, how='all')
            )

            if not dataframe.index.is_unique:
                print(f"Table {tableID} has non-unique regions, skipping")
                continue

            try:
                dataTable = Datatable(dataframe, meta=metaDict)
            except pint.errors.UndefinedUnitError as exc:
                print(f"Undefined unit `{exc.args[0]}` for table {tableID}, skipping")
                excludedTables['error'].append(tableID)
                continue

            # possible required unit conversion
            if not pd.isna(metaDict.get('unitTo')):
                dataTable = dataTable.convert(metaDict['unitTo'])
            tablesToCommit.append(dataTable)

        return tablesToCommit, excludedTables


class IMAGE15_2020(BaseImportTool):
    def __init__(self):

        self.setup = setupStruct()

        self.setup.SOURCE_ID = "IMAGE15_2020"
        self.setup.SOURCE_NAME = "IMAGE15"
        self.setup.SOURCE_YEAR = "2020"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/IMAGE15_2020'
        )
        self.setup.DATA_FILE = {
            fn[: fn.rindex(".")]: os.path.join(self.setup.SOURCE_PATH, fn)
            for fn in os.listdir(self.setup.SOURCE_PATH)
            if fn.endswith((".csv", ".xlsx")) and fn != 'mapping.xlsx'
        }
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = '?'  # TODO
        self.setup.URL = '?'  # TODO

        self.setup.COLUMNS_TO_DROP = ['model', 'scenario', 'variable', 'unit']

        self.createSourceMeta()

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
            )

    def createVariableMapping(self):
        with pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        ) as writer:

            if not hasattr(self, 'data'):
                self.loadData()

            meta = self.data.loc[
                ~self.data.index.duplicated(keep='first'),
                ['model', 'scenario', 'variable', 'unit'],
            ]
            variable = (
                meta['variable']
                .str.replace(" ", "_")
                .replace({"Emissions|Kyoto Gases": "Emissions|KYOTOGHG"})
            )
            variable_components = variable.str.split('|')
            two_component_entity = variable_components.str[0].isin(["Emissions", "GDP"])
            entity = (
                variable_components.str[:2]
                .str.join('|')
                .where(two_component_entity, variable_components.str[0])
            )
            category = (
                variable_components.str[2:]
                .str.join('|')
                .where(two_component_entity, variable_components.str[1:].str.join('|'))
            )

            self.mapping = meta.assign(
                source=self.setup.SOURCE_ID,
                variable=variable,
                entity=entity,
                category=category,
                unit=(
                    # ['million t DM/yr', 'GW', '$/kW', 'ppb', 'ppm',
                    #     'billion US$2005/yr', 'Mt BC/yr', 'kt C2F6/yr', 'kt C6F14/yr',
                    #     'kt CF4/yr', 'Mt CH4/yr', 'Mt CO2/yr', 'Mt CO/yr',
                    #     'Mt CO2-equiv/yr', 'kt HFC134a-equiv/yr', 'kt HFC125/yr',
                    #     'kt HFC134a/yr', 'kt HFC143a/yr', 'kt HFC227ea/yr', 'kt HFC23/yr',
                    #     'kt HFC245fa/yr', 'kt HFC32/yr', 'kt HFC43-10/yr', 'kt N2O/yr',
                    #     'Mt NH3/yr', 'Mt NO2/yr', 'Mt OC/yr', 'kt CF4-equiv/yr',
                    #     'kt SF6/yr', 'Mt SO2/yr', 'Mt VOC/yr', 'bn tkm/yr', 'bn pkm/yr',
                    #     'Tg N/yr', 'EJ/yr', 'kcal/cap/day', 'W/m2', 'million m3/yr',
                    #     'million ha', 'US$2005/MWh', 'million', 'Index (2005 = 1)',
                    #     'US$2005/GJ', 'EJ', 'ktU', ' °C', 't DM/ha/yr', '1000 t',
                    #     'US$2005/kW', '%', 'million Ha/yr', 'years', 'US$2005/kW/yr',
                    #     'US$2005/t CO2']
                    meta['unit']
                    .str.replace('US$2005', 'USD2005', regex=False)
                    .replace(
                        {
                            'ktU': 'kt U',
                            'Mt CO2-equiv/yr': 'Mt CO2eq/yr',
                            'kt HFC134a-equiv/yr': 'kt HFC134aeq/yr',
                            'Index (2005 = 1)': '1',  # ?? TODO
                        }
                    )
                    .str.replace('US$2005', 'USD2005', regex=False)
                    .str.replace('bn', 'billion', regex=False)
                    .str.replace('m3', 'm**3', regex=False)
                    .str.replace('m2', 'm**2', regex=False)
                ),
            )

            problematic_units = pd.Series(self.mapping.unit.unique())[
                lambda s: ~s.map(isUnit)
            ]
            if not problematic_units.empty:
                print("problematic units:", ", ".join(problematic_units))
            self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)

    def loadData(self):
        data = []
        for fn in self.setup.DATA_FILE.values():
            if fn.endswith(".csv"):
                df = pd.read_csv(fn, encoding='latin1')
            elif fn.endswith(".xlsx"):
                df = pd.read_excel(fn, sheet_name="data")
            else:
                raise RuntimeError("File must have a file ending .csv or .xlsx")

            df.set_axis(df.columns.str.lower(), axis=1, inplace=True)
            df.set_index(
                df['model'] + '_' + df['scenario'] + '_' + df['variable'], inplace=True
            )

            data.append(df)

        self.data = pd.concat(data, sort=False)

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):
        excludedTables = defaultdict(list)

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        for idx, metaDf in self.mapping.iterrows():
            metaDict = metaDf.to_dict()
            metaDict.update(
                source_name=self.setup.SOURCE_NAME,
                source_year=self.setup.SOURCE_YEAR,
            )

            metaDict = dt.core._update_meta(metaDict)
            tableID = dt.core._createDatabaseID(metaDict)

            if not updateTables and dt.core.DB.tableExist(tableID):
                excludedTables['exists'].append(tableID)
                print('table exists')
                print(tableID)
                continue

            dataframe = (
                self.data.loc[self.data.index == idx]
                .set_index('region')
                .drop(self.setup.COLUMNS_TO_DROP, axis=1)
                .astype(float)
                .dropna(axis=1, how='all')
            )

            if not dataframe.index.is_unique:
                print(f"Table {tableID} has non-unique regions, skipping")
                continue

            try:
                dataTable = Datatable(dataframe, meta=metaDict)
            except pint.errors.UndefinedUnitError:
                excludedTables['error'].append(tableID)
                continue

            # possible required unit conversion
            if not pd.isna(metaDict.get('unitTo')):
                dataTable = dataTable.convert(metaDict['unitTo'])
            tablesToCommit.append(dataTable)

        return tablesToCommit, excludedTables


class PRIMAP_HIST(BaseImportTool):
    def __init__(self, 
                 filename,
                 year,
                 mapping_file):
               

        self.setup = setupStruct()

        self.setup.SOURCE_ID = "PRIMAP_" + str(year)
        self.setup.SOURCE_NAME = "PRIMAP"
        self.setup.SOURCE_YEAR = str(year)
        self.setup.SOURCE_PATH = os.path.join('data','PRIMAP',str(year)
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, filename
        )
        self.setup.MAPPING_FILE = mapping_file
        self.setup.LICENCE = 'CC BY-4.0'
        self.setup.URL = 'https://zenodo.org/record/7179775'

        #        self.setup.INDEX_COLUMN_NAME = 'SeriesCode'
        #        self.setup.SPATIAL_COLUM_NAME = 'GeoAreaCode'
        #
        #        self.setup.COLUMNS_TO_DROP = ['Country Name', 'Indicator Name']
        self.setup.COLUMNS_TO_DROP = [
            'scenario',
            'category',
            'entity',
            'unit',
            'primary_code',
        ]

        self.createSourceMeta()

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
            )

    def pre_process_input_file(self):

        data = self.data.copy()

        data = data.rename(
            columns={
                'category (IPCC2006_PRIMAP)': 'category',
                'scenario (PRIMAP-hist)': 'scenario',
                'area (ISO3)': 'country',
            }
        )
        data = data.drop('source', axis=1)

        rename_cat_dict = {
            x: 'IPC' + x.replace('.', '') for x in data.category.unique()
        }

        data.category = data.category.map(rename_cat_dict)
        data.country = data.country.replace('EU27BX', 'EU27')

        self.data = data

    def createVariableMapping(self):
        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        if not hasattr(self, 'data'):
            self.loadData()

        meta = self.data.loc[~self.data.index.duplicated(keep='first')]
        scenario_mapping = {
            'HISTCR': 'Historic|country_reported',
            'HISTTP': 'Historic|third_party',
        }
        self.mapping = pd.DataFrame.from_dict(
            {
                'source': self.setup.SOURCE_ID,
                'scenario': meta['scenario'].replace(scenario_mapping),
                'category': meta['category'],
                'unit': meta['unit'],
                'entity': 'Emissions|' + meta['entity'],
            }
        )

        idx_mask = self.mapping.index[self.mapping.entity.str.contains('GWP')]
        self.mapping.loc[idx_mask, 'unit'] = 'Gg CO2eq /yr'
        self.mapping = self.mapping.assign(
            variable=self.mapping['entity'] + '|' + self.mapping['category'],
            pathway=self.mapping['scenario'],
            unitTo=self.mapping['unit'].str.replace('Gg', 'Mt'),
        ).reindex(columns=pd.Index(MAPPING_COLUMNS).union(self.mapping.columns))
        self.mapping.entity = self.mapping.entity.replace(
            {
                'Emissions|KYOTOGHG (AR4GWP100)': 'Emissions|KYOTOGHG_AR4',
                'Emissions|KYOTOGHG (SARGWP100)': 'Emissions|KYOTOGHG_SAR',
                'Emissions|FGASES (AR4GWP100)': 'Emissions|FGASES_AR4',
                'Emissions|HFCS (AR4GWP100)': 'Emissions|HFCS_AR4',
                'Emissions|PFCS (AR4GWP100)': 'Emissions|PFCS_AR4',
                'Emissions|FGASES (SARGWP100)': 'Emissions|FGASES_SAR',
                'Emissions|HFCS (SARGWP100)': 'Emissions|HFCS_SAR',
                'Emissions|PFCS (SARGWP100)': 'Emissions|PFCS_SAR',
            }
        )

        self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)

        sector_mapping = {
            'IPCM0EL': 'National Total excluding LULUCF',
            'IPC1': 'Energy',
            'IPC1A': 'Fuel Combustion Activities',
            'IPC1B': 'Fugitive Emissions from Fuels',
            'IPC1B1': 'Solid Fuels',
            'IPC1B2': 'Oil and Natural Gas',
            'IPC1B3': 'Other Emissons from Energy Production',
            'IPC1C': 'Carbon Dioxide Transport and Storage (currently no data available)',
            'IPC2': 'Industrial Processes and Product Use',
            'IPC2A': 'Mineral Industry',
            'IPC2B': 'Chemical Industry',
            'IPC2C': 'Metal Industry',
            'IPC2D': 'Non-Energy Products from Fuels and Solvent Use',
            'IPC2E': 'Electronics Industry (no data available as the category is only used for fluorinated gases which are only resolved at the level of category IPC2)',
            'IPC2F': 'Product uses as Substitutes for Ozone Depleting Substances (no data available as the category is only used for fluorinated gases which are only resolved at the level of category IPC2)',
            'IPC2G': 'Other Product Manufacture and Use',
            'IPC2H': 'Other',
            'IPCMAG': 'Agriculture, sum of IPC3A and IPCMAGELV',
            'IPC3A': 'Livestock',
            'IPCMAGELV': 'Agriculture excluding Livestock',
            'IPC4': 'Waste',
            'IPC5': 'Other',
        }

        dataFrame = pd.DataFrame.from_dict({'alternative': pd.Series(sector_mapping)})
        dataFrame.to_excel(writer, sheet_name='sector_mapping')

        writer.close()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, header=0)
        self.data['primary_code'] = self.data.index
        self.pre_process_input_file()
        self.data.set_index(
            self.data['entity']
            + '_'
            + self.data['category']
            + '_'
            + self.data['scenario'],
            inplace=True,
        )

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):
        excludedTables = defaultdict(list)

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        for idx, metaDf in self.mapping.iterrows():

            metaDict = metaDf.to_dict()
            metaDict.update(
                source_name=self.setup.SOURCE_NAME,
                source_year=self.setup.SOURCE_YEAR,
            )

            if pd.isnull(metaDict['entity']):
                print('skipping: ' + idx)
                continue
            else:
                print('processing: ' + idx)

            for key in metaDict.keys():
                if pd.isnull(metaDict[key]):
                    metaDict[key] = ''
            # if metaDict['entity'] == 'Emissions|KYOTOGHG':
            #     dfgs
            if not updateTables:
                metaDict = dt.core._update_meta(metaDict)
                tableID = dt.core._createDatabaseID(metaDict)
                if dt.core.DB.tableExist(tableID):
                    excludedTables['exists'].append(tableID)
                    print('table exists')
                    print(tableID)
                    continue

            metaDict['original code'] = idx

            if idx not in self.data.index:
                print('skipping :' + idx)
                continue
            dataframe = (
                self.data.loc[idx, :]
                .set_index('country')
                .drop(self.setup.COLUMNS_TO_DROP, axis=1)
                .astype(float)
                .dropna(axis=1, how='all')
            )

            dataTable = Datatable(dataframe, meta=metaDict)
            # possible required unit conversion
            if not pd.isna(metaDict.get('unitTo')):
                dataTable = dataTable.convert(metaDict['unitTo'])
            tablesToCommit.append(dataTable)

        return tablesToCommit, excludedTables


class CRF_DATA(BaseImportTool):
    def __init__(
        self,
        reportingYear,
        raw_data_folder=None,
    ):

        self.setup = setupStruct()
        self.year = str(reportingYear)

        if raw_data_folder is None:
            raw_data_folder = os.path.join(config.PATH_TO_DATASHELF, 'rawdata')

        self.setup.SOURCE_ID = "UNFCCC_CRF_" + str(reportingYear)
        self.setup.SOURCE_PATH = os.path.join(
            raw_data_folder, 'UNFCCC_CRF_' + str(reportingYear)
        )
        self.setup.LICENCE = 'open access (UN)'
        self.setup.URL = (
            'https://unfccc.int/process-and-meetings/transparency-and-reporting/reporting-and-review-under-the-convention/greenhouse-gas-inventories-annex-i-parties/national-inventory-submissions-'
            + str(reportingYear)
        )

        self.mappingDict = dict()

        self.mappingDict['sectors'] = {
            'IPC0': '7',  # total
            'IPC1': '8',  # energy
            'IPC1|Fuel_combustion': '9',
            'IPC1|Fuel_combustion|Energy_industries': '10',
            'IPC1|Fuel_combustion|Manufacturing&construction': '11',
            'IPC1|Fuel_combustion|Transport': '12',
            'IPC1|Fuel_combustion|Other_sectors': '13',
            'IPC1|Fuel_combustion|Other': '14',
            'IPC1|Fugitive_emissions': '15',
            'IPC1|Fugitive_emissions|Solid': '16',
            'IPC1|Fugitive_emissions|Fluid&gaseous': '17',
            'IPC2': '19',
            'IPC2|Mineral_industry': '20',  # industry and product use
            'IPC2|Chemical_industry': '21',  # industry and product use
            'IPC2|Metal_industry': '22',  # industry and product use
            'IPC4': '48',  # waste
            'IPCMAG': '28',  # agriculture
            'LULUCF': '39',  # LULUCF
            'LULUCF|Forestland': '40',  # LULUCF
            'LULUCF|Grassland': '41',  # LULUCF
            'LULUCF|Cropland': '42',  # LULUCF
            'LULUCF|Wetlands': '43',  # LULUCF
            'LULUCF|Settlements': '44',  # LULUCF
            'LULUCF|Harvested_wood_products': '46',  # LULUCF
            'IPC5': '54',  # Other
            'Aviation': '58',  # international
            'Marine': '59',
        }  # international

        self.mappingDict['gases'] = {
            'KYOTOGHG_AR4': 'J',
            'CO2': 'B',
            'CH4': 'C',
            'N2O': 'D',
            'HFCs': 'E',
            'PFCs': 'F',
            'SF6': 'G',
        }

    def prepareFolders(self):
        import zipfile
        import os

        folder = self.setup.SOURCE_PATH
        fileList = os.listdir(folder)
        fileList = [file for file in fileList if '.zip' in file]

        for file in fileList:
            try:
                with zipfile.ZipFile(os.path.join(folder, file), 'r') as zipObj:
                    coISO = file.split('-')[0].upper()
                    # Extract all the contents of zip file in different directory
                    zipObj.extractall(os.path.join(folder, coISO))
            except:
                print('failed: {}'.format(file))

    def gatherMappedData(self):

        dataTables = dict()
        countryList = list()
        folderList = [
            name
            for name in os.listdir(self.setup.SOURCE_PATH)
            if os.path.isdir(os.path.join(self.setup.SOURCE_PATH, name))
        ]
        for folder in folderList:
            coISO = folder
            countryPath = os.path.join(self.setup.SOURCE_PATH, folder)
            try:
                countryList.append(dt.mapp.countries.codes.loc[coISO, 'name'])
            except:
                countryList.append(coISO)
            print(coISO)

            fileList = os.listdir(countryPath)
            for fileName in fileList:
                year = int(fileName.split('_')[2])
                setup = dict()
                setup['filePath'] = countryPath
                setup['fileName'] = fileName
                setup['sheetName'] = 'Summary2'

                reader = dt.io.ExcelReader(setup)

                for sector in self.mappingDict['sectors']:
                    for gas in self.mappingDict['gases']:
                        variable = 'Emissions|' + gas + '|' + sector

                        if variable not in dataTables.keys():
                            # create table
                            meta = dict()
                            meta['entity'] = 'Emissions|' + gas
                            meta['category'] = sector
                            meta['scenario'] = 'Historic|country_reported'
                            meta['source'] = self.setup.SOURCE_ID
                            meta['unit'] = 'MtCO2eq'
                            dataTables[variable] = dt.Datatable(
                                columns=list(range(1990, 2017)), meta=meta
                            )
                        try:
                            dataTables[variable].loc[coISO, year] = (
                                reader.gatherValue(
                                    self.mappingDict['gases'][gas]
                                    + self.mappingDict['sectors'][sector]
                                )
                                / 1000
                            )
                        except:
                            pass

        import copy

        for gas in self.mappingDict['gases']:
            if 'Emissions|' + gas + '|LULUCF' in dataTables.keys():
                dataTables['Emissions|' + gas + '|IPCM0EL'] = (
                    dataTables['Emissions|' + gas + '|IPC0']
                    - dataTables['Emissions|' + gas + '|LULUCF']
                )  # national total excluding lulucf
                dataTables['Emissions|' + gas + '|IPCM0EL'].meta = copy.copy(
                    dataTables['Emissions|' + gas + '|IPC0'].meta
                )
                dataTables['Emissions|' + gas + '|IPCM0EL'].meta['category'] = "IPCM0EL"
                dataTables['Emissions|' + gas + '|IPC3'] = (
                    dataTables['Emissions|' + gas + '|IPCMAG']
                    + dataTables['Emissions|' + gas + '|LULUCF']
                )  # AFOLU
                dataTables['Emissions|' + gas + '|IPC3'].meta = copy.copy(
                    dataTables['Emissions|' + gas + '|IPC0'].meta
                )
                dataTables['Emissions|' + gas + '|IPC3'].meta['category'] = "IPCM3"

        tablesToCommit = list()
        for key in dataTables.keys():
            dataTables[key] = dataTables[key].astype(float)
            dataTables[key].generateTableID()
            tablesToCommit.append(dataTables[key])
        return tablesToCommit


class SDG_DATA_2019(BaseImportTool):
    def __init__(self):

        self.setup = setupStruct()

        self.setup.SOURCE_ID = "SDG_DB_2019"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/SDG_DB_2019'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'extract_05_2019.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = 'open access (UN)'
        self.setup.URL = 'https://unstats.un.org/sdgs/indicators/database/'

        self.setup.INDEX_COLUMN_NAME = 'SeriesCode'
        self.setup.SPATIAL_COLUM_NAME = 'GeoAreaCode'

        self.setup.COLUMNS_TO_DROP = ['Country Name', 'Indicator Name']

        self.createSourceMeta()

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
        else:
            self.mapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name=VAR_MAPPING_SHEET, index_col=0
            )

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, header=0)
        self.data['primary_code'] = self.data.index
        modifierColumns = [self.setup.INDEX_COLUMN_NAME] + list(
            self.data.columns[14:-1]
        )
        modifierColumns.remove('[Reporting Type]')
        # self.data[modifierColumns] = self.data[modifierColumns].fillna('NaN')
        # self.data[modifierColumns].apply(lambda x: '_'.join(x), axis=1)
        # self.data[modifierColumns].astype(str).apply(lambda x: x.str.cat(sep='_'), axis=1)
        self.data.index = (
            self.data[modifierColumns]
            .fillna('')
            .astype(str)
            .apply(lambda x: '_'.join(filter(None, x)), axis=1)
        )

    def createVariableMapping(self):
        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        if not hasattr(self, 'data'):
            #            self.data = pd.read_csv(self.setup.DATA_FILE, encoding='utf8', engine='python', index_col = None, header =0, na_values='..')
            #            self.data['combined'] = self.data[self.setup.INDEX_COLUMN_NAME].apply(lambda x: '_'.join(x), axis=1)
            self.loadData()

        # self.data[self.setup.INDEX_COLUMN_NAME].apply(lambda x: '_'.join(x), axis=1)
        availableSeries = self.data.index.unique()
        descrDf = self.data['SeriesDescription'].drop_duplicates()

        self.mapping = pd.DataFrame(
            index=availableSeries, columns=MAPPING_COLUMNS + ['description']
        )
        self.mapping.source = self.setup.SOURCE_ID
        self.mapping.scenario = 'historic'
        self.mapping.description = descrDf
        self.mapping.description = self.mapping.description.fillna(method='ffill')
        self.mapping.to_excel(writer, sheet_name=VAR_MAPPING_SHEET)
        writer.close()

    def gatherMappedData(self, spatialSubSet=None):
        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        indexDataToCollect = self.mapping.index[~pd.isnull(self.mapping['entity'])]

        tablesToCommit = []
        for idx in indexDataToCollect:
            metaDf = self.mapping.loc[idx]
            print(idx)

            # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
            # print(metaData[self.setup.INDEX_COLUMN_NAME])

            metaDict = {
                key: metaDf[key]
                for key in config.REQUIRED_META_FIELDS.union({'unitTo'})
            }

            metaDict['original code'] = idx
            # metaDict['original name'] = metaDf['Indicator Name']

            seriesIdx = idx

            dataframe = self.data.loc[seriesIdx, ['GeoAreaCode', 'TimePeriod', 'Value']]
            dataframe = dataframe.pivot(
                index='GeoAreaCode', columns='TimePeriod', values='Value'
            )

            dataframe = dataframe.astype(float)

            newData = list()
            for iRow in range(len(dataframe)):
                ISO_ID = dt.mapp.getSpatialID(dataframe.index[iRow])
                if dataframe.index[iRow] == 1:
                    ISO_ID = "World"
                if ISO_ID:
                    newData.append(ISO_ID)
                else:
                    newData.append(pd.np.nan)
            dataframe.index = newData

            if spatialSubSet:
                spatIdx = dataframe.index.isin(spatialSubSet)
                dataframe = dataframe.loc[spatIdx]

            dataframe = dataframe.dropna(axis=1, how='all')
            dataframe = dataframe.loc[~pd.isna(dataframe.index)]
            dataTable = Datatable(dataframe, meta=metaDict)
            # possible required unit conversion
            if not pd.isna(metaDict['unitTo']):
                dataTable = dataTable.convert(metaDict['unitTo'])
            tablesToCommit.append(dataTable)

        return tablesToCommit


class HOESLY2018(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "HOESLY2018"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/HOESLY2018'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'compiled_raw_hoesly.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = ' Creative Commons Attribution 3.0 License'
        self.setup.URL = 'https://www.geosci-model-dev.net/11/369/2018/'

        self.setup.MODEL_COLUMN_NAME = 'model'
        self.setup.SCENARIO_COLUMN_NAME = 'scenario'
        self.setup.REGION_COLUMN_NAME = 'region'
        self.setup.VARIABLE_COLUMN_NAME = 'variable'

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = dict()

            for var in ['variable', 'scenario', 'model', 'region']:
                df = pd.read_excel(
                    self.setup.MAPPING_FILE, sheet_name=var + '_mapping', index_col=0
                )
                df = df.loc[~df.loc[:, var].isna()]
                self.mapping.update(df.to_dict())

        self.createSourceMeta()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates('variable').set_index(
            self.setup.VARIABLE_COLUMN_NAME
        )['unit']
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=[self.setup.VARIABLE_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=[self.setup.MODEL_COLUMN_NAME])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.SCENARIO_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')

        # region
        index = np.unique(self.data[self.setup.region_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.REGION_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()

        for idx in self.mapping.index:
            iso = dt.util.identifyCountry(idx)
            if iso is not None:
                self.mapping.loc[idx, 'region'] = iso

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='region_mapping')
        writer.close()

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        import tqdm

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        for model in self.mapping['model'].keys():
            tempMo = self.data.loc[self.data.model == model]
            for scenario in self.mapping['scenario'].keys():
                tempMoSc = tempMo.loc[self.data.scenario == scenario]
                #                for variable in self.mapping['variable'].keys():
                #                    tempMoScVa = tempMoSc.loc[self.data.variable == variable]

                tables = dt.interfaces.read_long_table(
                    tempMoSc, list(self.mapping['variable'].keys())
                )
                for table in tables:
                    table.meta['category'] = ""
                    table.meta['source'] = self.setup.SOURCE_ID
                    table.index = table.index.map(self.mapping['region'])

                    tableID = dt.core._createDatabaseID(table.meta)
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                        else:
                            tablesToCommit.append(table)
        return tablesToCommit, excludedTables


class VANMARLE2017(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "VANMARLE2017"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/VANMARLE2017'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'compiled_raw_vanmarle.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = ' Creative Commons Attribution 3.0 License'
        self.setup.URL = 'https://www.geosci-model-dev.net/10/3329/2017/'

        self.setup.MODEL_COLUMN_NAME = 'model'
        self.setup.SCENARIO_COLUMN_NAME = 'scenario'
        self.setup.REGION_COLUMN_NAME = 'region'
        self.setup.VARIABLE_COLUMN_NAME = 'variable'

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = dict()

            for var in ['variable', 'scenario', 'model', 'region']:
                df = pd.read_excel(
                    self.setup.MAPPING_FILE, sheet_name=var + '_mapping', index_col=0
                )
                df = df.loc[~df.loc[:, var].isna()]
                self.mapping.update(df.to_dict())

        self.createSourceMeta()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates('variable').set_index(
            self.setup.VARIABLE_COLUMN_NAME
        )['unit']
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=[self.setup.VARIABLE_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=[self.setup.MODEL_COLUMN_NAME])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)
        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.SCENARIO_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')

        # region
        index = np.unique(self.data[self.setup.REGION_COLUMN_NAME].values)
        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.REGION_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        for idx in self.mapping.index:
            iso = dt.util.identifyCountry(idx)
            if iso is not None:
                self.mapping.loc[idx, 'region'] = iso

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='region_mapping')
        writer.close()

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        import tqdm

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        for model in self.mapping['model'].keys():
            tempMo = self.data.loc[self.data.model == model]
            for scenario in self.mapping['scenario'].keys():
                tempMoSc = tempMo.loc[tempMo.scenario == scenario]
                #                for variable in self.mapping['variable'].keys():
                #                    tempMoScVa = tempMoSc.loc[self.data.variable == variable]

                for variable in list(self.mapping['variable'].keys()):
                    tempMoScVar = tempMoSc.loc[tempMoSc.variable == variable]
                    tempMoScVar.unit = self.mapping['unit'][variable]
                    tables = dt.interfaces.read_long_table(tempMoScVar, [variable])
                    for table in tables:
                        table.meta['category'] = ""
                        table.meta['source'] = self.setup.SOURCE_ID
                        table.index = table.index.map(self.mapping['region'])

                        tableID = dt.core._createDatabaseID(table.meta)
                        if not updateTables:
                            if dt.core.DB.tableExist(tableID):
                                excludedTables['exists'].append(tableID)
                            else:
                                tablesToCommit.append(table)
        return tablesToCommit, excludedTables


class APEC(BaseImportTool):

    """
    IRENA data import tool
    """

    def __init__(self, year):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "APEC_" + str(year)
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/APEC/' + str(year)
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'compiled_raw_hoesly.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = '(c) 2019 Asia Pacific Economic Cooperation (APERC)'
        self.setup.URL = 'https://www.apec.org/Publications/2019/05/APEC-Energy-Demand-and-Supply-Outlook-7th-Edition---Volume-I'

        #        self.setup.INDEX_COLUMN_NAME = ['FLOW', 'PRODUCT']
        #        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        #        self.setup.COLUMNS_TO_DROP = [ 'PRODUCT','FLOW','combined']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            #            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = pd.read_excel(self.setup.MAPPING_FILE, sheet_name='APEC')

            self.spatialMapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name='spatial', index_col=0
            )
            self.spatialMapping = self.spatialMapping.loc[
                ~pd.isnull(self.spatialMapping.mapping)
            ]

        self.createSourceMeta()

    def addSpatialMapping(self):
        # EU
        mappingToCountries = dict()
        # self.spatialMapping.loc['Memo: European Union-28'] = 'EU28'

    #        mappingToCountries['ASEAN'] =  ['VNM', 'PHL', 'THA', 'SGP', 'MMR', 'IDN', 'KHM', 'BRN', 'MYS', 'LAO']
    #        dt.mapp.regions.addRegionToContext('WEO',mappingToCountries)

    def gatherMappedData(self, spatialSubSet=None):

        tablesToCommit = dt.TableSet()
        setup = dict()
        setup['filePath'] = self.setup.SOURCE_PATH
        setup['fileName'] = 'APEC_Energy_Outlook_7th_Edition_Tables.xlsx'

        # loop over energy mapping
        for region in self.spatialMapping.index:
            setup['sheetName'] = region
            setup['timeColIdx'] = ['AO:A0']
            setup['spaceRowIdx'] = ['AO:A0']

            ex = dt.io.ExcelReader(setup)
            coISO = self.spatialMapping.loc[region, 'mapping']
            print(region, ex.gatherValue('B1') + '->' + coISO)
            continue

            for i, idx in enumerate(list(self.mapping.index)):

                metaDf = self.mapping.loc[idx, :]
                # print(metaDf['What'])

                # Capacity
                setup['timeColIdx'] = tuple(metaDf['Time'].split(':'))
                setup['spaceRowIdx'] = tuple([metaDf['What']])
                # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
                # print(metaData[self.setup.INDEX_COLUMN_NAME])

                ex.timeColIdx = [
                    dt.io.excelIdx2PandasIdx(x) for x in setup['timeColIdx']
                ]
                ex.spaceRowIdx = [
                    dt.io.excelIdx2PandasIdx(x) for x in setup['spaceRowIdx']
                ]
                #                if "International" in metaDf['Name']:
                #                    sdf
                #                if ex.spaceRowIdx[0][0]>41 and region != 'World':
                #                    ex.spaceRowIdx = [(ex.spaceRowIdx[0][0]-1,0)]

                df = ex.gatherData()
                df.columns = df.columns.astype(int)
                metaDict = dict()
                metaDict['entity'] = metaDf['Name'].strip().replace('| ', '|')
                metaDict['category'] = ''
                metaDict['unit'] = metaDf['unit']
                metaDict['scenario'] = metaDf['Scenario']
                metaDict['source'] = self.setup.SOURCE_ID
                metaDict['unitTo'] = metaDf['unitTo']
                ID = dt.core._createDatabaseID(metaDict)

                if ID not in tablesToCommit.keys():
                    table = dt.Datatable(columns=range(2000, 2100), meta=metaDict)

                    table.loc[coISO, df.columns] = df.values
                    tablesToCommit.add(table)
                else:
                    table = tablesToCommit[ID]
                    table.loc[coISO, df.columns] = df.values

        tablesList = list()
        for ID in tablesToCommit.keys():
            dataTable = tablesToCommit[ID]
            print(dataTable.meta)
            if not pd.isna(dataTable.meta['unitTo']):
                dataTable = dataTable.convert(dataTable.meta['unitTo'])
                del dataTable.meta['unitTo']
            tablesList.append(dataTable.astype(float))

        return tablesList, []


class FAO(BaseImportTool):
    """
    FAO data import tool
    """

    def __init__(self, year=2019, data_path=None):

        if data_path is None:
            data_path = os.path.join(config.PATH_TO_DATASHELF, 'rawdata')

        self.setup = setupStruct()
        self.setup.SOURCE_ID = "FAO_" + str(year)
        self.setup.SOURCE_PATH = os.path.join(data_path, 'FAO_' + str(year))
        self.setup.DATA_FILE = {
            'Emissions_Land_Use_': os.path.join(
                self.setup.SOURCE_PATH,
                'Emissions_Land_Use_Land_Use_Total_E_All_Data.csv',
            ),
            'Emissions_Agriculture_': os.path.join(
                self.setup.SOURCE_PATH,
                'Emissions_Agriculture_Agriculture_total_E_All_Data.csv',
            ),
            'Environment_Emissions_by_Sector_': os.path.join(
                self.setup.SOURCE_PATH, 'Environment_Emissions_by_Sector_E_All_Data.csv'
            ),
            'Environment_Emissions_intensities_': os.path.join(
                self.setup.SOURCE_PATH,
                'Environment_Emissions_intensities_E_All_Data.csv',
            ),
            'Environment_LandCover_': os.path.join(
                self.setup.SOURCE_PATH, 'Environment_LandCover_E_All_Data.csv'
            ),
            'Environment_LandUse_': os.path.join(
                self.setup.SOURCE_PATH, 'Environment_LandUse_E_All_Data.csv'
            ),
            'Inputs_LandUse_': os.path.join(
                self.setup.SOURCE_PATH, 'Inputs_LandUse_E_All_Data.csv'
            ),
            'Emissions_Total': os.path.join(
                self.setup.SOURCE_PATH, 'Emissions_Totals_E_All_Data.csv'
            ),
        }

        self.setup.MAPPING_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'mapping_2022.xlsx'
        )
        self.setup.LICENCE = (
            'Food and Agriculture Organization of the United Nations (FAO)'
        )
        self.setup.URL = 'http://www.fao.org/faostat/en/#data/GL'
        #        self.setup.MODEL_COLUMN_NAME = 'model'
        self.setup.SCENARIO_COLUMN_NAME = 'scenario'
        self.setup.REGION_COLUMN_NAME = 'region'
        self.setup.VARIABLE_COLUMN_NAME = 'entity'
        #        self.setup.INDEX_COLUMN_NAME = ['FLOW', 'PRODUCT']
        #        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        #        self.setup.COLUMNS_TO_DROP = [ 'PRODUCT','FLOW','combined']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = dict()
            for var in ['entity', 'scenario', 'region']:
                df = pd.read_excel(
                    self.setup.MAPPING_FILE, sheet_name=var + '_mapping', index_col=0
                )
                df = df.loc[~df.loc[:, var].isna()]
                self.mapping.update(df.to_dict())
        self.createSourceMeta()

    def loadData(self):

        for i, fileKey in enumerate(self.setup.DATA_FILE.keys()):

            file = self.setup.DATA_FILE[fileKey]
            print(file)
            if not os.path.exists(file):
                continue
            temp = pd.read_csv(
                file, engine='python', index_col=None, header=0, encoding="ISO-8859-1"
            )
            temp.Element = temp.Element.apply(lambda x: fileKey + x)
            if not hasattr(self, 'data'):
                self.data = temp
            else:
                self.data = self.data.append(temp)

        self.data.loc[:, 'region'] = self.data.Area
        self.data.loc[:, 'entity'] = self.data.Element + '_' + self.data.Item
        self.data.loc[:, 'scenario'] = 'Historic'
        self.data.loc[:, 'model'] = ''
        index_keep = self.data.index[self.data['Source'] == 'FAO TIER 1']
        self.data = self.data.loc[index_keep]

        newColumns = ['region', 'entity', 'scenario', 'model', 'Unit']
        self.timeColumns = list()
        for column in self.data.columns:
            if column.startswith('Y') and len(column) == 5:
                self.data.loc[:, int(column[1:])] = self.data.loc[:, column]
                newColumns.append(int(column[1:]))
                self.timeColumns.append(int(column[1:]))

        self.data = self.data.loc[:, newColumns]

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()
        #        return None

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates('entity').set_index(
            self.setup.VARIABLE_COLUMN_NAME
        )['Unit']
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=[self.setup.VARIABLE_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        #        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)
        #
        #        self.availableSeries = pd.DataFrame(index=index)
        #        self.mapping = pd.DataFrame(index=index, columns = [self.setup.MODEL_COLUMN_NAME])
        #        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        #        self.mapping = self.mapping.sort_index()
        #        self.mapping.index.name = 'orignal'
        #        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)
        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.SCENARIO_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')

        # region
        index = np.unique(self.data[self.setup.REGION_COLUMN_NAME].values)
        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.REGION_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.index.name = 'orignal'
        for idx in self.mapping.index:
            iso = dt.util.identifyCountry(idx)
            if iso is not None:
                self.mapping.loc[idx, 'region'] = iso

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='region_mapping')
        writer.close()

    def gatherMappedData(self, spatialSubSet=None):

        import tqdm

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        for model in ['']:
            tempMo = self.data

            #            tempMo = self.data.loc[self.data.model == model]
            for scenario in self.mapping['scenario'].keys():
                tempMoSc = tempMo.loc[tempMo.scenario == scenario]
                #                for variable in self.mapping['variable'].keys():
                #                    tempMoScVa = tempMoSc.loc[self.data.variable == variable]

                for variable in list(self.mapping['entity'].keys()):
                    tempMoScVar = tempMoSc.loc[tempMoSc.entity == variable]
                    tempMoScVar.unit = self.mapping['unit'][variable]
                    #                    tables = dt.interfaces.read_long_table(tempMoScVar, [variable])

                    table = tempMoScVar.loc[:, self.timeColumns]

                    table = Datatable(
                        table,
                        meta={
                            'entity': self.mapping['entity'][variable],
                            'category': self.mapping['category'][variable],
                            'scenario': scenario,
                            'source': self.setup.SOURCE_ID,
                            'unit': self.mapping['unit'][variable],
                        },
                    )
                    table.index = tempMoScVar.region
                    #                    table.meta['category'] = ""
                    #                    table.meta['source'] =
                    table.index = table.index.map(self.mapping['region'])

                    table = table.loc[~pd.isna(table.index), :]

                    if not pd.isna(self.mapping['unitTo'][variable]):
                        print(
                            'conversion to : ' + str(self.mapping['unitTo'][variable])
                        )
                        table = table.convert(self.mapping['unitTo'][variable])

                    tableID = dt.core._createDatabaseID(table.meta)
                    tablesToCommit.append(table)

        return tablesToCommit, excludedTables


class WEO(BaseImportTool):
    """
    WEO data import tool
    """

    def __init__(self, year):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "WEO_" + str(year)
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, f'rawdata/WEO/{year}'
        )
        self.setup.DATA_FILE = f'WEO{year}_AnnexA.xlsx'
        self.setup.MAPPING_FILE = os.path.join(
            self.setup.SOURCE_PATH, f'mapping_WEO_{year}.xlsx'
        )
        self.setup.LICENCE = 'IEA all rights reserved'
        self.setup.URL = 'https://www.iea.org/weo/'

        #        self.setup.INDEX_COLUMN_NAME = ['FLOW', 'PRODUCT']
        #        self.setup.SPATIAL_COLUM_NAME = 'COUNTRY'
        #        self.setup.COLUMNS_TO_DROP = [ 'PRODUCT','FLOW','combined']

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            #            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mappingEnergy = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name='Energy_Balance'
            )
            self.mappingEmissions = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name='SI_CO2_Ind'
            )

            self.spatialMapping = pd.read_excel(
                self.setup.MAPPING_FILE, sheet_name='spatial'
            )

        self.createSourceMeta()

    def addSpatialMapping(self):
        # EU
        mappingToCountries = dict()
        # self.spatialMapping.loc['Memo: European Union-28'] = 'EU28'
        mappingToCountries['ASEAN'] = [
            'VNM',
            'PHL',
            'THA',
            'SGP',
            'MMR',
            'IDN',
            'KHM',
            'BRN',
            'MYS',
            'LAO',
        ]
        dt.mapp.regions.addRegionToContext('WEO', mappingToCountries)

    def gatherMappedData(self, spatialSubSet=None):

        tablesToCommit = dt.TableSet()
        setup = dict()
        setup['filePath'] = self.setup.SOURCE_PATH
        setup['fileName'] = self.setup.DATA_FILE

        # loop over energy mapping
        for region in self.spatialMapping.index:
            setup['sheetName'] = region + '_Balance'
            for i, idx in enumerate(list(self.mappingEnergy.index)):
                #                print(i)
                metaDf = self.mappingEnergy.loc[idx, :]
                print(metaDf['What'])

                # Capacity
                setup['timeIdxList'] = tuple(metaDf['Time'].split(':'))
                setup['spaceIdxList'] = tuple([metaDf['What']])
                # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
                # print(metaData[self.setup.INDEX_COLUMN_NAME])

                if i == 0:
                    ex = dt.io.ExcelReader(setup)
                else:
                    ex.timeIdxList = [
                        dt.io.excelIdx2PandasIdx(x) for x in setup['timeIdxList']
                    ]
                    ex.spaceIdxList = [
                        dt.io.excelIdx2PandasIdx(x) for x in setup['spaceIdxList']
                    ]
                    # print(ex.df)
                #                if "International" in metaDf['Name']:
                #                    sdf
                if ex.spaceIdxList[0][0] > 41 and region != 'World':
                    ex.spaceIdxList = [(ex.spaceIdxList[0][0] - 1, 0)]

                #                print(ex.setup())
                if i == 0:
                    df = ex.gatherData()
                else:
                    df = ex.gatherData(load=False)
                print(df)
                df = df.loc[:, yearsColumnsOnly(df)]
                df.columns = df.columns.astype(int)
                metaDict = dict()
                metaDict['entity'] = metaDf['Name'].strip().replace('| ', '|')
                metaDict['category'] = ''
                metaDict['unit'] = metaDf['unit']
                metaDict['scenario'] = metaDf['Scenario']
                metaDict['source'] = self.setup.SOURCE_ID
                metaDict['unitTo'] = metaDf['unitTo']
                metaDict = dt.core._update_meta(metaDict)
                ID = dt.core._createDatabaseID(metaDict)
                coISO = self.spatialMapping.loc[region, 'mapping']
                if ID not in tablesToCommit.keys():
                    table = dt.Datatable(columns=range(2000, 2100), meta=metaDict)

                    table.loc[coISO, df.columns] = df.values
                    tablesToCommit.add(table)
                else:
                    table = tablesToCommit[ID]
                    table.loc[coISO, df.columns] = df.values

            # CO2 and emission indicators

            setup['sheetName'] = region + '_El_CO2_Ind'
            for i, idx in enumerate(list(self.mappingEmissions.index)):
                metaDf = self.mappingEmissions.loc[idx, :]
                print(metaDf['What'])

                # Capacity
                setup['timeIdxList'] = tuple(metaDf['Time'].split(':'))
                setup['spaceIdxList'] = tuple([metaDf['What']])
                # print(metaDf[config.REQUIRED_META_FIELDS].isnull().all() == False)
                # print(metaData[self.setup.INDEX_COLUMN_NAME])

                if i == 0:
                    ex = dt.io.ExcelReader(setup)
                #                    asd
                else:
                    ex.timeIdxList = [
                        dt.io.excelIdx2PandasIdx(x) for x in setup['timeIdxList']
                    ]
                    ex.spaceIdxList = [
                        dt.io.excelIdx2PandasIdx(x) for x in setup['spaceIdxList']
                    ]
                #                if "International" in metaDf['Name']:
                #                    sdf
                if ex.spaceIdxList[0][0] > 51 and region != 'World':
                    ex.spaceIdxList = [(ex.spaceIdxList[0][0] - 1, 0)]

                if i == 0:
                    df = ex.gatherData()
                else:
                    df = ex.gatherData(load=False)
                print(df)
                df = df.loc[:, yearsColumnsOnly(df)]
                df.columns = df.columns.astype(int)
                metaDict = dict()
                metaDict['entity'] = metaDf['Name'].strip().replace('| ', '|')
                metaDict['category'] = ''
                metaDict['unit'] = metaDf['unit']
                metaDict['scenario'] = metaDf['Scenario']
                metaDict['source'] = self.setup.SOURCE_ID
                metaDict['unitTo'] = metaDf['unitTo']

                metaDict = dt.core._update_meta(metaDict)
                ID = dt.core._createDatabaseID(metaDict)
                coISO = self.spatialMapping.loc[region, 'mapping']
                if ID not in tablesToCommit.keys():
                    table = dt.Datatable(columns=range(2000, 2100), meta=metaDict)

                    table.loc[coISO, df.columns] = df.values
                    tablesToCommit.add(table)
                else:
                    table = tablesToCommit[ID]
                    table.loc[coISO, df.columns] = df.values

        tablesList = list()
        for ID in tablesToCommit.keys():
            dataTable = tablesToCommit[ID]
            print(dataTable.meta)
            if not pd.isna(dataTable.meta['unitTo']):
                dataTable = dataTable.convert(dataTable.meta['unitTo'])
                del dataTable.meta['unitTo']
            tablesList.append(dataTable.astype(float))

        return tablesList, []


class ENERDATA(BaseImportTool):
    def __init__(self, year=2019, data_path=None, filename=None):

        if data_path is None:
            data_path = os.path.join(config.PATH_TO_DATASHELF, 'rawdata')

        if filename is None:
            filename = f'enerdata_{year}.xlsx'
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "ENERDATA_" + str(year)
        self.setup.SOURCE_PATH = os.path.join(data_path, self.setup.SOURCE_ID)

        self.setup.DATA_FILE = os.path.join(self.setup.SOURCE_PATH, filename)
        self.setup.MAPPING_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'mapping_' + str(year) + '.xlsx'
        )
        self.setup.LICENCE = (
            ' Restricted use in the Climate Transparency Report project only'
        )
        self.setup.URL = 'https://www.enerdata.net/user/?destination=services.html'

        self.setup.REGION_COLUMN_NAME = 'ISO code'
        self.setup.VARIABLE_COLUMN_NAME = 'Item code'

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = dict()

            for var in ['entity', 'region']:
                df = pd.read_excel(
                    self.setup.MAPPING_FILE, sheet_name=var + '_mapping', index_col=0
                )
                df = df.loc[~df.loc[:, var].isna()]
                self.mapping.update(df.to_dict())

        self.createSourceMeta()

    def loadData(self):
        self.data = pd.read_excel(
            self.setup.DATA_FILE, index_col=None, header=0, na_values='n.a.'
        )

        # fix doouble entry of solar
        mask = (self.data.loc[:, 'Item code'] == 'edvpd') & (
            self.data.loc[:, 'Unit'] == 'Mtoe'
        )
        ind_to_dropp = self.data.index[mask]
        self.data = self.data.drop(ind_to_dropp)

        self.data.loc[:, 'region'] = self.data.loc[:, self.setup.REGION_COLUMN_NAME]
        self.data.loc[:, 'entity'] = self.data.loc[:, self.setup.VARIABLE_COLUMN_NAME]
        self.data.loc[:, 'scenario'] = 'Historic'

        self.timeColumns = list()
        for col in self.data.columns:
            if isinstance(col, int):
                self.timeColumns.append(col)

    #        self.data.loc[:,'model'] = ''
    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates(
            self.setup.VARIABLE_COLUMN_NAME
        ).set_index(self.setup.VARIABLE_COLUMN_NAME)[['Unit', 'Title']]
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=['entity', 'category', 'unitTo']
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        #        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)
        #
        #        self.availableSeries = pd.DataFrame(index=index)
        #        self.mapping = pd.DataFrame(index=index, columns = [self.setup.MODEL_COLUMN_NAME])
        #        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        #        self.mapping = self.mapping.sort_index()
        #
        #        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # scenarios
        #        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)
        #
        #        self.availableSeries = pd.DataFrame(index=index)
        #        self.mapping = pd.DataFrame(index=index, columns = [self.setup.SCENARIO_COLUMN_NAME])
        #        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        #        self.mapping = self.mapping.sort_index()
        #        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')

        # region
        index = self.data[self.setup.REGION_COLUMN_NAME].unique()

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(index=index, columns=['region'])
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()

        for idx in self.mapping.index:
            if not isinstance(idx, (str, int)):
                continue
            iso = dt.util.identifyCountry(idx)
            if iso is not None:
                self.mapping.loc[idx, 'region'] = iso

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='region_mapping')
        writer.close()

    def gatherMappedData(self, spatialSubSet=None):

        import tqdm

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        # fix double entries for power generation wind
        wind_mask = self.data.entity == 'eeopd'
        Mtoe_mask = self.data.Unit == 'Mtoe'
        mask = wind_mask & Mtoe_mask
        self.data = self.data.drop(self.data.index[mask])

        for variable in list(self.mapping['entity'].keys()):
            #            metaDf = self.mapping.loc[variable]
            tempMoScVar = self.data.loc[self.data.entity == variable]
            tempMoScVar.unit = self.mapping['unit'][variable]
            #                    tables = dt.interfaces.read_long_table(tempMoScVar, [variable])

            table = tempMoScVar.loc[:, self.timeColumns]

            table = Datatable(
                table,
                meta={
                    'entity': self.mapping['entity'][variable],
                    'category': self.mapping['category'][variable],
                    'scenario': 'Historic',
                    'source': self.setup.SOURCE_ID,
                    'unit': self.mapping['unit'][variable],
                    'original code': variable,
                },
            )
            table.index = tempMoScVar.region
            #                    table.meta['category'] = ""
            #                    table.meta['source'] =
            table.index = table.index.map(self.mapping['region'])

            table = table.loc[~pd.isna(table.index), :]
            if not pd.isna(self.mapping['unitTo'][variable]):
                print('conversion to : ' + str(self.mapping['unitTo'][variable]))
                table = table.convert(self.mapping['unitTo'][variable])
            tableID = dt.core._createDatabaseID(table.meta)
            tablesToCommit.append(table)

        return tablesToCommit, excludedTables


class PIK_NDC(BaseImportTool):
    def __init__(self, year, version):
        self.setup = setupStruct()

        self.setup.SOURCE_ID = "PIK_NDC_" + str(year)
        self.setup.SOURCE_NAME = "PIK_NDC"
        self.setup.SOURCE_YEAR = str(year)

        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, f'rawdata/PIK_NDC_{year}'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH,
            f'ndc_targets_pathways_per_country_used_for_group_pathways_{version}.csv',
        )
        # self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = 'CC BY-4.0'
        self.setup.URL = 'https://zenodo.org/record/5113987'
        self.version = version

    def gatherMappedData(self):

        pik_data = pd.read_csv(
            self.setup.DATA_FILE,
        )
        print(pik_data.head())

        # remove countries that do not have a calcualted target
        mask = pik_data.add_info.str.startswith('No targets calculated')
        idx_to_drop = pik_data.index[mask == True]

        pik_data = pik_data.drop(idx_to_drop)

        pik_data = pik_data.set_index(['condi', 'rge'])

        pledge_low = pik_data[
            (pik_data.index.get_level_values('condi') == 'conditional')
            & (pik_data.index.get_level_values('rge') == 'best')
        ]
        pledge_low = pledge_low.set_index(pledge_low.loc[:, 'iso3']).loc[
            :, [str(x) for x in range(1990, 2031)]
        ]
        pledge_low = dt.Datatable(
            pledge_low,
            meta={
                'entity': 'Emissions|KYOTOGHG_AR4',
                'category': 'National_total_excl_LULUCF',
                'model': f'PIK_NDCmitiQ|{self.version}',
                'scenario': 'Pledge_low',
                'source': self.setup.SOURCE_ID,
                'unit': 'Mt CO2eq',
            },
        )

        pledge_high = pik_data[
            (pik_data.index.get_level_values('condi') == 'unconditional')
            & (pik_data.index.get_level_values('rge') == 'worst')
        ]
        pledge_high = pledge_high.set_index(pledge_high.loc[:, 'iso3']).loc[
            :, [str(x) for x in range(1990, 2031)]
        ]
        pledge_high = dt.Datatable(
            pledge_high,
            meta={
                'entity': 'Emissions|KYOTOGHG_AR4',
                'category': 'National_total_excl_LULUCF',
                'model': f'PIK_NDCmitiQ|{self.version}',
                'scenario': 'Pledge_high',
                'source': self.setup.SOURCE_ID,
                'unit': 'Mt CO2eq',
            },
        )

        return [pledge_high, pledge_low], None


import pyam
from tqdm import tqdm


class IIASA(BaseImportTool):
    def __init__(self, source_ID, iiasa_source=None, data_file=None, meta_file=None):
        self.setup = setupStruct()

        self.setup.SOURCE_ID = source_ID

        self.setup.DATA_FILE = ''
        # self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = '?'  # TODO
        self.setup.URL = 'data.ene.iiasa.ac.at'

        self.SOURCE_ID = source_ID

        if iiasa_source is not None:
            self.conn = pyam.iiasa.Connection(iiasa_source)
            self.iiasa_source = iiasa_source
            self.meta = self.conn.meta()
            self.api = True
            self.mapping_key = iiasa_source

        elif data_file is not None:
            self.data_file = data_file
            self.meta_file = meta_file
            self.meta = pd.DataFrame()
            self.api = False
            self.mapping_key = source_ID

        self.region_mapping = dict()

        
        
        self.region_mapping['iamc15'] =mapping.IPCC_SR15().region_mapping()
        self.region_mapping['IPCC_SR15'] = self.region_mapping['iamc15']
        self.region_mapping["IAMC15_2019_R2"] = self.region_mapping["iamc15"]  
        
        self.region_mapping['CD_LINKS'] =mapping.CD_LINKS().region_mapping()
        
        self.region_mapping['engage'] =mapping.ENGAGE().region_mapping()
        
        self.region_mapping['IPCC_AR5'] = mapping.IPCC_AR5().region_mapping()

        self.region_mapping['IPCC_AR6'] = mapping.IPCC_AR6().region_mapping()
        self.region_mapping['IPCC_AR6_raw'] = mapping.IPCC_AR6().region_mapping()

        self.region_mapping['ngfs_2'] = mapping.NGFS().region_mapping()

        self.region_mapping['ADVANCE'] = mapping.ADVANCE().region_mapping()

    def adapt_units(self, meta):
        meta['unit'] = (
            meta['unit']
            .replace('ktU', 'kt U')
            .replace('Mt CO2-equiv/yr', 'Mt CO2eq/yr')
            .replace('kt CF4-equiv/y', 'kt CF4eq/y')
            .replace('kt HFC134a-equiv/yr', 'kt HFC134aeq/yr')
            .replace('Index (2005 = 1)', 'dimensionless')  # ?? TODO
            .replace('US$', 'USD')
            .replace('kt HFC43-10/yr', 'kt HFC43_10/yr')
        )
        return meta

    def gatherMappedData(self, models=None, iamc_filter=[]):

        tables = list()
        tables_excluded = list()

        if not hasattr(self, 'data'):
            if not self.api:
                # load data
                self.loadData()
        
        if 'model' not in iamc_filter.keys():
            
            
            if models is None:
    
                if self.api:
                    models = self.meta.index.get_level_values(0).unique()
                else:
                    models = self.data.model
    
            for model in tqdm(models, desc='Loop over models'):
    
                if self.api:
                    idf = self.conn.query(**iamc_filter, model=model)
                else:
                    idf = self.data.filter(**iamc_filter, model=model)
                self.idf = idf
                if len(idf) == 0:
                    continue
    
                tables, tables_excluded = self._read_idf_data(
                    idf, model, tables, tables_excluded
                )
        else:
           if self.api:
               idf = self.conn.query(**iamc_filter)
           else:
               idf = self.data.filter(**iamc_filter)
           self.idf = idf
           
   
           for model in idf.model: 
               idf_sel = idf.filter(model=model)
               if len(idf_sel) == 0:
                   continue
               tables, tables_excluded = self._read_idf_data(
                   idf_sel, model, tables, tables_excluded)
                

        return tables, tables_excluded

    def _read_data(self, data_file, meta_file=None, **filters):
        print(f'reading in:  {data_file}')
        data = pyam.IamDataFrame(dt.tools.pyam.read_partial(data_file, **filters))
        if meta_file is not None:
            data.load_meta(meta_file)

        return data

    def loadData(self, **filters):

        # self.data = pd.read_csv(self.data_file)
        if isinstance(self.data_file, list) and isinstance(self.data_file, list):
            # multiple files
            data_list = list()
            for data_file, meta_file in zip(self.data_file, self.meta_file):

                data_list.append(self._read_data(data_file, meta_file, **filters))

            self.data = pyam.concat(data_list)
            self.meta = self.data.meta
            del data_list
            # del data

        else:
            # single file
            self.data = self._read_data(self.data_file, self.meta_file, **filters)
        # setting meta
        self.meta = self.data.meta

    def _read_idf_data(self, idf, model, tables, tables_excluded):

        wdf = idf.timeseries()
        for scenario in wdf.index.get_level_values(1).unique():
            sub_scenario = wdf.loc[slice(None), [scenario], slice(None), slice(None)]

            for var in sub_scenario.index.get_level_values(3).unique():
                subset = (
                    sub_scenario.loc[slice(None), slice(None), slice(None), [var]]
                    .reset_index()
                    .set_index('region')
                )

                time_cols = dt.util.yearsColumnsOnly(subset.columns)
                meta_cols = subset.columns.difference(time_cols)

                table = dt.Datatable(subset.loc[:, time_cols])
                for meta_key in meta_cols:
                    table.meta[meta_key] = list(subset[meta_key].unique())[0]
                if (model, scenario) in self.meta.index:
                    for meta_col, value in self.meta.loc[(model, scenario), :].items():

                        # manually change meta category to avoid overwrite of datatoolbox category
                        if meta_col == 'category':
                            meta_col = 'climate_category'
                        if meta_col not in config.ID_FIELDS:
                            table.meta[meta_col] = value
                        else:
                            print(
                                'Warming: could not set meta key {meta_col} since it is part of ID fields'
                            )
                table.meta['source'] = self.SOURCE_ID

                table = self.add_standard_region(
                    table, self.region_mapping[self.mapping_key]
                )
                table.meta = self.adapt_units(table.meta)

                table.generateTableID()
                if dt.core._validate_unit(table):
                    tables.append(table)
                else:
                    tables_excluded.append(table)
        return tables, tables_excluded

    def get_region_mapping(self):
        return self.region_mappire


class CAT_Paris_Sector_Rollout(BaseImportTool):
    def __init__(self):
        self.setup = setupStruct()
        self.setup.SOURCE_ID = "CAT_PSR_2019"
        self.setup.SOURCE_PATH = os.path.join(
            config.PATH_TO_DATASHELF, 'rawdata/CAT_PSR_2019'
        )
        self.setup.DATA_FILE = os.path.join(
            self.setup.SOURCE_PATH, 'portal_data_all_160620.csv'
        )
        self.setup.MAPPING_FILE = os.path.join(self.setup.SOURCE_PATH, 'mapping.xlsx')
        self.setup.LICENCE = ' Creative Commons Attribution 3.0 License'
        self.setup.URL = 'https://climateactiontracker.org/data-portal/'

        self.colums_to_process = ['variable', 'scenario', 'region']

        #        self.setup.MODEL_COLUMN_NAME = 'model'
        self.setup.SCENARIO_COLUMN_NAME = 'variable'
        self.setup.REGION_COLUMN_NAME = 'country'
        self.setup.VARIABLE_COLUMN_NAME = 'old_var'

        self.setup.columnMapping = {
            'variable': 'entity',
            'region': 'region',
            'scenario': 'scenario',
        }

        if not (os.path.exists(self.setup.MAPPING_FILE)):
            self.createVariableMapping()
            print("no mapping file found")
        else:
            self.mapping = dict()

            for var in self.colums_to_process:
                df = pd.read_excel(
                    self.setup.MAPPING_FILE, sheet_name=var + '_mapping', index_col=0
                )
                df = df.loc[~df.loc[:, self.setup.columnMapping[var]].isna()]
                self.mapping.update(df.to_dict())

        self.createSourceMeta()

    def loadData(self):
        self.data = pd.read_csv(self.setup.DATA_FILE, index_col=None, header=0)
        self.data.loc[:, 'old_var'] = self.data.loc[:, ['sector', 'indicator']].apply(
            lambda x: '_'.join(map(str, x)), axis=1
        )
        self.data.loc[:, 'scenario'] = self.data.loc[:, 'variable']
        self.data.loc[:, 'region'] = self.data.loc[:, 'country']
        self.data.loc[:, 'model'] = ''
        self.data.loc[:, 'variable'] = self.data.loc[:, 'old_var']

    def createVariableMapping(self):

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        import numpy as np

        writer = pd.ExcelWriter(
            self.setup.MAPPING_FILE,
            engine='xlsxwriter',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy',
        )

        # variables
        # index = self.data[self.setup.VARIABLE_COLUMN_NAME].unique()
        self.availableSeries = self.data.drop_duplicates(
            self.setup.VARIABLE_COLUMN_NAME
        ).set_index(self.setup.VARIABLE_COLUMN_NAME)['unit']
        self.mapping = pd.DataFrame(
            index=self.availableSeries.index, columns=['enitty', 'category']
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name=VAR_MAPPING_SHEET)

        # models
        #        index = np.unique(self.data[self.setup.MODEL_COLUMN_NAME].values)

        #        self.availableSeries = pd.DataFrame(index=index)
        #        self.mapping = pd.DataFrame(index=index, columns = [self.setup.MODEL_COLUMN_NAME])
        #        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        #        self.mapping = self.mapping.sort_index()
        #
        #        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='model_mapping')

        # scenarios
        index = np.unique(self.data[self.setup.SCENARIO_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.SCENARIO_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()
        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='scenario_mapping')

        # region
        index = np.unique(self.data[self.setup.REGION_COLUMN_NAME].values)

        self.availableSeries = pd.DataFrame(index=index)
        self.mapping = pd.DataFrame(
            index=index, columns=[self.setup.REGION_COLUMN_NAME]
        )
        self.mapping = pd.concat([self.mapping, self.availableSeries], axis=1)
        self.mapping = self.mapping.sort_index()

        for idx in self.mapping.index:
            iso = dt.util.identifyCountry(idx)
            if iso is not None:
                self.mapping.loc[idx, 'region'] = iso

        self.mapping.to_excel(writer, engine='openpyxl', sheet_name='region_mapping')
        writer.close()

    def gatherMappedData(self, spatialSubSet=None, updateTables=False):

        import tqdm

        # loading data if necessary
        if not hasattr(self, 'data'):
            self.loadData()

        tablesToCommit = []
        metaDict = dict()
        metaDict['source'] = self.setup.SOURCE_ID
        excludedTables = dict()
        excludedTables['empty'] = list()
        excludedTables['error'] = list()
        excludedTables['exists'] = list()

        #        self.data = self.data

        for scenario in self.mapping['scenario'].keys():
            tempMoSc = self.data.loc[self.data.scenario == scenario]
            for variable in self.mapping['entity'].keys():
                tempMoScVa = tempMoSc.loc[tempMoSc.variable == variable]
                #                for category in self.mapping['category'].keys():
                #                tempMoScVa = tempMoScVa.loc[tempMoScVa.variable == category]
                tempMoScVa = tempMoScVa.loc[
                    :,
                    [
                        'unit',
                        'model',
                        'scenario',
                        'year',
                        'region',
                        'value',
                        'variable',
                    ],
                ]
                tempMoScVa.loc[:, 'unit'] = self.mapping['unit'][variable]
                tempMoScVa.loc[:, 'scenario'] = self.mapping['scenario'][scenario]
                ##                    dfg
                tables = dt.interfaces.read_long_table(tempMoScVa, [variable])
                for table in tables:
                    table.meta['entity'] = self.mapping['entity'][variable]
                    table.meta['category'] = self.mapping['category'][variable]
                    table.meta['scenario'] = self.mapping['scenario'][scenario]
                    table.meta['source'] = self.setup.SOURCE_ID
                    table.index = table.index.map(self.mapping['region'])

                    tableID = table.generateTableID()
                    if not updateTables:
                        if dt.core.DB.tableExist(tableID):
                            excludedTables['exists'].append(tableID)
                        else:
                            tablesToCommit.append(table)
        return tablesToCommit, excludedTables


#%% Import functions


def HDI_import(year=2020):
    sourceMeta = {
        'SOURCE_ID': 'HDI_' + str(year),
        'collected_by': 'AG',
        'date': dt.core.getDateString(),
        'source_url': 'http://hdr.undp.org/en/data',
        'licence': 'open source',
    }

    SOURCE_PATH = os.path.join(
        dt.config.PATH_TO_DATASHELF, 'rawdata', sourceMeta['SOURCE_ID']
    )
    data = pd.read_csv(
        os.path.join(SOURCE_PATH, "Human Development Index (HDI).csv"),
        na_values='..',
    )

    yearColumns = dt.util.yearsColumnsOnly(data)

    data = data.loc[:, ['Country'] + yearColumns]

    #    regionMapping = {x:y for x,y in zip(list(data.loc[:,'Country']), list(data.loc[:,'Country'].map(lambda x : dt.getCountryISO(str(x)))))}
    regionMapping = {
        ' Afghanistan': 'AFG',
        ' Albania': 'ALB',
        ' Algeria': 'DZA',
        ' Andorra': 'AND',
        ' Angola': 'AGO',
        ' Antigua and Barbuda': 'ATG',
        ' Argentina': 'ARG',
        ' Armenia': 'ARM',
        ' Australia': 'AUS',
        ' Austria': 'AUT',
        ' Azerbaijan': 'AZE',
        ' Bahamas': 'BHS',
        ' Bahrain': 'BHR',
        ' Bangladesh': 'BGD',
        ' Barbados': 'BRB',
        ' Belarus': 'BLR',
        ' Belgium': 'BEL',
        ' Belize': 'BLZ',
        ' Benin': 'BEN',
        ' Bhutan': 'BTN',
        ' Bolivia (Plurinational State of)': 'BOL',
        ' Bosnia and Herzegovina': 'BIH',
        ' Botswana': 'BWA',
        ' Brazil': 'BRA',
        ' Brunei Darussalam': 'BRN',
        ' Bulgaria': 'BGR',
        ' Burkina Faso': 'BFA',
        ' Burundi': 'BDI',
        ' Cabo Verde': 'CPV',
        ' Cambodia': 'KHM',
        ' Cameroon': 'CMR',
        ' Canada': 'CAN',
        ' Central African Republic': 'CAF',
        ' Chad': 'TCD',
        ' Chile': 'CHL',
        ' China': 'CHN',
        ' Colombia': 'COL',
        ' Comoros': 'COM',
        ' Congo': 'COG',
        ' Congo (Democratic Republic of the)': 'COD',
        ' Costa Rica': 'CRI',
        ' Croatia': 'HRV',
        ' Cuba': 'CUB',
        ' Cyprus': 'CYP',
        ' Czechia': 'CZE',
        " Côte d'Ivoire": 'CIV',
        ' Denmark': 'DNK',
        ' Djibouti': 'DJI',
        ' Dominica': 'DMA',
        ' Dominican Republic': 'DOM',
        ' Ecuador': 'ECU',
        ' Egypt': 'EGY',
        ' El Salvador': 'SLV',
        ' Equatorial Guinea': 'GNQ',
        ' Eritrea': 'ERI',
        ' Estonia': 'EST',
        ' Eswatini (Kingdom of)': 'SWZ',
        ' Ethiopia': 'ETH',
        ' Fiji': 'FJI',
        ' Finland': 'FIN',
        ' France': 'FRA',
        ' Gabon': 'GAB',
        ' Gambia': 'GMB',
        ' Georgia': 'GEO',
        ' Germany': 'DEU',
        ' Ghana': 'GHA',
        ' Greece': 'GRC',
        ' Grenada': 'GRD',
        ' Guatemala': 'GTM',
        ' Guinea': 'GIN',
        ' Guinea-Bissau': 'GNB',
        ' Guyana': 'GUY',
        ' Haiti': 'HTI',
        ' Honduras': 'HND',
        ' Hong Kong, China (SAR)': 'HKG',
        ' Hungary': 'HUN',
        ' Iceland': 'ISL',
        ' India': 'IND',
        ' Indonesia': 'IDN',
        ' Iran (Islamic Republic of)': 'IRN',
        ' Iraq': 'IRQ',
        ' Ireland': 'IRL',
        ' Israel': 'ISR',
        ' Italy': 'ITA',
        ' Jamaica': 'JAM',
        ' Japan': 'JPN',
        ' Jordan': 'JOR',
        ' Kazakhstan': 'KAZ',
        ' Kenya': 'KEN',
        ' Kiribati': 'KIR',
        ' Korea (Republic of)': 'KOR',
        ' Kuwait': 'KWT',
        ' Kyrgyzstan': 'KGZ',
        " Lao People's Democratic Republic": 'LAO',
        ' Latvia': 'LVA',
        ' Lebanon': 'LBN',
        ' Lesotho': 'LSO',
        ' Liberia': 'LBR',
        ' Libya': 'LBY',
        ' Liechtenstein': 'LIE',
        ' Lithuania': 'LTU',
        ' Luxembourg': 'LUX',
        ' Madagascar': 'MDG',
        ' Malawi': 'MWI',
        ' Malaysia': 'MYS',
        ' Maldives': 'MDV',
        ' Mali': 'MLI',
        ' Malta': 'MLT',
        ' Marshall Islands': 'MHL',
        ' Mauritania': 'MRT',
        ' Mauritius': 'MUS',
        ' Mexico': 'MEX',
        ' Micronesia (Federated States of)': 'FSM',
        ' Moldova (Republic of)': 'MDA',
        ' Mongolia': 'MNG',
        ' Montenegro': 'MNE',
        ' Morocco': 'MAR',
        ' Mozambique': 'MOZ',
        ' Myanmar': 'MMR',
        ' Namibia': 'NAM',
        ' Nepal': 'NPL',
        ' Netherlands': 'NLD',
        ' New Zealand': 'NZL',
        ' Nicaragua': 'NIC',
        ' Niger': 'NER',
        ' Nigeria': 'NGA',
        ' North Macedonia': 'MKD',
        ' Norway': 'NOR',
        ' Oman': 'OMN',
        ' Pakistan': 'PAK',
        ' Palau': 'PLW',
        ' Palestine, State of': 'PSE',
        ' Panama': 'PAN',
        ' Papua New Guinea': 'PNG',
        ' Paraguay': 'PRY',
        ' Peru': 'PER',
        ' Philippines': 'PHL',
        ' Poland': 'POL',
        ' Portugal': 'PRT',
        ' Qatar': 'QAT',
        ' Romania': 'ROU',
        ' Russian Federation': 'RUS',
        ' Rwanda': 'RWA',
        ' Saint Kitts and Nevis': 'KNA',
        ' Saint Lucia': 'LCA',
        ' Saint Vincent and the Grenadines': 'VCT',
        ' Samoa': 'WSM',
        ' Sao Tome and Principe': 'STP',
        ' Saudi Arabia': 'SAU',
        ' Senegal': 'SEN',
        ' Serbia': 'SRB',
        ' Seychelles': 'SYC',
        ' Sierra Leone': 'SLE',
        ' Singapore': 'SGP',
        ' Slovakia': 'SVK',
        ' Slovenia': 'SVN',
        ' Solomon Islands': 'SLB',
        ' South Africa': 'ZAF',
        ' South Sudan': 'SSD',
        ' Spain': 'ESP',
        ' Sri Lanka': 'LKA',
        ' Sudan': 'SDN',
        ' Suriname': 'SUR',
        ' Sweden': 'SWE',
        ' Switzerland': 'CHE',
        ' Syrian Arab Republic': 'SYR',
        ' Tajikistan': 'TJK',
        ' Tanzania (United Republic of)': 'TZA',
        ' Thailand': 'THA',
        ' Timor-Leste': 'TLS',
        ' Togo': 'TGO',
        ' Tonga': 'TON',
        ' Trinidad and Tobago': 'TTO',
        ' Tunisia': 'TUN',
        ' Turkey': 'TUR',
        ' Turkmenistan': 'TKM',
        ' Uganda': 'UGA',
        ' Ukraine': 'UKR',
        ' United Arab Emirates': 'ARE',
        ' United Kingdom': 'GBR',
        ' United States': 'USA',
        ' Uruguay': 'URY',
        ' Uzbekistan': 'UZB',
        ' Vanuatu': 'VUT',
        ' Venezuela (Bolivarian Republic of)': 'VEN',
        ' Viet Nam': 'VNM',
        ' Yemen': 'YEM',
        ' Zambia': 'ZMB',
        ' Zimbabwe': 'ZWE',
        'Human Development': None,
        'Very high human development': None,
        'High human development': None,
        'Medium human development': None,
        'Low human development': None,
        'Developing Countries': None,
        'Regions': None,
        'Arab States': None,
        'East Asia and the Pacific': None,
        'Europe and Central Asia': None,
        'Latin America and the Caribbean': None,
        'South Asia': None,
        'Sub-Saharan Africa': None,
        'Least Developed Countries': None,
        'Small Island Developing States': None,
        'Organization for Economic Co-operation and Development': None,
        'World': 'World',
    }

    data.index = data.loc[:, 'Country'].map(regionMapping)

    data.index[data.index.duplicated()]
    data = data.loc[:, yearColumns]
    data = data.loc[~data.index.isnull(), :].astype(float)

    meta = {
        'entity': 'HDI_Human_development_index',
        'scenario': 'Historic',
        'unit': 'dimensionless',
        'source': sourceMeta['SOURCE_ID'],
    }
    table = dt.Datatable(data, meta=meta)
    table.generateTableID()

    dt.commitTables([table], 'HDI data update', sourceMeta, update=True)


def UN_WPP_2019_import():
    sourceMeta = {
        'SOURCE_ID': 'UN_WPP2019',
        'collected_by': 'AG',
        'date': dt.core.getDateString(),
        'source_url': 'https://population.un.org/wpp/Download/Standard/Population/',
        'licence': 'open source',
    }

    mappingDict = {
        int(x): y
        for x, y in zip(dt.mapp.countries.codes.numISO, dt.mapp.countries.codes.index)
        if not (pd.np.isnan(x))
    }
    mappingDict[900] = 'World'
    SOURCE = "UN_WPP2019"
    SOURCE_PATH = os.path.join(dt.config.PATH_TO_DATASHELF, 'rawdata/UN_WPP2019')
    metaSetup = {
        'source': SOURCE,
        'entity': 'population',
        'unit': 'thousands',
        'category': 'total',
    }

    # change setup
    excelSetup = dict()
    excelSetup['filePath'] = SOURCE_PATH
    excelSetup['fileName'] = 'WPP2019_POP_F01_1_TOTAL_POPULATION_BOTH_SEXES.xlsx'
    excelSetup['sheetName'] = 'ESTIMATES'
    excelSetup['timeColIdx'] = ('H17', 'CJ17')
    excelSetup['spaceRowIdx'] = ('E18', 'E306')

    tables = list()

    # gather historic data
    #        itool = UN_WPP(excelSetup)
    metaSetup['scenario'] = 'historic'
    extractor = dt.io.ExcelReader(excelSetup)
    table = extractor.gatherData()
    table = table.replace('…', pd.np.nan)
    table['newIndex'] = table.index
    table['newIndex'] = table.index.to_series().map(mappingDict)
    table.loc[pd.isnull(table['newIndex']), 'newIndex'] = table.index[
        pd.isnull(table['newIndex'])
    ]
    table = table.set_index('newIndex')
    newIdx = [mappingDict[x] for x in table.index if x in mappingDict.keys()]
    tables.append(dt.Datatable(table, meta=metaSetup))

    # gather projectinos
    # excelSetup['timeColIdx']  = ('F17', 'CW17')

    scenDict = {
        'PROJECTION_LOW': 'LOW VARIANT',
        'PROJECTION_MED': 'MEDIUM VARIANT',
        'PROJECTION_HI': 'HIGH VARIANT',
    }
    # itool = UN_WPP(excelSetup)

    for scenario, sheetName in scenDict.items():
        metaSetup['scenario'] = scenario
        excelSetup['sheetName'] = sheetName
        extractor = dt.io.ExcelReader(excelSetup)
        table = extractor.gatherData()
        table = table.replace('…', pd.np.nan)
        table['newIndex'] = table.index
        table['newIndex'] = table.index.to_series().map(mappingDict)
        table.loc[pd.isnull(table['newIndex']), 'newIndex'] = table.index[
            pd.isnull(table['newIndex'])
        ]
        table = table.set_index('newIndex')
        tables.append(dt.Datatable(table, meta=metaSetup))
    #%%
    def add_EU(table):
        EU_COUNTRIES = list(dt.mapp.regions.EU28.membersOf('EU28'))
        table.loc['EU28'] = table.loc[EU_COUNTRIES].sum()
        return table

    tables = [add_EU(table) for table in tables]

    dt.commitTables(tables, 'UNWPP2017 data', sourceMeta, append_data=True, update=True)
    return tables


if config.DEBUG:
    print('Raw sources loaded in {:2.4f} seconds'.format(time.time() - tt))


#%% Import example
if __name__ == '__main__':
    # xdfg
    """
    Config
    """
    update_content = True

    """ 
    Initialize class:
    This will load the mapping file, read the data and prepare the additional
    meta informations.
    """
    # reader = PRIMAP_HIST(version="v2.3_no_rounding_28_Jul_2021", year=2021)
    # reader = PIK_NDC(2021, version = 'v1.0.2')
    # reader = PRIMAP(year = 2021)
    # reader = IIASA('ENGAGE_2021', 'engage')
    # reader = IIASA('NGFS_2021', 'ngfs_2')
    # reader = IIASA('CD_LINKS', 'cdlinks')
    # reader = IIASA('IPCC_AR5', data_file ='/media/sf_Documents/datashelf_v03/rawdata/AR5_database/ar5_public_version102_compare_compare_20150629-130000.csv')
    reader = IIASA(
        'ADVANCE',
        data_file='/media/sf_Documents/datashelf_v03/rawdata/ADVANCE_DB/ADVANCE_Synthesis_version101_compare_20190619-143200.csv',
    )
    # reader = IIASA('IPCC_SR15', 'iamc15')
    # reader = IIASA('IPCC_SR15',
    #                data_file ='/media/sf_Documents/datashelf_v03/rawdata/IAMC15_2019b/iamc15_scenario_data_all_regions_r2.0.xlsx',
    #                meta_file ='/media/sf_Documents/datashelf_v03/rawdata/IAMC15_2019b/sr15_metadata_indicators_r2.0.xlsx')
    # sdf
    """ 
    Process data:
    This will process the data according to the mapping file and fill a list of 
    tables which are to be added to the database.
    
    Excluded tables will be listed seperately for review.
    """
    # filter_var = lambda x : any([x.startswith(var) for var in ['Emissions',
    #                                                            'Primary',
    #                                                            'Secondary'
    #                                                            'GDP',
    #                                                            'Population']])
    iamc_filter = {
        # 'variable' : [
        # 'Emissions**',
        # 'Primary**',
        # 'Secondary**'
        # 'GDP**',
        # 'Population**'
        # 'Subsidies**',
        # 'Price**',
        # 'Investments**',
        # 'Carbon Sequestratio**',
        # 'Capacity**',
        # 'Cumulative Capacity**'
        # ]
    }
    # models = ['AIM/Hub-India 2.2',
    #           'AIM/Hub-Thailand 2.2',
    #           'AIM/Hub-Japan 2.1',
    #             'AIM/Hub-China 2.2',
    #             'AIM/Hub-Korea 2.0',
    #             'AIM/Hub-Vietnam 2.2',
    #             'AIM/CGE V2.2',
    #             'COFFEE 1.1']
    # models = ['TIAM-ECN 1.1',
    #     'MESSAGEix-GLOBIOM V1.2', 'MESSAGEix-GLOBIOM 1.1', 'WITCH 5.0',
    #     'GEM-E3 V2021', 'REMIND-MAgPIE 2.1-4.2', 'IMAGE 3.0',
    #     'POLES-JRC ENGAGE']

    # iamc_filter = {'variable' : [
    #                         'Emissions|CO2',
    #                         'Emissions|CH4',
    #                         'Emissions|N2O',
    #                         'Emissions|F-Gases',
    #                         'Emissions|Kyo**']}
    models = None
    # tables_to_commit, excludedTables = reader.gatherMappedData_test(models, iamc_filter=iamc_filter)
    tables_to_commit, excludedTables = reader.gatherMappedData(iamc_filter=iamc_filter)

    # For review:
    # tableIDs = [table.ID for table in tableList]
    # cleanTableList = [dt.tools.cleanDataTable(table) for table in tableList

    # find empty tablse
    # [x.ID for x in tableList if len(x.index) ==0]

    #%%
    # new_exclude_tables = list()
    # new_tables_to_commit = list()
    # for table in excludedTables:
    #     table.meta = adapt_meta(table.meta)

    #     if dt.core.check_table(table):
    #         new_exclude_tables.append(table)
    #     else:
    #         new_tables_to_commit.append(table)
    # #%%
    #   compare_regions = set()
    #   for table in tables_to_commit:
    #       compare_regions_table = [x for x in table.index if '(R' in x]
    #       compare_regions = compare_regions.union(set(compare_regions_table))

    #   #%%
    #   for table in tables_to_commit:
    #       table  = self.add_standard_region(table, self.region_mapping['engage'])
    #       sdf
    #%%
    # def standardise_regions(table):

    #%%
    # """
    # Update database:

    # This will add the tables in the list to the database and will also add the
    # mapping file to the repository.
    # """

    reader.update_database(tables_to_commit, updateContent=False)

#%%
