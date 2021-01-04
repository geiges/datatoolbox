#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 11:25:15 2019

@author: Andreas Geiges
"""
import pandas as pd
import numpy as np
from typing import Union, Iterable, Optional

import datatoolbox as dt
from datatoolbox import mapping as mapp
from datatoolbox import core
from datatoolbox import config
from datatoolbox import data_structures
from datatoolbox.greenhouse_gas_database import GreenhouseGasTable 
import matplotlib.pylab as plt
import os
import tqdm

import deprecated as dp
#from .tools import kaya_idendentiy_decomposition


try:
    from hdx.location.country import Country
    
    def getCountryISO(string):
        #print(string)
        try:
            string = string.replace('*','')
            results = Country.get_iso3_country_code_fuzzy(string)
            if len(results)> 0:
                return results[0]
            else:
                return None
        except:
            print('error for: ' + string)
        
except:
    def getCountryISO(string):
        print("the package hdx is not installed, thus this function is not available")
        print('use: "pip install hdx-python-country" to install')
        return None


from .tools import matplotlib as _plt

colorGenerator = dp.deprecated(_plt.colorGenerator, 
                               reason="please import from matplotlib toolkid", 
                               version='0.4.0')
savefig = dp.deprecated(_plt.savefig,
                               reason="please import from matplotlib toolkid", 
                               version='0.4.0')
from .tools import pandas as _pd

yearsColumnsOnly = dp.deprecated(_pd.yearsColumnsOnly,
                               reason="please import from pandas toolkid", 
                               version='0.4.0')
 
#%%
from .tools import html as _html

def export_to_pdf( dataframe, fileName):
    html = _html.export_to_html(dataframe)
    import pdfkit
    pdfkit.from_string(html, fileName)


GHG_data = GreenhouseGasTable()

def diff_eps(df1, df2, eps = 1e-6):
    """
    Identify differences between two Datatables higher than a defined treshold.
    
    Returns a pandas dataframe with all changes
    """
    assert (df1.columns == df2.columns).all(), \
        "DataFrame column names are different"
    if any(df1.dtypes != df2.dtypes):
        "Data Types are different, trying to convert"
        df2 = df2.astype(df1.dtypes)
    if df1.equals(df2):
        return None
    else:
        # need to account for np.nan != np.nan returning True
        diff_mask = ((df1 - df2).abs()>eps) & ~(((df1.isnull()) & df2.isnull()).isnull())
        ne_stacked = diff_mask.stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['id', 'col']
        difference_locations = np.where(diff_mask)
        changed_from = df1.values[difference_locations]
        changed_to = df2.values[difference_locations]
        return pd.DataFrame({'from': changed_from, 'to': changed_to},
                            index=changed.index)
        
def diff(df1, df2):
    """
    Identify differences between two Datatables.
    
    Returns a pandas dataframe with all changes
    """
    assert (df1.columns == df2.columns).all(), \
        "DataFrame column names are different"
    if any(df1.dtypes != df2.dtypes):
        "Data Types are different, trying to convert"
        df2 = df2.astype(df1.dtypes)
    if df1.equals(df2):
        return None
    else:
        # need to account for np.nan != np.nan returning True
        diff_mask = (df1 != df2) & ~(df1.isnull() & df2.isnull())
        ne_stacked = diff_mask.stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['id', 'col']
        difference_locations = np.where(diff_mask)
        changed_from = df1.values[difference_locations]
        changed_to = df2.values[difference_locations]
        return pd.DataFrame({'from': changed_from, 'to': changed_to},
                            index=changed.index)        



def isUnit(unit):
    try:
        core.getUnit(unit)
    except:
        return False
    else:
        return True

def scatterIndicatorComparison(tableX, tableY):
    timeCol = list(set(tableY.columns).intersection(set(tableX.columns)))
    for ISOcode in tableX.index:
        coName= mapp.countries.codes.name.loc[ISOcode]
    #    
    #    p = plt.plot(tableX.loc[ISOcode,timeCol4], tableY.loc[ISOcode,timeCol4],linewidth=.2)
    #    color = p[0].get_color()
    #    p = plt.scatter(tableX.loc[ISOcode,timeCol], tableY.loc[ISOcode,timeCol], facecolors=color, edgecolors=color)
    #    plt.scatter(tableX.loc[ISOcode,timeCol2], tableY.loc[ISOcode,timeCol2], s=40, facecolors='none', edgecolors=color)
    #    plt.scatter(tableX.loc[ISOcode,timeCol3], tableY.loc[ISOcode,timeCol3], s=40, facecolors='none', edgecolors=color)
    #    
        p = plt.scatter(tableX.loc[ISOcode,timeCol], tableY.loc[ISOcode,timeCol], s = 100)
        
        plt.text(tableX.loc[ISOcode,timeCol]+.1, tableY.loc[ISOcode,timeCol]+.2, coName)
    plt.xlabel(tableX.ID)
    plt.ylabel(tableY.ID)
    
    plt.xlim([0,60])
    plt.ylim([0,20])
    plt.tight_layout()

#def harmonize_IPCC(startYear, endYear, startValue=None, endValue=None):
#    
#    assert (startValue is not None) or (endValue is not None)
#    
#    if endValue is None:
#        
#        
#        e
    
def cleanDataTable(dataTable):
    # ensure valid ISO or region IDs
    invalidSpatialIDs = dataTable.index.difference(mapp.getValidSpatialIDs())
    if not invalidSpatialIDs.empty:
        print(
            f"Removing unknown regions from dataTable {dataTable.meta.get('ID', 'unnamed')}:",
            ", ".join(invalidSpatialIDs)
        )
        dataTable = dataTable.loc[dataTable.index.difference(invalidSpatialIDs)]
    
    dataTable = dataTable.dropna(how="all", axis=1).dropna(how="all", axis=0)

    # clean meta data
    keysToDelete = list()
    for key in dataTable.meta.keys():
        if np.any(pd.isna(dataTable.meta[key])):
            if key not in config.ID_FIELDS:
                keysToDelete.append(key)
            else:
                dataTable.meta[key] = ''
    for key in keysToDelete:
        del dataTable.meta[key]

    # ensure time colums to be integer
    dataTable.columns = dataTable.columns.astype(int)
    
    return dataTable




    
    
def identifyCountry(string):
    
    #numeric ISO code
    try:
        numISO = float(string)
        mask = numISO == mapp.countries.codes['numISO']
        if mask.any():
            return mapp.countries.index[mask][0]
    except:
        pass
    if len(string) == 2:
        mask = string == mapp.countries.codes['alpha2']
        if mask.any():
            return mapp.countries.codes.index[mask][0]
    
    if len(string) == 3:
         
        if string.upper() in mapp.countries.codes.index:
            return string.upper()
    
    try: 
        coISO = dt.getCountryISO(string)
        return coISO
    except:
        print('not matching country found')    
        return None
            
def convertIndexToISO(table):
    replaceDict = dict()
    
    for idx in table.index:
        iso = identifyCountry(idx)
        if iso is not None:
            replaceDict[idx] = iso
    table.index = table.index.map(replaceDict)           
    table = table.loc[~table.index.isna(),:]
  
    return table

def addCountryNames(table):
    names = list()
    for idx in table.index:
        if idx in mapp.countries.codes.index:
            names.append(mapp.countries.codes.loc[idx,'name'])
        else:
            names.append('')
    table.loc[:,'country_name'] = names
    return table




def update_source_from_file(fileName, message=None):
    sourceData = pd.read_csv(fileName)
    for index in sourceData.index:
        dt.core.DB._addNewSource(sourceData.loc[index,:].to_dict())
    
def update_DB_from_folder(folderToRead, message=None):
    fileList = os.listdir(folderToRead)
    fileList = [file for file in fileList if '.csv' in file[-5:].lower()]

    tablesToUpdate = dict()

    for file in fileList:
        table = dt.read_csv(os.path.join(folderToRead, file))
        source = table.meta['source']
        if source in tablesToUpdate.keys():


            tablesToUpdate[source].append(table)
        else:
            tablesToUpdate[source] = [table]
    if message is None:

        message = 'External data added from external source by ' + config.CRUNCHER
    
    for source in tablesToUpdate.keys():
        sourceMetaDict = dict()
        sourceMetaDict['SOURCE_ID']= source
        dt.commitTables(tablesToUpdate[source], 
                        message = message, 
                        sourceMetaDict = sourceMetaDict, 
                        append_data=True, 
                        update=True)


def update_DB_from_folderV3(folderToRead, message=None, cleanTables=True):
    import math
    fileList = os.listdir(folderToRead)
    fileList = [file for file in fileList if '.csv' in file[-5:].lower()]

    

    filesPerCommit = 5000
    nCommits = math.ceil((len(fileList))/filesPerCommit)
    for nCommit in range(nCommits):
        tablesToUpdate = dict()
        for file in fileList[nCommit*filesPerCommit:(nCommit+1)*filesPerCommit]:

            table = dt.read_csv(os.path.join(folderToRead, file))
            source = table.meta['source']
            
#            if not 'Emissions|CO2|Industrial' in table.ID:
#                continue
            if source in tablesToUpdate.keys():
    
    
                tablesToUpdate[source].append(table)
            else:
                tablesToUpdate[source] = [table]
        
        if message is None:
    
            message = 'External data added from external source by ' + config.CRUNCHER + '{}/{}'.format(nCommit,nCommits)
    
        for source in tablesToUpdate.keys():
    
            tablesToUpdate[source] = metaV2_to_meta_V3(tablesToUpdate[source])
#        return tablesToUpdate
    
        for source in tablesToUpdate.keys():
            sourceMetaDict = dict()
            sourceMetaDict['SOURCE_ID']= source
            core.DB.commitTables(tablesToUpdate[source], 
                            message = message,
                            sourceMetaDict = sourceMetaDict, 
                            cleanTables=cleanTables,
                            update=True)

#def update_DB_from_folderV3(folderToRead, message=None, cleanTables=True):
#    import math
#    fileList = os.listdir(folderToRead)
#    fileList = [file for file in fileList if '.csv' in file[-5:].lower()]
#
#    
#
##    filesPerCommit = 5000
##    nCommits = math.ceil((len(fileList))/filesPerCommit)
##    for nCommit in range(nCommits):
#    tablesToUpdate = dict()
#    for file in fileList:
#
#        table = dt.read_csv(os.path.join(folderToRead, file))
#        source = table.meta['source']
#        
#        if not 'Emissions|CO2|Industrial' in table.ID:
#                continue
#        if source in tablesToUpdate.keys():
#
#
#            tablesToUpdate[source].append(table)
#        else:
#            tablesToUpdate[source] = [table]
#    
##    if message is None:
##
##        message = 'External data added from external source by ' + config.CRUNCHER + '{}/{}'.format(nCommit,nCommits)
#
#    for source in tablesToUpdate.keys():
#
#        tablesToUpdate[source] = metaV2_to_meta_V3(tablesToUpdate[source])
#    return tablesToUpdate
    
#        for source in tablesToUpdate.keys():
#            sourceMetaDict = dict()
#            sourceMetaDict['SOURCE_ID']= source
#            core.DB.commitTables(tablesToUpdate[source], 
#                            message = message,
#                            sourceMetaDict = sourceMetaDict, 
#                            cleanTables=cleanTables)

def metaV2_to_meta_V3(tableSet):
    replacementDict = {#'Capacity': 'Electricity|capacity',
                   'Capacity|Electricity' : 'Electricity|capacity',
                   'Heat_output_'   : 'Heat_output|',
                   'Losses_' : 'Losses|' ,
                   'Final_energy_demand_by_fuel|': 'Final_Energy|Total|',
                   'Final_energy' :'Final_Energy',
                   'Secondary_energy' : 'Secondary_Energy',
                   'Emissions|KyotoGHG' : 'Emissions|KYOTOGHG_AR4',
                   'Emissions|KYOTOGHG' : 'Emissions|KYOTOGHG',
                   'Emission|KYOTO_GHG_AR4' : 'Emissions|KYOTOGHG_AR4',
                   'Emission|KYOTOGHG'      : 'Emissions|KYOTOGHG_AR4',
                   'Emissions|Kyoto Gases|AR5-GWP100' : 'Emissions|KYOTOGHG_AR5',
                   'Emission|KYOTO_GHG_AR4' : 'Emissions|KYOTOGHG_AR4',
                   'Emissions_KYOTOGHGAR4' : 'Emissions|KYOTOGHG_AR4',
                   'Emissions|Kyoto Gases|AR4-GWP10' : 'Emissions|KYOTOGHG_AR4',
                   'Emissions|KYOTOGHG_AR40' : 'Emissions|KYOTOGHG_AR4',
                   'Emissions_KYOTOGHG_AR4' : 'Emissions|KYOTOGHG_AR4',
                   'Emissions|Kyoto_Gases' : 'Emissions|KYOTOGHG',
                   'Emissions|Fuel|CO2' : 'Emissions|CO2|Fuel',
                   'Emissions|Fuel_CO2' : 'Emissions|CO2|Fuel',
                   'Exports_' : 'Exports|',
                   'population_total' : 'Population',
                   'population' : 'Population',
                   'gdp_ppp' : 'GDP|PPP',
                   'gdp_pp'  : 'GDP|PPP',
                   'GDP_PPP'  : 'GDP|PPP',
                   'GDP_MER'  : 'GDP|MER',
                   'Emissions_CH4' : 'Emissions|CH4',
                   'Emissions_CO2' : 'Emissions|CO2',
                   'Emissions_CO2_energy' : 'Emissions|CO2|Energy',
                   'Emissions|CO2_energy' : 'Emissions|CO2|Energy',
                   'Emissions|HFCS' : 'Emissions|HFCs',
                   'Emissions|PFCS' : 'Emissions|PFCs',
                   'Electricity_output'  : 'Electricity_generation',
                   'Electricity_output ' : 'Electricity_generation',
                   'Elect_Capacity'     : 'Electricity_capacity',  
                   'Electrical_capacity' : 'Electricity_capacity',
                   'Electrical|capacity' : 'Electricity_capacity',
                   'Electricity|capacity ': 'Electricity_capacity',
                   'Electricity»generation' : 'Electricity_generation',
                   'Electricity»genertation ' : 'Electricity_generation', 
                   'Elect_Generation' : 'Electricity_generation',
                   'Electricity_and_heat_generation' : 'Electricity&Heat_generation',
                   'Price_' : 'Price|',
                   'Primary_Energy_': 'Primary_Energy|',
                   'Primary_energy' : 'Primary_Energy',
                   'Production_' : 'Production|',
                   'Stock_changes_'  :'Stock_changes|'  ,
                   'Transfers_' : 'Transfers|',
                   'Total_PE_supply_' : 'Total_PE_supply|',
                   'Total_consumption_' : 'Total_consumption|',
                   'Emissions_per_capita' : 'Emissions_per_capita'}

    entityList = ['Electricity|generation|',
                  'Electricity|capacity|',
                  'Electricity&Heat|generation|',
                  'Emissions|KYOTOGHG_AR4|',
                  'Emissions|KYOTOGHG_AR5|',
                  'Emissions|KYOTOGHG|',
                  'Emissions|BC|',
                  'Emissions|CO2|',
                  'Emissions|CH4|',
                  'Emissions|NH3|',
                  'Emissions|N2O|',
                  'Emissions|NF3|',
                  'Emissions|NOx|',
                  'Emissions|HFCs|',
                  'Emissions|OC|',
                  'Emissions|SF6|',
                  'Emissions|PFCs|',
                  'Emissions|VOC|',
                  'Exports|',
                  'Final_Energy|',
                  'Investment|',
                  'GDP|PPP|constant|',
                  'GDP|PPP|current|',
                  'GDP|MER|',
                  'Heat_output|',
                  'Secondary_Energy|',
                  'Stock_changes|' ,
                  'Transfers|',
                  'Total_consumption|',
                  'Population|',
                  'Primary_Energy|',
                  'Price|',
                  'Production|']


    scenarioReplacementDict = {'historic'   :'Historic',
                               'Historical' : 'Historic',
                               'historical' : 'Historic',
                               'History'    : 'Historic',
                               'HISTCR'     : 'Historic|country_reported',
                               'HISTTP'     : 'Historic|third_party',
                               'computed historic' : 'Historic|computed'}
    
#inventory.category = None
#for entity in entityList:
#    mask = inventory.entity.str.startswith(entity)
#    
#    inventory.loc[mask, 'category'] = inventory.loc[mask, 'category'] + inventory.loc[mask, 'entity'].apply(lambda x: x.replace(entity,'')) 
#    inventory.loc[mask, 'entity'] = entity[:-1]
    outList = list()
    for table in tqdm.tqdm(tableSet):
#        table = tableSet[tableID]
        for string, newString in replacementDict.items():
            table.meta['entity'] = table.meta['entity'].replace(string,newString)
        
        for entity in entityList:
            if table.meta['entity'].startswith(entity):
                if 'category' in table.meta:
                    table.meta['category'] = '|'.join([table.meta['entity'].replace(entity,''), table.meta['category']]).lstrip('|')
                else:
                    table.meta['category'] = table.meta['entity'].replace(entity,'')
                table.meta['entity'] = entity.rstrip('|')
       
        for scenario in scenarioReplacementDict.keys():
            table.meta['scenario'] = table.meta['scenario'].replace(scenario, scenarioReplacementDict[scenario])
        
        if 'model' in table.meta.keys() and (table.meta['model'] in table.meta['scenario']):
            table.meta['scenario'] = table.meta['scenario'].replace(table.meta['model'],'').rstrip('|')
            table.generateTableID()
           
        sourceSplit = table.meta['source'].split('_') 
        if len(sourceSplit) ==2 :
            table.meta['source_name'], table.meta['source_year'] = sourceSplit
        else:
            if table.meta['source'].startswith('CAT'):
                table.meta['source_name'] = 'CAT'
                table.meta['source_year'] = table.meta['source'].replace('CAT_','')
            elif table.meta['source'].startswith('CA_NDCA'):
                table.meta['source_name'] = 'CA_NDCA'
                table.meta['source_year'] = table.meta['source'].replace('CA_NDCA_','')
            elif table.meta['source'].startswith('AIM_SSPx_DATA'):
                table.meta['source_name'] = 'AIM_SSPx_DATA'
                table.meta['source_year'] = table.meta['source'].replace('AIM_SSPx_DATA_','')
            elif table.meta['source'].startswith('CA_NDCA'):
                table.meta['source_name'] = 'CA_NDCA'
                table.meta['source_year'] = table.meta['source'].replace('CA_NDCA_','')
            elif table.meta['source'].startswith('IEA_CO2_FUEL'):
                table.meta['source_name'] = 'IEA_CO2_FUEL'
                table.meta['source_year'] = table.meta['source'].replace('IEA_CO2_FUEL_','')
            elif table.meta['source'].startswith('IEA_WEB'):
                table.meta['source_name'] = 'IEA_WEB'
                table.meta['source_year'] = table.meta['source'].replace('IEA_WEB_','')
            elif table.meta['source'].startswith('SDG_DB'):
                table.meta['source_name'] = 'SDG_DB'
                table.meta['source_year'] = table.meta['source'].replace('SDG_DB_','')
                  
            elif table.meta['source'].startswith('SSP_DB'):
                table.meta['source_name'] = 'SSP_DB'
                table.meta['source_year'] = table.meta['source'].replace('SSP_DB_','')
                  
            elif table.meta['source'].startswith('UNFCCC_CRF'):
                table.meta['source_name'] = 'UNFCCC_CRF'
                table.meta['source_year'] = table.meta['source'].replace('UNFCCC_CRF_','')
                  
            elif table.meta['source'].startswith('UN_WPP'):
                table.meta['source_name'] = 'UN_WPP'
                table.meta['source_year'] = table.meta['source'].replace('UN_WPP','')

                
        outList.append(table)
        
    return outList

#%%
def zipExport(IDList, fileName):
    from zipfile import ZipFile
    folder = os.path.join(config.PATH_TO_DATASHELF, 'exports/')
    os.makedirs(folder, exist_ok=True)
    
#    root = config.PATH_TO_DATASHELF
    
    sources = dt.find().loc[IDList].source.unique()
    sourceMeta = dt.core.DB.sources.loc[sources]
    sourceMeta.to_csv(os.path.join(folder, 'sources.csv'))
    zipObj = ZipFile(os.path.join(folder, fileName), 'w')
    zipObj.write(os.path.join(folder, 'sources.csv'),'./sources.csv')
    for ID in tqdm.tqdm(IDList):
        # Add multiple files to the zip
        tablePath = dt.core.DB._getPathOfTable(ID)
        csvFileName = os.path.basename(tablePath) 
        
        zipObj.write(tablePath,os.path.join('./data/', csvFileName))
#        zipObj.write(tablePath, os.path.relpath(os.path.join(root, file), os.path.join(tablePath, '..')))
 
    # close the Zip File
    zipObj.close()
    
def update_DB_from_zip_toV3(filePath, cleanTables=True):
    
#%%        
    from zipfile import ZipFile
    import shutil
    zf = ZipFile(filePath, 'r')
    
    tempFolder = os.path.join(config.PATH_TO_DATASHELF, 'temp/')
    shutil.rmtree(tempFolder, ignore_errors=True)
    os.makedirs(tempFolder)
    zf.extractall(tempFolder)
    zf.close()
    
    update_source_from_file(os.path.join(tempFolder, 'sources.csv'))
    tablesToUpdate = update_DB_from_folderV3(os.path.join(tempFolder, 'data'), message= 'DB update from ' + os.path.basename(filePath))
    return tablesToUpdate

def update_DB_from_zip(filePath):
    
#%%        
    from zipfile import ZipFile
    import shutil
    zf = ZipFile(filePath, 'r')
    
    tempFolder = os.path.join(config.PATH_TO_DATASHELF, 'temp/')
    shutil.rmtree(tempFolder, ignore_errors=True)
    os.makedirs(tempFolder)
    zf.extractall(tempFolder)
    zf.close()
    
    update_source_from_file(os.path.join(tempFolder, 'sources.csv'))
    update_DB_from_folder(os.path.join(tempFolder, 'data'), message= 'DB update from ' + os.path.basename(filePath))
#%%

def forAll(funcHandle, subset='scenario', source='IAMC15_2019_R2'):
    
    outTables = list()
    success = dict()
    if subset == "scenario":
        scenarios = dt.find(source=source).scenario.unique()
        
        for scenario in scenarios:
            try:
                outTables.append(funcHandle(scenario))
                print('{} run successfully'.format(scenario))
                success[scenario] = True
            except:
                #print('{} failed to run'.format(scenario))
                success[scenario] = False
                pass
    return outTables, success




import csv

def dict_to_csv(dictionary, filePath):

    with open(filePath, 'w', newline='') as file:
        writer = csv.writer(file)
        for key, val in dictionary.items():
            writer.writerow([key, val])
#%%
def csv_to_dict(filePath):

    with open(filePath, 'r', newline='') as file:
        reader = csv.reader(file)
        mydict = dict()
        for row in reader:
            print(row)
#            v = rows[1]
            mydict[row[0]] =  row[1]
    return mydict

def aggregate_table_to_region(table, mapping):
    #TODO remove soon
    raise(Warning('Depricated soon. Please use new implementaion in datatoolbox.tools.for_datatables'))
    
    missingCountryDict = dict()
    
    for region in mapping.listAll():

        
        missingCountries = set(mapping.membersOf(region)) - set(table.index)
#                print('missing countries: {}'.format(missingCountries))
        missingCountryDict[region] = list(missingCountries)
        availableCountries = set(mapping.membersOf(region)).intersection(table.index)
        if len(availableCountries) >0:
            table.loc[region,:] = table.loc[availableCountries,:].sum(axis=0, skipna=True)

    return table, missingCountryDict

def aggregate_tableset_to_region(tableSet, mapping):
    missingCountryDf = pd.DataFrame(columns=mapping.listAll())

    for tableKey in tableSet.keys():

        for region in mapping.listAll():
#                print(region)
            
            missingCountries = set(mapping.membersOf(region)) - set(tableSet[tableKey].index)
#                print('missing countries: {}'.format(missingCountries))
            missingCountryDf.loc[tableSet[tableKey].ID, region] = list(missingCountries)
            availableCountries = set(mapping.membersOf(region)).intersection(tableSet[tableKey].index)
            if len(availableCountries) >0:
                tableSet[tableKey].loc[region,:] = tableSet[tableKey].loc[availableCountries,:].sum(axis=0, skipna=True)

    return tableSet, missingCountryDf


import matplotlib.pylab as plt

def plotTables(tableList, countryList):
    fig = plt.figure(1)
    plt.clf()
    NUM_COLORS = len(countryList)
    
    cm = plt.get_cmap('gist_rainbow')
    coList = [cm(1.*i/NUM_COLORS) for i in range(NUM_COLORS)]
#    fig = plt.figure()
#    ax = fig.add_subplot(111)
#    ax.set_color_cycle([cm(1.*i/NUM_COLORS) for i in range(NUM_COLORS)])

#    ax.set_color_cycle([cm(1.*i/NUM_COLORS) for i in range(NUM_COLORS)])
    for table in tableList:
        for i, coISO in enumerate(countryList):
            plt.plot(table.columns, table.loc[coISO,:].T, color =coList[i] )
    plt.legend(countryList)
    plt.title(table.ID)


def pattern_match(
    data: pd.Series,
    patterns: Union[str, Iterable[str]],
    regex : bool = False
):
    """Find matches in `data` for a list of shell-style `patterns`

    Arguments
    ---------
    data : pd.Series
        Series of data to match against
    patterns : Union[str, Iterable[str]]
        One or multiple patterns, which are OR'd together
    regex : bool, optional
        Accept plain regex syntax instead of shell-style, default: False
    
    Returns
    -------
    matches : pd.Series
        Mask for selecting matched rows
    """

    if not isinstance(patterns, Iterable) or isinstance(patterns, str):
        patterns = [patterns]
    elif not patterns:
        raise ValueError("pattern list may not be empty")

    matches = False
    for pat in patterns:
        if isinstance(pat, str):
            if not regex:
                pat = shell_pattern_to_regex(pat) + '$'
            matches |= data.str.match(pat, na=False)
        else:
            matches |= data == pat

    return matches


def shell_pattern_to_regex(s):
    """Escape characters with specific regexp use"""
    return (
        str(s)
        .replace('|', r'\|')
        .replace('.', r'\.')  # `.` has to be replaced before `*`
        .replace('**', '__starstar__') # temporarily __starstar__
        .replace('*', r'[^|]*')
        .replace('__starstar__', r'.*')
        .replace('+', r'\+')
        .replace('(', r'\(')
        .replace(')', r'\)')
        .replace('$', r'\$')
    )

def fix_source_inconsistency(sourceID):
    gitManager = dt.core.DB.gitManager.__dict__['repositories'][sourceID]
    gitManager.git.execute(["git", "add", "tables/*"])
    dt.core.DB.gitManager.sources.loc['IEA_WEB_2020','git_commit_hash'] = gitManager.commit().hexsha
    dt.core.DB.gitManager.commit('inconsistent fix')


def _create_sandbox_tables(sourceID, random_seed):
    #%%
#    import datatoolbox as dt
    import numpy as np
    np.random.seed(1)
    

    tables = list()
    source_meta = {'SOURCE_ID': sourceID,
                      'collected_by' : 'Hard worker 1',
                      'date': core.getDateString(),
                      'source_url' : 'www.www.www',
                      'licence': 'open access' }
    
    meta = {'entity' : 'Emissions|CO2',
            'category': 'Total',
            'model': None,
            'scenario' : 'Historic',
            'source' : sourceID,
            'unit' : 'Mt CO2'}
    
    table = data_structures.Datatable(np.random.randint(0,20,[3,21]), 
                         columns = list(range(2000,2021)), 
                         index = ['World', 'Asia', 'ZAF'],
                         meta=meta).astype(float)
    tables.append(table)
    
    meta = {'entity' : 'Emissions|CO2',
            'category': 'Total',
            'scenario': 'Medium',
            'model' : 'Projection',
            'source' : sourceID,
            'unit' : 'Mt CO2'}
    
    table = data_structures.Datatable(np.random.randint(20,30,[3,31]), 
                         columns = list(range(2020,2051)), 
                         index = ['World', 'Asia', 'ZAF'],
                         meta=meta).astype(float)
    tables.append(table)
    
    
    
    meta = {'entity' : 'Emissions|CO2',
            'category': 'Total_excl_LULUCF',
            'scenario': None,
            'model' : 'Historic',
            'source_name' : sourceID,
            'unit' : 'Mt CO2'}
    
    table = data_structures.Datatable(np.random.randint(0,15,[3,21]), 
                         columns = list(range(2000,2021)), 
                         index = ['World', 'Asia', 'ZAF'],
                         meta=meta).astype(float)
    tables.append(table)
    return tables,source_meta

#%%    
if __name__ == '__main__':
    #%%
    def calculateTotalBiomass(scenario):
        source = 'IAMC15_2019_R2'
        tableID = core._createDatabaseID({"entity":"Primary_Energy|Biomass|Traditional",
                                         "category":"",
                                         "scenario":scenario,
                                         "source":'IAMC15_2019_R2'})
        tratBio = dt.getTable(tableID)
        
        tableID = core._createDatabaseID({"entity":"Primary_Energy|Biomass|Modern|wo_CCS",
                                         "category":"",
                                         "scenario":scenario,
                                         "source":'IAMC15_2019_R2'})
        modernBio = dt.getTable(tableID)
        
        tableID = core._createDatabaseID({"entity":"Primary_Energy|Biomass|Modern|w_CCS",
                                         "category":"",
                                         "scenario":scenario,
                                         "source":'IAMC15_2019_R2'})
        modernBioCCS = dt.getTable(tableID)
        
        table = tratBio + modernBio + modernBioCCS
        
        table.meta.update({"entity": "Primary_Energy|Biomass|Total",
                           "scenario" : scenario,
                           "source" : source,
                           "calculated" : "calculatedTotalBiomass.py",
                           "author" : 'AG'})
        return table

    outputTables, success = forAll(calculateTotalBiomass, "scenario")
    
    