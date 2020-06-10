#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 11:25:15 2019

@author: Andreas Geiges
"""
import pandas as pd
import numpy  as np
import pandas as pd
import datatoolbox as dt
from datatoolbox import mapping as mapp
from datatoolbox import core
from datatoolbox import config
#from datatoolbox.data_structures import Datatable, read_csv
from datatoolbox.greenhouse_gas_database import GreenhouseGasTable 
import matplotlib.pylab as plt
import os
import tqdm

#from .tools import kaya_idendentiy_decomposition

DataFrameStyle = """
<style>
    tr:nth-child(even) {color: blue; }
    tr:nth-child(odd)  {background: #ededed;}
    thead th      { background: #cecccc; }
    tr:hover { background: silver;   cursor: pointer;}
    td, th {padding: 5px;}
    {border: 1px solid silver;}
    {border-collapse: collapse; }
    {font-size: 11pt;}
    {font-family: Arial;}
    {text-align: left;}
</style>
"""
#nice green #d2f4e5

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


 
#%%

def generate_html(dataframe, style =None) :
    
    if style is None:
        style = DataFrameStyle
    pd.set_option('colheader_justify', 'center')   # FOR TABLE <th>
    
    html = _HTML_with_style(dataframe, '<style>table {}</style>'.format(style))
    
    return html.data.replace('NaN','')

def export_to_pdf( dataframe, fileName):
    html = export_to_html(dataframe)
    import pdfkit
    pdfkit.from_string(html, fileName)

def export_to_html( dataframe, fileName = None, heading =''):
    pd.set_option('colheader_justify', 'center')   # FOR TABLE <th>
    
    html = _HTML_with_style(dataframe, '<style>table {}</style>'.format(DataFrameStyle))
    
    html_string = '''
            <html>
              <head><title>HTML Pandas Dataframe with CSS</title></head>
              <link rel="stylesheet" type="text/css" href="df_style.css"/>
              <body>
              <h1>{heading}</h1>
                {table}
              </body>
            </html>
            '''
    df_html = html_string.format(table=html.data, heading=heading)
    
    if fileName:
        with open(fileName, 'w') as f:
            f.write(df_html)
    
    return df_html

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

def convertToCO2eq(dataTable, context = 'GWP100_AR5'):
    # load the greenhouse gase table if not done yet
    
    
    
    # remove mass unit
    unit = core.getUnit(dataTable.meta['unit'])
    unitDict = unit.u._units
    unitEntity = (unit / core.ur('kg')).to_base_units().u
    
    
    if len(unitEntity._units.keys()) > 1:
        raise Exception('More than one gas entity found: ' + str(unitEntity) + ' - cannot convert')
    
    # get the GWP equivalent from the GHG database
    gasPropertyDict = GHG_data.searchGHG(str(unitEntity))
    if gasPropertyDict is None:
        raise Exception('No gas been found for conversion')
        
    GWP_equivalent = gasPropertyDict[context]
    
    # prepate a dict of units and remove the old gas entity
    unitDict =  unitDict.remove(str(unitEntity))
    # add CO2eq as unit
    unitDict = unitDict.add('CO2eq',1)
    
    # create a new composite sting of the new mass of CO2eq    
    newUnit = ' '.join([xx for xx in unitDict.keys()])
    
    # copy table and convert values and create new meta data
    outTable = dataTable.copy()
    outTable.values[:] = outTable.values[:] * GWP_equivalent
    outTable.meta['unit'] = newUnit
    
    return outTable

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
    dataTable = dataTable.filter(spaceIDs = mapp.getValidSpatialIDs())    
    
    dataTable = dataTable.loc[:,~dataTable.isnull().all(axis=0)]
    dataTable = dataTable.loc[~dataTable.isnull().all(axis=1),:]
    #dataTable = dataTable.loc[~dataTable.isnull().all(axis=0),:]
    
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

def _HTML_with_style(df, style=None, random_id=None):
    from IPython.display import HTML
    import numpy as np
    import re

    df_html = df.to_html(escape =False)

    if random_id is None:
        random_id = 'id%d' % np.random.choice(np.arange(1000000))

    if style is None:
        style = """
        <style>
            table#{random_id} {{color: blue}}
        </style>
        """.format(random_id=random_id)
    else:
        new_style = []
        s = re.sub(r'</?style>', '', style).strip()
        for line in s.split('\n'):
                line = line.strip()
                if not re.match(r'^table', line):
                    line = re.sub(r'^', 'table ', line)
                new_style.append(line)
        new_style = ['<style>'] + new_style + ['</style>']

        style = re.sub(r'table(#\S+)?', 'table#%s' % random_id, '\n'.join(new_style))

    df_html = re.sub(r'<table', r'<table id=%s ' % random_id, df_html)

#    html_string = '''
#        <html>
#          <head><title>HTML Pandas Dataframe with CSS</title></head>
#          <link rel="stylesheet" type="text/css" href="df_style.css"/>
#          <body>
#          <h1>{heading}</h1>
#            {table}
#          </body>
#        </html>.
#        '''
    #df_html = html_string.format(table=df_html, heading=heading)
    return HTML(style + df_html)

def savefig(*args, **kwargs):
    """
    Wrapper around plt.savefig to append the filename of the creating scrip
    for re-producibiltiy
    """
    import inspect, os
    import matplotlib.pylab as plt
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    try:
        pyFileName = os.path.basename(module.__file__)
    except:
        pyFileName = 'unkown'
    
    plt.gcf().text(0.01, 0.02, 'file:' + pyFileName)
    plt.savefig(*args, **kwargs)
    
    
def identifyCountry(string):
    
    #numeric ISO code
    try:
        numISO = float(string)
        mask = numISO == dt.mapp.countries.codes['numISO']
        if mask.any():
            return dt.mapp.countries.index[mask][0]
    except:
        pass
    if len(string) == 2:
        mask = string == dt.mapp.countries.codes['alpha2']
        if mask.any():
            return dt.mapp.countries.codes.index[mask][0]
    
    if len(string) == 3:
         
        if string.upper() in dt.mapp.countries.codes.index:
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

def getCountryExtract(countryList, sourceList='all'):
    
    if sourceList == 'all':
        sourceList = list(dt.core.DB.sources.index)
    
    if not isinstance(countryList,list):
        countryList = [countryList]
    if not isinstance(sourceList,list):
        sourceList = [sourceList]
            
    #%%
    resFull = list()
    sourceList.sort()
    for source in sourceList:
        print(source)
        newList = list(dt.find(source=source).index)    
        newList.sort()
        resFull = resFull + newList
    #resFull.sort()
    #return(resFull)
    _res2Excel(resFull, countryList)

def _res2Excel(resFull, countryList):
    tableSet = dt.TableSet()
    
    yearRange = (pd.np.inf, -pd.np.inf)
    
    ID_list = list()
    for ID in resFull:
        table = dt.getTable(ID)
        if 'Gg' in table.meta['unit']:
            table = table.convert(table.meta['unit'].replace('Gg','Mt'))
        tableSet.add(table)
        ID_list.append(table.ID)
        
        minYears = min(yearRange[0], table.columns.min())
        maxYears = max(yearRange[1], table.columns.max())
        yearRange = (minYears, maxYears)
        
    for country in countryList:
        
        coISO = identifyCountry(country)
        
        if coISO is None:
            coISO = country
        
        outDf = pd.DataFrame(columns = ['Source', 'Entity', 'Category', 'Scenario', 'Unit'] + list(range(yearRange[0], yearRange[1]+1)))
        
        i =0
        for ID in ID_list:
            if coISO in tableSet[ID].index:
                years =  tableSet[ID].columns[~tableSet[ID].loc[coISO].isna()]
                outDf.loc[i,years] = tableSet[ID].loc[coISO,years]
                outDf.loc[i,['Source', 'Entity','Category', 'Scenario', 'Unit']] = [tableSet[ID].meta[x] for x in ['source', 'entity','category', 'scenario', 'unit']]
                i = i+1
        
        outDf = outDf.loc[:,~outDf.isnull().all(axis=0)]
        outDf.to_excel(config.MODULE_PATH + 'extracts/' + coISO + '.xlsx')
        
        
    #%%
def compare_excel_files(file1, file2, eps=1e-6):
    #%%
    
    def report_diff(x):
        try:
#            print(x)
            x = x.astype(float)
#            print(x)
            return x[1] if (abs(x[0] - x[1]) < eps) or  pd.np.any(pd.isnull(x))  else '{0:.2f} -> {1:.2f}'.format(*x)
        except:
            return x[1] if (x[0] == x[1]) or  pd.np.any(pd.isnull(x))  else '{} -> {}'.format(*x)
            #print(x)
            
    xlFile1 = pd.ExcelFile(file1)
    sheetNameList1 = set(xlFile1.sheet_names)
    
    xlFile2 = pd.ExcelFile(file2)
    sheetNameList2 = set(xlFile2.sheet_names)
    
    writer = pd.ExcelWriter("diff_temp.xlsx")
    
    for sheetName in sheetNameList2.intersection(sheetNameList1):
        data1  = pd.read_excel(xlFile1, sheet_name=sheetName)
        data2  = pd.read_excel(xlFile2, sheet_name=sheetName)
        data1 = data1.replace(pd.np.nan, '').astype(str)
        data2 = data2.replace(pd.np.nan, '').astype(str)
        data1 = data1.apply(lambda x: x.str.strip())
        data2 = data2.apply(lambda x: x.str.strip())
        diff_panel = pd.Panel(dict(df1=data1,df2=data2))
        diff_output = diff_panel.apply(report_diff, axis=0)

        diff_output.to_excel(writer,sheet_name=sheetName)
    
        workbook  = writer.book
        worksheet = writer.sheets[sheetName]
        highlight_fmt = workbook.add_format({'font_color': '#FF0000', 'bg_color':'#B1B3B3'})
    
        grey_fmt = workbook.add_format({'font_color': '#8d8f91'})
        
        worksheet.conditional_format('A1:ZZ1000', {'type': 'text',
                                                'criteria': 'containing',
                                                'value':'->',
                                                'format': highlight_fmt})
        worksheet.conditional_format('A1:ZZ1000', {'type': 'text',
                                                'criteria': 'not containing',
                                                'value':'->',
                                                'format': grey_fmt})
        
    writer.close()
    os.system('libreoffice ' + "diff_temp.xlsx")
    os.system('rm diff_temp.xlsx')


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
            if source in tablesToUpdate.keys():
    
    
                tablesToUpdate[source].append(table)
            else:
                tablesToUpdate[source] = [table]
        
        if message is None:
    
            message = 'External data added from external source by ' + config.CRUNCHER + '{}/{}'.format(nCommit,nCommits)
    
        for source in tablesToUpdate.keys():
    
            tablesToUpdate[source] = metaV2_to_meta_V3(tablesToUpdate[source])
    #    return tablesToUpdate
    
        for source in tablesToUpdate.keys():
            sourceMetaDict = dict()
            sourceMetaDict['SOURCE_ID']= source
            core.DB.commitTables(tablesToUpdate[source], 
                            message = message,
                            sourceMetaDict = sourceMetaDict, 
                            cleanTables=cleanTables)

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
                   'Elect_Capacity'     : 'Electricity|capacity',  
                   'Electrical_capacity' : 'Electricity|capacity',
                   'Electrical|capacity' : 'Electricity|capacity',
                   'Electricity|capacity ': 'Electricity|capacity',
                   'Electricity_generation' : 'Electricity|generation',
                   'Electricity_genertation ' : 'Electricity|generation', 
                   'Elect_Generation' : 'Electricity|generation',
                   'Electricity_and_heat_generation' : 'Electricity&Heat|generation',
                   'Price_' : 'Price|',
                   'Primary_Energy_': 'Primary_Energy|',
                   'Primary_energy' : 'Primary_Energy',
                   'Production_' : 'Production|',
                   'Stock_changes_'  :'Stock_changes|'  ,
                   'Transfers_' : 'Transfers|',
                   'Total_PE_supply_' : 'Total_PE_supply|',
                   'Total_consumption_' : 'Total_consumption|',
                   'Emissions_er_capita' : 'Emissions_per_capita'}

    entityList = ['Electricity|generation|',
                  'Electricity|capacity|',
                  'Electricity&Heat|generation|',
                  'Emissions|KYOTOGHG_AR4|',
                  'Emissions|KYOTOGHG_AR5|',
                  'Emissions|KYOTOGHG|',
                  'Emissions|BC|',
                  'Emissions|CO2',
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
                
        if hasattr(table.meta,'model') and (table.meta['model'] in table.meta['scenario']):
            table.meta['scenario'] = table.meta['scenario'].replace(table.meta['model'],'').rstrip('|')
            table.generateTableID()
        outList.append(table)
        
    return outList

#%%
def zipExport(IDList, fileName):
    from zipfile import ZipFile
    folder = os.path.join(config.PATH_TO_DATASHELF, 'exports/')
    os.makedirs(folder, exist_ok=True)
    zipObj = ZipFile(os.path.join(folder, fileName), 'w')
#    root = config.PATH_TO_DATASHELF
    
    sources = list(dt.find().loc[IDList].source.unique())
    sourceMeta = dt.core.DB.sources.loc[sources]
    sourceMeta.to_csv(os.path.join(folder, 'sources.csv'))
    
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


def yearsColumnsOnly(dataframe):
    import re
    REG_YEAR   = re.compile('^[0-9]{4}$')
    
    newColumns = []
    for col in dataframe.columns:
        if REG_YEAR.search(str(col)) is not None:
            newColumns.append(col)
    return newColumns

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