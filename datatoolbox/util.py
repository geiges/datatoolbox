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
from datatoolbox.data_structures import Datatable, read_csv
from datatoolbox.greenhouse_gas_database import GreenhouseGasTable 
import matplotlib.pylab as plt
import os

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

class TableSet(dict):
    def __init__(self):
        super(dict, self).__init__()
        self.inventory = pd.DataFrame(columns = config.ID_FIELDS)
    
    def __iter__(self):
        return iter(self.values())
    
    def add(self, datatables=None, tableID=None):
        if isinstance(datatables, (list, TableSet)):
            for datatable in datatables:
                self._add(datatable, tableID)
        else:
            datatable = datatables
            self._add(datatable, tableID)
            
    def _add(self, datatable=None, tableID=None):
        if datatable is None:
            # adding only the ID, the full table is only loaded when necessary
            self[tableID] = None
            self.inventory.loc[tableID] = [None for x in config.ID_FIELDS]
        else:
            # loading the full table
            if datatable.ID is None:
                datatable.generateTableID()
            self[datatable.ID] = datatable
            self.inventory.loc[datatable.ID] = [datatable.meta[x] for x in config.ID_FIELDS]
    
    def remove(self, tableID):
        del self[tableID]
        self.inventory.drop(tableID, inplace=True)
        
    
    def filter(self, ):
        pass
    
    def __getitem__(self, key):
        item = super(TableSet, self).__getitem__(key)
        
        #load datatable if necessary
        if item is None:
            item = core.DB.getTable(key)
            self[key] = item
        
        return item
    
    def to_excel(self, fileName):
        writer = pd.ExcelWriter(fileName, 
            engine='openpyxl', 
            mode='a',
            datetime_format='mmm d yyyy hh:mm:ss',
            date_format='mmmm dd yyyy') 
        
        for i,eKey in enumerate(self.keys()):
            table = self[eKey].dropna(how='all', axis=1).dropna(how='all', axis=0)
            sheetName = str(i) + table.meta['ID'][:25]
            print(sheetName)
            table.to_excel(writer=writer, sheetName = sheetName)
            
        writer.close()
        
    def create_country_dataframes(self, countryList=None, timeIdxList= None):
        
        # using first table to get country list
        if countryList is None:
            countryList = self[list(self.keys())[0]].index
        
        coTables = dict()
        
        for country in countryList:
            coTables[country] = pd.DataFrame([], columns= ['entity', 'unit', 'source'] +list(range(1500,2100)))
            
            for eKey in self.keys():
                table = self[eKey]
                if country in table.index:
                    coTables[country].loc[eKey,:] = table.loc[country]
                else:
                    coTables[country].loc[eKey,:] = np.nan
                coTables[country].loc[eKey,'source'] = table.meta['source']
                coTables[country].loc[eKey,'unit'] = table.meta['unit']
                                    
            coTables[country] = coTables[country].dropna(axis=1, how='all')
            
            if timeIdxList is not None:
                
                containedList = [x for x in timeIdxList if x in coTables[country].columns]
                coTables[country] = coTables[country][['source', 'unit'] + containedList]

            
        return coTables

    def entities(self):
        return list(self.inventory.entity.unique())

    def scenarios(self):
        return list(self.inventory.scenario.unique())
    
    def sources(self):
        return list(self.inventory.source.unique())

    def to_LongTable(self):
        #%%
        import pyam
        import copy
        #self = dt.getTables(res.index)
        tableList= list()
        minMax = (-np.inf, np.inf)
        for key in self.keys():
            table= copy.copy(self[key])
            #print(table.columns)
            
            oldColumns = list(table.columns)
            minMax = (max(minMax[0], min(oldColumns)), min(minMax[1],max(oldColumns)))
#            for field in config.ID_FIELDS:
#                table.loc[:,field] = table.meta[field]
            
            table.loc[:,'region'] = table.index
            table.loc[:,'unit']   = table.meta['unit']
            table.loc[:,'variable']   = table.meta['entity']
            
            scenModel = table.meta['scenario'].split('|')
            if len(scenModel) == 2:
                table.loc[:,'model'] = scenModel[1]
                table.loc[:,'scenario']   =scenModel[0]
            else:
                table.loc[:,'model'] = table.meta['scenario']
                table.loc[:,'scenario']   = ''
            tableNew = table.loc[:, ['variable','region', 'scenario',  'model', 'unit'] +  oldColumns]
            tableNew.index= range(len(tableNew.index))
            tableList.append(tableNew)
        #df = pd.DataFrame(columns = ['variable','region', 'model', 'unit'] + list(range(minMax[0], minMax[1])))
        
#        iaDf = pyam.IamDataFrame(tableList[0])
#        for table in tableList[1:]:
#            iaDf.append(table)
#        return iaDf
        fullDf = pd.DataFrame(tableList[0])
        for table in tableList[1:]:
            #print(table)
            fullDf = fullDf.append(pd.DataFrame(table))
        fullDf.index = range(len(fullDf))
        return fullDf
    
    def to_IamDataFrame(self):
        #%%
        import pyam
        import copy
        #self = dt.getTables(res.index)
        tableList= list()
        minMax = (-np.inf, np.inf)
        for key in self.keys():
            table= copy.copy(self[key])
            #print(table.columns)
            
            oldColumns = list(table.columns)
            minMax = (max(minMax[0], min(oldColumns)), min(minMax[1],max(oldColumns)))
#            for field in config.ID_FIELDS:
#                table.loc[:,field] = table.meta[field]
            
            table.loc[:,'region'] = table.index
            table.loc[:,'unit']   = table.meta['unit']
            table.loc[:,'variable']   = table.meta['entity']
            
            scenModel = table.meta['scenario'].split('|')
            if len(scenModel) == 2:
                table.loc[:,'model'] = scenModel[1]
                table.loc[:,'scenario']   =scenModel[0]
            else:
                table.loc[:,'model'] = table.meta['scenario']
                table.loc[:,'scenario']   = ''
            tableNew = table.loc[:, ['variable','region', 'scenario',  'model', 'unit'] +  oldColumns]
            tableNew.index= range(len(tableNew.index))
            tableList.append(tableNew)
        #df = pd.DataFrame(columns = ['variable','region', 'model', 'unit'] + list(range(minMax[0], minMax[1])))
        
#        iaDf = pyam.IamDataFrame(tableList[0])
#        for table in tableList[1:]:
#            iaDf.append(table)
#        return iaDf
        iaDf = pyam.IamDataFrame(pd.DataFrame(tableList[0]))
        for table in tableList[1:]:
            print(table)
            iaDf = iaDf.append(pd.DataFrame(table))
        return iaDf         


 
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
    tableSet = TableSet()
    
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
        table = read_csv(os.path.join(folderToRead, file))
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
    for ID in IDList:
        # Add multiple files to the zip
        tablePath = dt.core.DB._getPathOfTable(ID)
        csvFileName = os.path.basename(tablePath) 
        
        zipObj.write(tablePath,os.path.join('./data/', csvFileName))
#        zipObj.write(tablePath, os.path.relpath(os.path.join(root, file), os.path.join(tablePath, '..')))
 
    # close the Zip File
    zipObj.close()
    

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