#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV and git-based database to store year-country data in a multi-user
setup
"""


import os 
import git
import tqdm
import time
import copy
import types
from collections import defaultdict
from pathlib import Path

import pandas as pd
import numpy as np

from . import config
from .data_structures import Datatable, TableSet, read_csv
from .utilities import plot_query_as_graph, shorten_find_output
from . import mapping as mapp
from . import io_tools as io
from . import util
from . import core

class Database():
    """
    CSV based database that uses git for as distributed version control system.
    Each table is saved locally as a csv file and identified by a  unique ID. 
    The csv files are organized in various sources in individual folders. Each 
    sources comes with its own git repository and can be shared with others.
    """
    def __init__(self):
        """ 
        Initialized the database and creates an empty one in case the directory
        in the config is empty.
        """
        tt = time.time()
        self.path = config.PATH_TO_DATASHELF
        
        if not os.path.exists(os.path.join(self.path, 'inventory.csv')):
            self.create_empty_datashelf(config.MODULE_PATH,
                                        self.path)
        
        self.gitManager = GitRepository_Manager(config)
        self.INVENTORY_PATH = os.path.join(self.path, 'inventory.csv')
        self.inventory = pd.read_csv(self.INVENTORY_PATH, index_col=0, dtype={'source_year': str})
        self.sources   = self.gitManager.sources
        
        self.gitManager._validateRepository('main')


        if config.DEBUG:
            print('Database loaded in {:2.4f} seconds'.format(time.time()-tt))
    
        if config.TEST_ENV:
            # if no config is given, the empty sandbox is populated with some test
            # tables
            tablesToCommit, source_meta  = util._create_sandbox_tables('SOURCE_A_2020', 1)
            self.commitTables(tablesToCommit,
                         message = 'added dummy tablesof source A', 
                         sourceMetaDict = source_meta)
            tablesToCommit, source_meta  = util._create_sandbox_tables('SOURCE_B_2020', 2)
            self.commitTables(tablesToCommit,
                         message = 'added dummy tables of source B', 
                         sourceMetaDict = source_meta)
            print('Added test tables to Sandbox datashelf')
            
 
    
    def create_empty_datashelf(self,
                               modulePath, 
                               pathToDataself):
        """
        Method to create the required files for an empty csv-based data base. 
        (Equivalent to the fucntions in admin.py)
        """
        from pathlib import Path
        import os
        import shutil
        path = Path(pathToDataself)
        path.mkdir(parents=True,exist_ok=True)
        
        # add subfolders database
        Path(os.path.join(pathToDataself, 'database')).mkdir(exist_ok=True)
        Path(os.path.join(pathToDataself, 'mappings')).mkdir(exist_ok=True)
        Path(os.path.join(pathToDataself, 'rawdata')).mkdir(exist_ok=True)
        
        #create mappings
        os.makedirs(os.path.join(pathToDataself, 'mappings'),exist_ok=True)
        shutil.copyfile(os.path.join(modulePath, 'data/regions.csv'),
                        os.path.join(pathToDataself, 'mappings/regions.csv'))
        shutil.copyfile(os.path.join(modulePath, 'data/continent.csv'),
                        os.path.join(pathToDataself, 'mappings/continent.csv'))
        shutil.copyfile(os.path.join(modulePath, 'data/country_codes.csv'),
                        os.path.join(pathToDataself, 'mappings/country_codes.csv'))    
        
        # created sources.csv that contains the indivitual information for each source
        sourcesDf = pd.DataFrame(columns = config.SOURCE_META_FIELDS)
        filePath= os.path.join(pathToDataself, 'sources.csv')
        sourcesDf.to_csv(filePath)
        
        # creates inventory.csv that contains all data tables from all sources
        inventoryDf = pd.DataFrame(columns = config.INVENTORY_FIELDS)
        filePath= os.path.join(pathToDataself, 'inventory.csv')
        inventoryDf.to_csv(filePath)
        git.Repo.init(pathToDataself)
    
    
    def _validateRepository(self, repoID='main'):
        """
        Private
        Checks that a sub repository is valid. Valid means that the git repository
        is clean and not outanding commits are there.
        """
        return self.gitManager._validateRepository(repoID)
    
    def info(self):
        """
        Shows the most inmportant information about the status of the database
        """
        
        print('######## Database informations: #############')
        print('Your database is located at: ' + self.path)
        print('Number of tables: {}'.format(len(self.inventory)))  
        print('Number of data sources: {}'.format(len(self.gitManager.sources))) 
        print('Number of commits: {}'.format(self.gitManager['main'].git.rev_list("--all", "--count")))
#        root_directory = Path(config.PATH_TO_DATASHELF)
#        print('Size of datashelf: {:2.2f} MB'.format(sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file() )/1e6))
#        root_directory = Path(os.path.join(config.PATH_TO_DATASHELF, 'rawdata'))
#        print('Size of raw_data: {:2.2f} MB'.format(sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file() )/1e6))
        print('#############################################')
        
    def sourceInfo(self):
        """ 
        Returns a list of available sources and meta data
        """
        sources = copy.copy(self.gitManager.sources)
        return sources.sort_index()

    def returnInventory(self):
        """
        Returns a copy of the data base inventory. 
        """
        return copy.copy(self.inventory)


    def _reloadInventory(self):
        """
        Private
        Reloades the inventory from the csv file
        """
        self.inventory = pd.read_csv(self.INVENTORY_PATH, index_col=0, dtype={'source_year': str})
        
    def sourceExists(self, source):
        """ 
        Function to check if a source is propperly registered in the data base
        
        Input: SourceID
        """
        return source in self.gitManager.sources.index
    
    def add_to_inventory(self, datatable):
        """
        Method to add a table to the global inventory file. 
        Input: datatable
        """
        self.inventory.loc[datatable.ID] = [datatable.meta.get(x,None) for x in config.INVENTORY_FIELDS]
        #self.gitManager.updatedRepos.add('main') # TODO not needed anymore?
        

    def remove_from_inventory(self, tableID):
        """
        Method to remove a table from the global inventory
        Input: tableID
        """
        self.inventory.drop(tableID, inplace=True)
#        self.gitManager.updatedRepos.add('main')
    
    def getInventory(self, **kwargs):
        """
        Method to search through the inventory. kwargs can be all inventory entires
        (see config.INVENTORY_FIELDS).
        """
        
        table = self.inventory.copy()
        # loop over all keys of kwargs to filter based on all of them
        for key in kwargs.keys():
            
            mask = self.inventory[key].str.contains(kwargs[key], regex=False)
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            table = table.loc[mask].copy()
        
        # test to add function to a instance (does not require class)
        table.graph = types.MethodType( plot_query_as_graph, table )
        table.short = types.MethodType(shorten_find_output, table)
        return table

    def findp(self, level=None, regex=False, **filters):
        """ 
        Future defaulf find method that allows for more
        sophisticated syntax in the filtering
        
        Usage:
        -------
        filters : Union[str, Iterable[str]]
            One or multiple patterns, which are OR'd together
        regex : bool, optional
            Accept plain regex syntax instead of shell-style, default: False
        
        Returns
        -------
        matches : pd.Series
        Mask for selecting matched rows
        """    
            
        # filter by columns and list of values
        keep = True

        for field, pattern in filters.items():
            # treat `col=None` as no filter applied
            if pattern is None:
                continue

            if field not in self.inventory:
                raise ValueError(f'filter by `{field}` not supported')

            keep &= util.pattern_match(
                self.inventory[field], pattern, regex=regex
            )

        if level is not None:
            keep &= self.inventory['variable'].str.count(r"\|") == level

        table = self.inventory.loc[keep]

        # test to add function to a instance (does not require class)
        table.graph = types.MethodType(plot_query_as_graph, table)
        table.short = types.MethodType(shorten_find_output, table)
        return table

    
    def findExact(self, **kwargs):
        """
        Finds an exact match for the given filter criteria
        """
        table = self.inventory.copy()
        for key in kwargs.keys():
            mask = self.inventory[key] == kwargs[key]
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            table = table.loc[mask].copy()
        
        # test to add function to a instance (does not require class)
        table.graph = types.MethodType( plot_query_as_graph, table )
        table.short = types.MethodType(shorten_find_output, table)
        return table
    
    def _getTableFilePath(self,ID):
        """
        Private
        Return the file path for a given tableID
        """
        source = self.inventory.loc[ID].source
        fileName = self._getTableFileName(ID)
        return os.path.join(config.PATH_TO_DATASHELF, 'database/', source, 'tables', fileName)

    def _getTableFileName(self, ID):
        """
        For compatibility to windows based sytems, the pipe '|' symbols is replaces
        by double underscore '__' for the csv filename.
        """
        return ID.replace('|','-').replace('/','-') + '.csv'

    
    def getTable(self, ID):
        """
        Method to return the datatable for the given tableID.
        
        Input
        -----
        tableID : str
        
        Returns
        table : Datatable
        """
        if config.logTables:
            core.LOG['tableIDs'].append(ID)


        if self._tableExists(ID):
            # load table from database
            
            filePath = self._getTableFilePath(ID)
            return read_csv(filePath)
        else:
            #try to load locally from data
            
            if os.path.exists('data'):
                fileName = self._getTableFileName(ID)
                filePath = os.path.join('data', fileName)
                
                if os.path.exists(filePath):
                    return read_csv(filePath)
                else:
                    raise(BaseException('Table {} not found'.format(ID)))

    def getTables(self, iterIDs):
        """
        Method to return multiple datatables at once as a dictionary like 
        set fo tables.
        
        Input 
        -----
        table list : list [str]
        
        Returns
        tables : TableSet
        """
        
        if config.logTables:
            IDs = list()
        res = TableSet()
        for ID in iterIDs:
            table = self.getTable(ID)
            if config.logTables:
                IDs.append(ID)
            res.add(table)
        if config.logTables:
            core.LOG['tableIDs'].extend(IDs)
        return res
  
    def startLogTables(self):
        """
        Starts the logging of loaded datatables. This is useful to collect all
        required tables for a given analysis to create a datapackage for off-line
        useage
        """
        config.logTables = True
        core.LOG['tableIDs'] = list()
    
    def stopLogTables(self):
        """ 
        Stops the logging process of datatables and return the list of loaded
        table IDs for more processing.
        """
        import copy
        config.logTables = False
        outList = copy.copy(core.LOG['tableIDs'])
        #core.LOG.TableList = list()
        return outList
    
    def clearLogTables(self):
        """ 
        Clears the list of logged tables. This is anyway done if the package is
        newly loaded
        """
        core.LOG['tableIDs'] = list()
    
    def save_logged_tables(self,
                           folder='data'):
        """
        Creates a local data directory that can be used to run
        the logges analysis indepenedly.
        

        Parameters
        ----------
        folder : str, optional
            DESCRIPTION. The default is 'data'.

        Returns
        -------
        None.

        """
        #create folder if required
        if ~os.path.exists(folder):
            os.mkdir(folder)
        #save tables to disk
        self.saveTablesToDisk(folder, core.LOG['tableIDs'])
        if config.DEBUG:
            print('{} tables stored to directory {}'.format(
                len(core.LOG['tableIDS']), folder))
        
        
    def saveTablesToDisk(self, folder, IDList):
        """ 
        Function to save a list of tables to disk as csv files.
        """
        from shutil import copyfile
        import os 

        for ID in IDList:
            pathToFile = self._getTableFilePath(ID)
            print()
            copyfile(pathToFile, folder + '/' + os.path.basename(pathToFile))
          
            
    def isSource(self, sourceID):
        """
        Checks is the source is in the database
        
        Input
        ------
        sourceID : str
        """
        return self.gitManager.isSource(sourceID)

    def commitTable(self, dataTable, message, sourceMetaDict=None):
        """
        Adds a table permamently to the underlying database. For the first table
        of a new source, the meta data for the sources needs to be provides as well
        
        Input
        ------
        table : Datatable
        message : str
        sourceMetaDict [Optional] :  dict
        """
        sourceID = dataTable.meta['source']
        if not self.isSource(sourceID):
            if sourceMetaDict is None:
                raise(BaseException('Source does not exist and no sourceMeta provided'))
            else:
                if not( sourceMetaDict['SOURCE_ID'] in self.gitManager.sources.index):
                    self._addNewSource(sourceMetaDict)
        
        dataTable = util.cleanDataTable(dataTable)
        self._addTable(dataTable)
        self.add_to_inventory(dataTable)
           
        self._gitCommit(message)

    def commitTables(self, 
                     dataTables, 
                     message, 
                     sourceMetaDict, 
                     append_data=False, 
                     update=False, 
                     overwrite=False , 
                     cleanTables=True):
        """
        Adds multipe tables permamently to the underlying database. For the first table
        of a new source, the meta data for the sources needs to be provides as well
        
        Input
        ------
        tables : list of Datatable
        message : str
        sourceMetaDict [Optional] :  dict
        append_data [optinal]  : bool to choose if new data is added to the existing
                                 table (new data does not overwrite old data)
        update : [optional]    : bool to choose if the exting data is updated
        overwrire : [optional] : bool to choose if data is overwriten (new data 
                                 overwrites old data)
        cleanTables [optional] : bool (default: true) to choose if tables are 
                                 cleaned before commit
        
        TODO: Check flags
        """
        # create a new source if not extisting
        if not self.isSource(sourceMetaDict['SOURCE_ID']):
            self._addNewSource(sourceMetaDict)

        # only test if an table is update if the source did exist
        if update:
            oldTableIDs = [table.generateTableID() for table in dataTables]
            self.updateTables(oldTableIDs, dataTables, message)
            return            
        
        else:
            for dataTable in tqdm.tqdm(dataTables):
                if cleanTables:
                    dataTable = util.cleanDataTable(dataTable)
                
                if dataTable.isnull().all().all():
                    print('ommiting empty table: ' + dataTable.ID)
                    continue
                
                if dataTable.ID not in self.inventory.index:
                    # add entire new table
                    
                    self._addTable(dataTable)
                    self.add_to_inventory(dataTable)
                elif overwrite:
                    oldTable = self.getTable(dataTable.ID)
                    mergedTable = dataTable.combine_first(oldTable)
                    mergedTable = Datatable(mergedTable, meta = dataTable.meta)
                    self._addTable(mergedTable)
                elif append_data:
                    # append data to table
                    oldTable = self.getTable(dataTable.ID)
                    mergedTable = oldTable.combine_first(dataTable)
                    mergedTable = Datatable(mergedTable, meta = dataTable.meta)
                    self._addTable(mergedTable)
                
        self._gitCommit(message)
        

    def updateTable(self, oldTableID, newDataTable, message):
        """
        Specific method to update the data of an existing table
        
        Input
        -----
        oldTableID    : str
        newDataTabble : Datatable
        message       : str 
                        Commit message to describle the added data
        """
        sourceID = self._getSourceFromID(newDataTable.ID)
        if not self.isSource(sourceID):
            raise(BaseException('source  does not exist'))
            
        self._updateTable(oldTableID, newDataTable)
        self._gitCommit(message)
    
    def updateTables(self, oldTableIDs, newDataTables, message):
        """
        Equivalent method to updateTable, but for multiple tables at once
        
        Input
        -----
        oldTableIDs    : list of str
        newDataTabbles : list of Datatable
        message        : str 
                         Commit message to describle the added data
        """ 
        sourcesToUpdate = list()
        for tableID in oldTableIDs:
            sourceID = self._getSourceFromID(tableID)
            if sourceID not in sourcesToUpdate:
                sourcesToUpdate.append(sourceID)
        
        # check that all sources do exist
        for sourceID in sourcesToUpdate:
            if not self.isSource(sourceID):
                raise(BaseException('source  does not exist'))
        
        # loop over tables
        for oldTableID, newDataTable in tqdm.tqdm(zip(oldTableIDs, newDataTables)):
            
            if oldTableID in self.inventory.index:
                self._updateTable(oldTableID, newDataTable)
            else:
                dataTable = util.cleanDataTable(newDataTable)
                self._addTable(dataTable)
                self.add_to_inventory(dataTable)
        self._gitCommit(message)

    def _updateTable(self, oldTableID, newDataTable):
        """
        Private
        
        Method as a common function form multiple functions
        """
        newDataTable = util.cleanDataTable(newDataTable)
        
        oldDataTable = self.getTable(oldTableID)
        oldID = oldDataTable.meta['ID']
        newID = newDataTable.generateTableID()
        
        if oldID == newID and (oldDataTable.meta['unit'] == newDataTable.meta['unit']):
            #only change data
            self._addTable( newDataTable)
        else:
            # delete old table
            self.removeTable(oldID)
            
            # add new table
            self._addTable( newDataTable)

            #change inventory
            self.inventory.rename(index = {oldID: newID}, inplace = True)
            self.add_to_inventory(newDataTable)


    def validate_ID(self, ID, print_statement=True):
        """ 
        Method to chekc the validity of a table ID and check the state of the 
        data
        """
        RED = '\033[31m'
        GREEN = '\033[32m'
        BLACK = '\033[30m'
        source = ID.split(config.ID_SEPARATOR)[-1]
        print('TableID: {}'.format(ID))
        valid = list()
        if self.sourceExists(source):
            if print_statement:
                print(GREEN + "Source {} does exists".format(source))
            valid.append(True)
        else:
            if print_statement:
                print(RED + "Source {} does not exists".format(source))
            valid.append(False)
        if ID in self.inventory.index:
            if print_statement:
                print(GREEN + "ID is in the inventory")
            valid.append(True)
        else:
            if print_statement:
                print(RED + "ID is missing in the inventory")
            valid.append(False)
            
        fileName = self._getTableFileName(ID)
        tablePath = os.path.join(config.PATH_TO_DATASHELF, 'database', source, 'tables', fileName)

        if os.path.isfile(tablePath):
            if print_statement:
                print(GREEN + "csv file exists")
            valid.append(True)
        else:
            if print_statement:
                print(RED + "csv file does not exists")
            
            valid.append(False)

        print(BLACK)

        return all(valid)

    def removeTables(self, IDList):
        """
        Method to remnove tables from the database
        
        Input
        -----
        IDList : list of str
        """
        
        sourcesToUpdate = list()
        for tableID in IDList:
            sourceID = self._getSourceFromID(tableID)
            if sourceID not in sourcesToUpdate:
                sourcesToUpdate.append(sourceID)
        
        # check that all sources do exist
        for source in sourcesToUpdate:
            if not self.isSource(sourceID):
                raise(BaseException('source  does not exist'))
        
        for ID in IDList:
            source = self.inventory.loc[ID, 'source']
            tablePath = self._getTableFilePath(ID)

            self.remove_from_inventory(ID)
            self.gitManager.gitRemoveFile(source, tablePath)

        self._gitCommit('Tables removed')
        
    def removeTable(self, tableID):
        """
        Method to remnove tables from the database
        
        Input
        -----
        tableID : str
        """
        sourceID = self._getSourceFromID(tableID)
        if not self.isSource(sourceID):
            raise(BaseException('source  does not exist'))
        
        self._removeTable(tableID)
        self._gitCommit('Table removed')
        self._reloadInventory()

    def _removeTable(self, ID):
        """
        Private method
        Function to pool code for removing a table from the database
        """
        source = self.inventory.loc[ID, 'source']
        tablePath = self._getTableFilePath(ID)
        self.remove_from_inventory(ID)
#        self.gitManager[source].execute(["git", "rm", tablePath])
        self.gitManager.gitRemoveFile(source, tablePath)

        
        
    def _tableExists(self, ID):
        return ID in self.inventory.index
    


    def tableExist(self, tableID):
        """
        Method to test if table exists in the database
        
        Input
        -----
        tableID : str
        """
        
        return self._tableExists(tableID)

    def isConsistentTable(self, datatable):
        """
        Checks if that table is fitting the following requirements 
        - numeric data 
        - spatial identifiers are known to the database
        - columns are propper years
        - index is not duplicated
        """
        if not np.issubdtype(datatable.values.dtype, np.number):
            
            raise(BaseException('Sorry, data of table {} is needed to be numeric'.format(datatable)))            
            
        # check that spatial index is consistend with defined countries or regions
        invalidSpatialIDs = datatable.index.difference(mapp.getValidSpatialIDs())
        if len(invalidSpatialIDs) > 0:
            raise(BaseException('Sorry, regions in table {}: {} do not exist'.format(datatable, invalidSpatialIDs)))
        
        # check that the time colmns are years
        from pandas.api.types import is_integer_dtype
        if not is_integer_dtype(datatable.columns):
            raise(BaseException('Sorry, year index in table {} is not integer'.format(datatable)))

        if sum(datatable.index.duplicated()) > 0:
            print(datatable.meta)
            raise(BaseException('Sorry, region index in table {} is not  unique'.format(datatable)))
        return True
    

    def _addTable(self, datatable):
        """
        Private
        Pools functionality to add table to the database
        """
        
        ID = datatable.generateTableID()
        source = datatable.source()
        datatable.meta['creator'] = config.CRUNCHER
        sourcePath = os.path.join('database', source)
        filePath = os.path.join(sourcePath, 'tables',  self._getTableFileName(ID))
        if (config.OS == 'win32') | (config.OS == "Windows"):
            filePath = filePath.replace('|','___')
        
        datatable = datatable.sort_index(axis='index')
        datatable = datatable.sort_index(axis='columns')
        
        
        
        self.isConsistentTable(datatable)
        

        self._gitAddTable(datatable, source, filePath)

    def _gitAddTable(self, datatable, source, filePath):
        """
        Private
        Added file to git system
        """
        datatable.to_csv(os.path.join(config.PATH_TO_DATASHELF, filePath))
        
        self.gitManager.gitAddFile(source, os.path.join('tables', self._getTableFileName(datatable.ID)))
        
    def _gitCommit(self, message):
        """
        Private
        Commits all changes to git
        """
        self.inventory.to_csv(self.INVENTORY_PATH)
        self.gitManager.gitAddFile('main',self.INVENTORY_PATH)

        for sourceID in self.gitManager.updatedRepos:
            if sourceID == 'main':
                continue
            repoPath = os.path.join(config.PATH_TO_DATASHELF, 'database', sourceID)
            sourceInventory = self.inventory.loc[self.inventory.source==sourceID,:]
            sourceInventory.to_csv(os.path.join(repoPath, 'source_inventory.csv'))
            self.gitManager.gitAddFile(sourceID, os.path.join(repoPath, 'source_inventory.csv'))  
        self.gitManager.commit(message)
            

    def _addNewSource(self, sourceMetaDict):
        """
        Private
        Adds new source to the sources table 
        """
        source_ID = sourceMetaDict['SOURCE_ID']
        
        if not self.sourceExists(source_ID):
            
            sourcePath = os.path.join(config.PATH_TO_DATASHELF, 'database', sourceMetaDict['SOURCE_ID'])
            self.gitManager.init_new_repo(sourcePath, source_ID, sourceMetaDict)
            

        else:
            print('source already exists')

    def _getSourceFromID(self, tableID):
        """
        Private
        Returns the source of a given table
        """
        return tableID.split(config.ID_SEPARATOR)[-1]
    
    
    def removeSource(self, sourceID):
        """
        Function to remove an entire source from the database. 
        """
        import shutil
        if self.sourceExists(sourceID):
            sourcePath = os.path.join(config.PATH_TO_DATASHELF, 'database', sourceID)
            shutil.rmtree(sourcePath, ignore_errors=False, onerror=None)
        self.gitManager.sources = self.gitManager.sources.drop(sourceID, axis=0)
        self.sources            = self.gitManager.sources
        tableIDs = self.inventory.index[self.inventory.source==sourceID]
        self.inventory = self.inventory.drop(tableIDs, axis=0)
#        self.gitManager.updatedRepos.add('main')
        self._gitCommit(sourceID + ' deleted')
    
    def updateExcelInput(self, fileName):
        """
        This function updates all data values that are defined in the input sheet
        in the given excel file
        """
        if config.DB_READ_ONLY:
            assert self._validateRepository()
        ins = io.Inserter(fileName='demo.xlsx')
        for setup in ins.getSetups():
            dataTable = self.getTable(setup['dataID'])
            ins._writeData(setup, dataTable)

    #database mangement
    
    def _checkTablesOnDisk(self):
        notExistingTables = list()
        for tableID in self.inventory.index:
            filePath = self._getTableFilePath(tableID)
            if not os.path.exists(filePath):
                notExistingTables.append(tableID)
        
        return notExistingTables
                
      
    def importSourceFromRemote(self, remoteName):
        """
        This functions imports (git clone) a remote dataset and creates a local
        copy of it.
        
        Input is an existing sourceID. 
        """
        repoPath = os.path.join(config.PATH_TO_DATASHELF, 'database', remoteName)
        
        self.gitManager.clone_source_from_remote(remoteName, repoPath)

        sourceInventory = pd.read_csv(os.path.join(repoPath, 'source_inventory.csv'), index_col=0, dtype={'source_year': str})
        for idx in sourceInventory.index:
            self.inventory.loc[idx,:] = sourceInventory.loc[idx,:]
        self._gitCommit('imported ' + remoteName)

    def exportSourceToRemote(self, sourceID):
        """
        This function exports a new local dataset to the defind remote database.
        
        Input is a local sourceID as a str.
        """
        self.gitManager.create_remote_repo(sourceID)
        self.gitManager.push_to_remote_datashelf(sourceID)
        print('export successful: ({})'.format( config.DATASHELF_REMOTE +  sourceID))
        

#%%
class GitRepository_Manager:
    """
    # Management of git repositories for fast access
    """
    def __init__(self, config):
        self.PATH_TO_DATASHELF = config.PATH_TO_DATASHELF
        self.sources = pd.read_csv(config.SOURCE_FILE, index_col='SOURCE_ID')
        
        self.repositories = dict()
        self.updatedRepos = set()
        self.validatedRepos = set()
        self.filesToAdd   = defaultdict(list)

        for sourceID in self.sources.index:
            repoPath = os.path.join(self.PATH_TO_DATASHELF,  'database', sourceID)
            self.repositories[sourceID] = git.Repo(repoPath)
            self.verifyGitHash(sourceID)
        
        self.repositories['main'] = git.Repo(self.PATH_TO_DATASHELF)
        self._validateRepository('main')

    def __getitem__(self, sourceID):
        """ 
        Retrieve `sourceID` from repositories dictionary and ensure cleanliness
        """
        repo = self.repositories[sourceID]
        if sourceID not in self.validatedRepos:
            self._validateRepository(sourceID)
        return repo
    
    def _validateRepository(self, sourceID):
        """ 
        Private
        Cheks if sourceID points to a valid repository
        
        """
        repo = self.repositories[sourceID]

        if sourceID != 'main':
            self.verifyGitHash(sourceID)

        if repo.is_dirty():
            raise RuntimeError('Git repo: "{}" is inconsistent! - please check uncommitted modifications'.format(sourceID))

        config.DB_READ_ONLY = False
        if config.DEBUG:
            print('Repo {} is clean'.format(sourceID))
        self.validatedRepos.add(sourceID)
        return True
        
    def init_new_repo(self, repoPath, repoID, sourceMetaDict):
        """
        Method to create a new repository for a source
        
        Input
        ----
        repoPath : str
        repoID   : str 
        sourceMetaDict : dict with the required meta data descriptors
        """
        self.sources.loc[repoID] = pd.Series(sourceMetaDict)
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile('main', config.SOURCE_FILE)

        repoPath = Path(repoPath)
        print(f'creating folder {repoPath}')
        repoPath.mkdir(parents=True, exist_ok=True)
        self.repositories[repoID] = git.Repo.init(repoPath)

        for subFolder in config.SOURCE_SUB_FOLDERS:
            subPath = repoPath / subFolder
            subPath.mkdir(exist_ok=True)
            filePath = subPath / '.gitkeep'
            filePath.touch()
            self.gitAddFile(repoID, filePath)

        metaFilePath = repoPath / 'meta.csv'
        util.dict_to_csv(sourceMetaDict, metaFilePath)
        self.gitAddFile(repoID, metaFilePath)

        self.commit('added source: ' + repoID)

    def gitAddFile(self, repoName, filePath):
        """
        Addes a new file to a repository
        
        Input
        -----
        repoName : str 
        filePath : str of the relative file path
        """
        if config.DEBUG:
            print('Added file {} to repo: {}'.format(filePath,repoName))
        
        self.filesToAdd[repoName].append(str(filePath))
        self.updatedRepos.add(repoName)
        
    def gitRemoveFile(self, repoName, filePath):
        """
        Removes a file from the git repository
        
        Input
        -----
        repoName : str 
        filePath : str of the relative file path
        """
        if config.DEBUG:
            print('Removed file {} to repo: {}'.format(filePath,repoName))
#        self[repoName].index.remove(filePath, working_tree=True)
        self[repoName].git.execute(["git", "rm", "-f", filePath]) #TODO Only works with -f forced, but why?
        self.updatedRepos.add(repoName)
    
    def _gitUpdateFile(self, repoName, filePath):
        pass
        
    def commit(self, message):
        """
        Function to commit all oustanding changes to git. All repos including 
        'main' are commited if there is any change
        
        Input
        ----
        message : str - commit message
        """
        if 'main' in self.updatedRepos:
            self.updatedRepos.remove('main')

        for repoID in self.updatedRepos:
            repo = self.repositories[repoID]
            repo.index.add(self.filesToAdd[repoID])
            commit = repo.index.commit(message + " by " + config.CRUNCHER)
            self.sources.loc[repoID, 'git_commit_hash'] = commit.hexsha
            del self.filesToAdd[repoID]
        
        # commit main repository
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile('main', config.SOURCE_FILE)

        main_repo = self['main']
        main_repo.index.add(self.filesToAdd['main'])
        main_repo.index.commit(message + " by " + config.CRUNCHER)
        del self.filesToAdd['main']

        #reset updated repos to empty
        self.updatedRepos        = set()
        
        
    def create_remote_repo(self, repoName):
        """
        Function to create a remove git repoisitoy from an existing local repo
        """
        repo = self[repoName]
        if 'origin' in repo.remotes:
            print('remote origin already exists, skip')
            return 

        origin = repo.create_remote("origin", config.DATASHELF_REMOTE + repoName + ".git")
        origin.fetch()

        repo.heads.master.set_tracking_branch(origin.refs.master)
        origin.push(repo.heads.master)
    
    def push_to_remote_datashelf(self, repoName):
        """
        This function used git push to update the remote database with an updated
        source dataset. 
        
        Input is the source ID as a str.
        
        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO

        """
        self[repoName].remote('origin').push(progress=TqdmProgressPrinter())
        
    def clone_source_from_remote(self, repoName, repoPath):
        """
        Function to clone a remote git repository as a local copy.
        
        Input
        ----- 
        repoName : str - valid repository in the remove database
        repoPath : str - path of the repository
        """
        
        url = config.DATASHELF_REMOTE + repoName + '.git'
        repo = git.Repo.clone_from(url=url, to_path=repoPath, progress=TqdmProgressPrinter())  
        self.repositories[repoName] = repo

        # Update source file
        sourceMetaDict = util.csv_to_dict(os.path.join(repoPath, 'meta.csv'))
        sourceMetaDict['git_commit_hash'] = repo.commit().hexsha
        self.sources.loc[repoName] = pd.Series(sourceMetaDict)   
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile('main', config.SOURCE_FILE) 

        return repo
        
    def pull_update_from_remote(self, repoName):
        """
        This function used git pull an updated remote source dataset to the local
        database.
        
        Input is the source ID as a str.
        
        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO

        """
        self[repoName].remote('origin').pull(progress=TqdmProgressPrinter())
        self.updateGitHash(repoName)
        self.commit('udpate from remote')
    
    def verifyGitHash(self, repoName):
        """
        Function to verify the git hash code of an existing git repository
        """
        repo = self.repositories[repoName]
        if repo.commit().hexsha != self.sources.loc[repoName, 'git_commit_hash']:
            raise RuntimeError('Source {} is inconsistent with overall database'.format(repoName))

    def updateGitHash(self, repoName):
        """
        Function to update the git hash code in the sources.csv by the repo hash code
        """
        self.sources.loc[repoName,'git_commit_hash'] = self[repoName].commit().hexsha
        
    def setActive(self, repoName):
        """
        Function to set a reposity active
        """
        self[repoName].git.refresh()
        
    def isSource(self, sourceID):
        if sourceID in self.sources.index:
            self[sourceID].git.refresh()
            return True
        else:
            return False
        
class TqdmProgressPrinter(git.RemoteProgress):
    known_ops = {
        git.RemoteProgress.COUNTING: "counting objects",
        git.RemoteProgress.COMPRESSING: "compressing objects",
        git.RemoteProgress.WRITING: "writing objects",
        git.RemoteProgress.RECEIVING: "receiving objects",
        git.RemoteProgress.RESOLVING: "resolving stuff",
        git.RemoteProgress.FINDING_SOURCES: "finding sources",
        git.RemoteProgress.CHECKING_OUT: "checking things out"
    }

    def __init__(self):
        super().__init__()
        self.progressbar = None

    def update(self, op_code, cur_count, max_count=None, message=''):
        if op_code & self.BEGIN:
            desc = self.known_ops.get(op_code & self.OP_MASK)
            self.progressbar = tqdm.tqdm(desc=desc, total=max_count)

        self.progressbar.set_postfix_str(message, refresh=False)
        self.progressbar.update(cur_count)

        if op_code & self.END:
            self.progressbar.close()


