#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 14:55:32 2019

@author: and
"""

import pandas as pd
import os 
import git
import tqdm
import time
import copy
import types
from collections import defaultdict
from pathlib import Path

from . import config
from .data_structures import Datatable, TableSet, read_csv
from .utilities import plot_query_as_graph
from . import mapping as mapp
from . import io_tools as io
from . import util
from . import core

class Database():
    
    def __init__(self):
        tt = time.time()
        self.path = config.PATH_TO_DATASHELF
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
            
    
    def _validateRepository(self, repoID='main'):
        return self.gitManager._validateRepository(repoID)
    
    def info(self):
        #%%
        print('######## Database informations: #############')
        print('Number of tables: {}'.format(len(self.inventory)))  
        print('Number of data sources: {}'.format(len(self.gitManager.sources))) 
        print('Number of commits: {}'.format(self.gitManager['main'].git.rev_list("--all", "--count")))
#        root_directory = Path(config.PATH_TO_DATASHELF)
#        print('Size of datashelf: {:2.2f} MB'.format(sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file() )/1e6))
#        root_directory = Path(os.path.join(config.PATH_TO_DATASHELF, 'rawdata'))
#        print('Size of raw_data: {:2.2f} MB'.format(sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file() )/1e6))
        print('#############################################')
        #%%
    def sourceInfo(self):
        sources = copy.copy(self.gitManager.sources)
        return sources.sort_index()

    def returnInventory(self):
        return copy.copy(self.inventory)


    def _reloadInventory(self):
        self.inventory = pd.read_csv(self.INVENTORY_PATH, index_col=0, dtype={'source_year': str})
        
    def sourceExists(self, source):
        return source in self.gitManager.sources.index
    
    def add_to_inventory(self, datatable):
#        if config.DB_READ_ONLY:
#            assert self._validateRepository()
#        entry = [datatable.meta[key] for key in config.ID_FIELDS]
#        print(entry)
#        self.inventory.loc[datatable.ID] = entry
       
        self.inventory.loc[datatable.ID] = [datatable.meta.get(x,None) for x in config.INVENTORY_FIELDS]
        #self.gitManager.updatedRepos.add('main')

    def remove_from_inventory(self, tableID):
        self.inventory.drop(tableID, inplace=True)
#        self.gitManager.updatedRepos.add('main')
    
    def getInventory(self, **kwargs):
        
        table = self.inventory.copy()
        for key in kwargs.keys():
            #table = table.loc[self.inventory[key] == kwargs[key]]
            mask = self.inventory[key].str.contains(kwargs[key], regex=False)
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            table = table.loc[mask].copy()
        
        # test to add function to a instance (does not require class)
        table.graph = types.MethodType( plot_query_as_graph, table )
        
        return table

    def findp(self, level=None, regex=False, **filters):
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

        return table

    def findExact(self, **kwargs):
        
        table = self.inventory.copy()
        for key in kwargs.keys():
            #table = table.loc[self.inventory[key] == kwargs[key]]
            mask = self.inventory[key] == kwargs[key]
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            table = table.loc[mask].copy()
        
        # test to add function to a instance (does not require class)
        table.graph = types.MethodType( plot_query_as_graph, table )
        
        return table
    
    def _getTableFilePath(self,ID):
        source = self.inventory.loc[ID].source
        fileName = self._getTableFileName(ID)
        return os.path.join(config.PATH_TO_DATASHELF, 'database/', source, 'tables', fileName)

    def _getTableFileName(self, ID):
        return ID.replace('|','-').replace('/','-') + '.csv'

    
    def getTable(self, ID):
        
        if config.logTables:
            core.LOG['tableIDs'].append(ID)

        filePath = self._getTableFilePath(ID)
        return read_csv(filePath)

#    def _getTableWindows(self, ID):
#        
#        if config.logTables:
#            core.LOG['tableIDs'].append(ID)
#
#        filePath = self._getTableFilePath(ID).replace('|','___')
#        return read_csv(filePath)

    def getTables(self, iterIDs):
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
        config.logTables = True
        core.LOG['tableIDs'] = list()
    
    def stopLogTables(self):
        import copy
        config.logTables = False
        outList = copy.copy(core.LOG['tableIDs'])
        #core.LOG.TableList = list()
        return outList
    
    def clearLogTables(self):
        core.LOG['tableIDs'] = list()

    def isSource(self, sourceID):
        return self.gitManager.isSource(sourceID)

    def commitTable(self, dataTable, message, sourceMetaDict=None):
        
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
        
        sourceID = self._getSourceFromID(newDataTable.ID)
        if not self.isSource(sourceID):
            raise(BaseException('source  does not exist'))
            
        self._updateTable(oldTableID, newDataTable)
        self._gitCommit(message)
    
    def updateTables(self, oldTableIDs, newDataTables, message):

        """
        same as updateTable, but for list of tables
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
        
        
        for oldTableID, newDataTable in tqdm.tqdm(zip(oldTableIDs, newDataTables)):
            
            if oldTableID in self.inventory.index:
                self._updateTable(oldTableID, newDataTable)
            else:
                dataTable = util.cleanDataTable(newDataTable)
                self._addTable(dataTable)
                self.add_to_inventory(dataTable)
        self._gitCommit(message)

    def _updateTable(self, oldTableID, newDataTable):
        
        newDataTable = util.cleanDataTable(newDataTable)
        
        oldDataTable = self.getTable(oldTableID)
        oldID = oldDataTable.meta['ID']
        #print(oldDataTable.meta)
        newID = newDataTable.generateTableID()
        
        if oldID == newID and (oldDataTable.meta['unit'] == newDataTable.meta['unit']):
            #only change data
            self._addTable( newDataTable)
        else:
            # delete old table
#            tablePath = self._getPathOfTable(oldID)
            self.removeTable(oldID)
            
            # add new table
            self._addTable( newDataTable)

            #change inventory
            self.inventory.rename(index = {oldID: newID}, inplace = True)
            self.add_to_inventory(newDataTable)


    def validate_ID(self, ID, print_statement=True):
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

        sourceID = self._getSourceFromID(tableID)
        if not self.isSource(sourceID):
            raise(BaseException('source  does not exist'))
        
        self._removeTable( tableID)
        self._gitCommit('Table removed')
        self._reloadInventory()

    def _removeTable(self, ID):

        source = self.inventory.loc[ID, 'source']
        tablePath = self._getTableFilePath(ID)
        self.remove_from_inventory(ID)
        
#        self.gitManager[source].execute(["git", "rm", tablePath])
        self.gitManager.gitRemoveFile(source, tablePath)
#        self._gitCommit('Table removed')
#        self._reloadInventory()
        
        
    def _tableExists(self, ID):
        return ID in self.inventory.index
    


    def tableExist(self, tableID):
        return self._tableExists(tableID)

    def isConsistentTable(self, datatable):
        
        if not pd.np.issubdtype(datatable.values.dtype, pd.np.number):
            
            raise(BaseException('Sorry, data of table {} is needed to be numeric'.format(datatable)))            
            
        # check that spatial index is consistend with defined countries or regions
        for regionIdx in datatable.index:
            #print(regionIdx + '-')
            if not mapp.regions.exists(regionIdx) and not mapp.countries.exists(regionIdx):
                raise(BaseException('Sorry, region in table {}: {} does not exist'.format(datatable, regionIdx)))
        
        # check that the time colmns are years
        from pandas.api.types import is_integer_dtype
        if not is_integer_dtype(datatable.columns):
            raise(BaseException('Sorry, year index in table {} is not integer'.format(datatable)))

        if sum(datatable.index.duplicated()) > 0:
            print(datatable.meta)
            raise(BaseException('Sorry, region index in table {} is not  unique'.format(datatable)))
        return True
    

    def _addTable(self, datatable):
    
        
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
        
#        if not self.sourceExists(source):
            
        
#        tt = time.time()
        self._gitAddTable(datatable, source, filePath)
#        print('time: {:2.6f}'.format(time.time()-tt))
        #self.add_to_inventory(datatable)
        
    def _gitAddTable(self, datatable, source, filePath):
        
        datatable.to_csv(os.path.join(config.PATH_TO_DATASHELF, filePath))
        
        self.gitManager.gitAddFile(source, os.path.join('tables', self._getTableFileName(datatable.ID)))
        
    def _gitCommit(self, message):
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
        source_ID = sourceMetaDict['SOURCE_ID']
        
        if not self.sourceExists(source_ID):
            
            sourcePath = os.path.join(config.PATH_TO_DATASHELF, 'database', sourceMetaDict['SOURCE_ID'])
            self.gitManager.init_new_repo(sourcePath, source_ID, sourceMetaDict)
            

        else:
            print('source already exists')

    def _getSourceFromID(self, tableID):
        return tableID.split(config.ID_SEPARATOR)[-1]
    
    
    def removeSource(self, sourceID):
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

#    if config.DB_READ_ONLY:
#        def commitTable(self, dataTable, message, sourceMetaDict):
#            
#            raise(BaseException('Not possible in read only mode'))
#    
#        def commitTables(self, dataTables, message, sourceMetaDict, append_data=False):
#    
#            raise(BaseException('Not possible in read only mode'))
#
#            
#    
#        def updateTable(self, oldTableID, newDataTable, message):
#            raise(BaseException('Not possible in read only mode'))
#        
#        def updateTables(self, oldTableIDs, newDataTables, message):
#            raise(BaseException('Not possible in read only mode'))
            
    #%% database mangement
    
    def _checkTablesOnDisk(self):
        notExistingTables = list()
        for tableID in self.inventory.index:
            filePath = self._getTableFilePath(tableID)
            if not os.path.exists(filePath):
                notExistingTables.append(tableID)
        
        return notExistingTables
                
    def saveTablesToDisk(self, folder, IDList):
        from shutil import copyfile
        import os 
        #%%
#        _getTableFilePath = dt.core.DB._getTableFilePath
        
        for ID in IDList:
            pathToFile = self._getTableFilePath(ID)
            print()
            copyfile(pathToFile, folder + '/' + os.path.basename(pathToFile))
            
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

    def gitAddFile(self, repoName, filePath, addToGit=True):
        if config.DEBUG:
            print('Added file {} to repo: {}'.format(filePath,repoName))
        
        self.filesToAdd[repoName].append(str(filePath))
        self.updatedRepos.add(repoName)
        
    def gitRemoveFile(self, repoName, filePath):
        if config.DEBUG:
            print('Removed file {} to repo: {}'.format(filePath,repoName))
        self[repoName].index.remove(filePath, working_tree=True)
        self.updatedRepos.add(repoName)
    
    def _gitUpdateFile(self, repoName, filePath):
        pass
        
    def commit(self, message):
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
        self.commit()
    
    def verifyGitHash(self, repoName):
        repo = self.repositories[repoName]
        if repo.commit().hexsha != self.sources.loc[repoName, 'git_commit_hash']:
            raise RuntimeError('Source {} is inconsistent with overall database'.format(repoName))

    def updateGitHash(self, repoName):
        self.sources.loc[repoName,'git_commit_hash'] = self[repoName].commit().hexsha
        
    def setActive(self, repoName):
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


