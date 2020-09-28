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
from dataclasses import dataclass
from typing import Dict, Optional, Mapping, Union

from . import config
from .data_structures import Datatable, TableSet, read_csv
from .utilities import plot_query_as_graph
from . import mapping as mapp
from . import io_tools as io
from . import util
from . import core


class DatatoolboxException(Exception):
    pass


class Source:
    pass


@dataclass
class Inventory:
    data: pd.DataFrame
    sources: Dict[str, Source]

    @classmethod
    def from_datashelf(cls, path: Union[str, os.PathLike, None] = None) -> Inventory:
        datashelf = DatashelfRepository(path)
        return cls.from_sources(datashelf.sources())

        path = Path(path)

        return cls.from_sources(sources)

    @classmethod
    def from_sources(cls, sources: Mapping[Source]) -> Inventory:
        data = pd.concat(
            (
                source.inventory.assign(source=name)
                for name, source in sources.items()
            ),
            sort=False
        )

        return cls(data, dict(sources))

    def reload_inventory(self) -> None:
        self.data = pd.concat(
            (
                source.inventory.assign(source=name)
                for name, source in sources.items()
            ),
            sort=False
        )

    def update_sources(self, *sources: Union[str, Source]) -> None:
        if len(sources) == 0:
            sources = list(self.sources.values())

        source_names = []
        source_objs = []
        for source in sources:
            if isinstance(source, str):
                source = self.sources[source]

            source.update()
            source_objs.append(source)
            source_names.append(source.name)

        self.data = pd.concat(
            [self.data.loc[~self.data.source.isin(source_names)]] +
            [source.inventory for source in source_objs],
            sort=False
        )

    def add_source(self, source: Source) -> None:
        self.sources[source.name] = source
        self.data.append(
            source.inventory.assign(source=source.name),
            inplace=True, sort=False
        )

    def remove_source(self, source: Union[str, Source]) -> None:
        if isinstance(source, Source):
            source = source.name

        source.remove()

        self.data = self.data.loc[self.data.source != source]
        del self.sources[source]

    def info(self):
        print('######## Database informations: #############')
        print('Number of tables: {}'.format(len(self.data)))
        print('Number of data sources: {}'.format(len(self.sources)))
        print('#############################################')

    def source_info(self):
        return pd.DataFrame([s.meta for s in self.sources]).sort_index()

    def filter(self, /, regex: bool = False, **filters) -> Inventory:

        data = self.data
        # ...

        # create shallow copies
        return Inventory(
            pd.DataFrame(data),
            dict(self.sources)
        )


class Database():
    
    def __init__(self):
        tt = time.time()
        self.path = config.PATH_TO_DATASHELF
        self.gitManager = GitRepository_Manager(config)
        self.INVENTORY_PATH = os.path.join(self.path, 'inventory.csv')
        self.inventory = pd.read_csv(self.INVENTORY_PATH, index_col=0, dtype={'source_year': str})
        self.sources   = self.gitManager.sources
        
        self.gitManager._validateRepository('main')
            
#        if (config.OS == 'win32') | (config.OS == "Windows"):
#            self.getTable = self._getTableWindows
#        else:
#            self.getTable = self._getTableLinux

        if config.DEBUG:
            print('Database loaded in {:2.4f} seconds'.format(time.time()-tt))
    
    def _validateRepository(self, repoID='main'):
        return self.gitManager._validateRepository(repoID)
    

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
                raise(BaseException('Source does not extist and now sourceMeta provided'))
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
@dataclass
class GitRepositorySource(Source):
    """
    # Management of git repositories for fast access
    """
    repo: git.Repo
    inventory: pd.DataFrame
    is_clean: bool = False

    @classmethod
    def from_repo(cls, repo: git.Repo) -> GitRepositorySource:
        inventory = pd.read_csv(str(Path(path) / "source_inventory.csv"), index_col=0, dtype={'source_year': str})
        return cls(repo, inventory)

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> GitRepositorySource:
        return cls.from_repo(git.Repo(path))
    
    @classmethod
    def clone_from_remote(cls, name: str, path: Union[str, os.PathLike, None] = None) -> GitRepositorySource:
        if path is None:
            path = Path(config.PATH_TO_DATASHELF) / 'database' / name

        url = config.DATASHELF_REMOTE + name + '.git'
        repo = git.Repo.clone_from(url=url, to_path=path, progress=TqdmProgressPrinter())  

        return cls.from_repo(repo)

    @classmethod
    def init(cls, name: str, meta: Mapping[str, str], path: Union[str, os.PathLike, None] = None) -> GitRepositorySource:
        if path is None:
            path = Path(config.PATH_TO_DATASHELF) / 'database' / name
        else:
            path = Path(path)

        print(f'creating folder {path}')
        path.mkdir(parents=True, exist_ok=True)
        repo = git.Repo.init(path)

        for folder in config.SOURCE_SUB_FOLDERS:
            sub_path = path / folder
            sub_path.mkdir(exist_ok=True)
            gitkeep = sub_path / '.gitkeep'
            gitkeep.touch()
            repo.index.add(gitkeep)

        meta_path = path / 'meta.csv'
        util.dict_to_csv(meta, meta_path)
        repo.index.add(meta_path)

        inventory_path = path / "source_inventory.csv"
        inventory = pd.DataFrame(columns=config.INVENTORY_FIELDS)
        inventory.to_csv(inventory_path)
        repo.index.add(inventory_path)

        repo.index.commit('added source: ' + name)

        return cls(repo, inventory)

    def check_clean(self) -> bool:
        is_clean = self.is_clean = not self.repo.is_dirty()
        return is_clean

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
        origin.push(repo.heads.master, progress=TqdmProgressPrinter())
    
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
