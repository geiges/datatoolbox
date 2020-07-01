#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 14:55:32 2019

@author: and
"""

from . import config
from .data_structures import Datatable, TableSet, read_csv

from . import mapping as mapp
from . import io_tools as io
from . import util
from . import core
import pandas as pd
import os 
import git
import tqdm
import time
import copy


class Database():
    
    def __init__(self):
        tt = time.time()
        self.path = config.PATH_TO_DATASHELF
        self.gitManager = GitRepository_Manager(config)
        self.INTVENTORY_PATH = os.path.join(self.path, 'inventory.csv')
        self.inventory = pd.read_csv(self.INTVENTORY_PATH, index_col=0)
        self.sources   = self.gitManager.sources
        
        self.gitManager._validateRepository('main')
            
#        if (config.OS == 'win32') | (config.OS == "Windows"):
#            self.getTable = self._getTableWindows
#        else:
#            self.getTable = self._getTableLinux

        if config.DEBUG:
            print('Database loaded in {:2.4f} seconds'.format(time.time()-tt))
    
    def _validateRepository(self, repoID='main'):
        if self.gitManager[repoID].diff() != '':
            raise(Exception('database is inconsistent! - please check uncommitted modifications'))
        else:
            config.DB_READ_ONLY = False
            print('databdase in write mode')
            return True
    
    def info(self):
        #%%
        from pathlib import Path
        print('######## Database informations: #############')
        print('Number of tables: {}'.format(len(self.inventory)))  
        print('Number of data sources: {}'.format(len(self.gitManager.sources))) 
        print('Number of commits: {}'.format(self.gitManager['main'].execute(["git", "rev-list", "--all", "--count"])))
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
        self.inventory = pd.read_csv(self.INTVENTORY_PATH, index_col=0)
        
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
            
        
        return table

    def findExact(self, **kwargs):
        
        table = self.inventory.copy()
        for key in kwargs.keys():
            #table = table.loc[self.inventory[key] == kwargs[key]]
            mask = self.inventory[key] == kwargs[key]
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            table = table.loc[mask].copy()
            
        
        return table
    
    def _getTableFilePath(self,ID):
        source = self.inventory.loc[ID].source
        fileName = self._getTableFileName(ID)
        return os.path.join(config.PATH_TO_DATASHELF, 'database/', source, 'tables', fileName)

    def _getTableFileName(self, ID):
        return ID.replace('|','-') + '.csv'

    
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
        for source in sourcesToUpdate:
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
        
        if oldID == newID:
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
            raise(BaseException('Sorry, data is needed to be numeric'))            
            
        # check that spatial index is consistend with defined countries or regions
        for regionIdx in datatable.index:
            #print(regionIdx + '-')
            if not mapp.regions.exists(regionIdx) and not mapp.countries.exists(regionIdx):
                raise(BaseException('Sorry, region: ' + regionIdx + ' does not exist'))
        
        # check that the time colmns are years
        from pandas.api.types import is_integer_dtype
        if not is_integer_dtype(datatable.columns):
            raise(BaseException('Sorry, year index is not integer'))

        if sum(datatable.index.duplicated()) > 0:
            raise(BaseException('Sorry, region index is unique'))
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
        self.inventory.to_csv(self.INTVENTORY_PATH)
#        self['main'].execute(["git", "add", self.INTVENTORY_PATH])
        self.gitManager.gitAddFile('main',self.INTVENTORY_PATH)

        for sourceID in self.gitManager.updatedRepos:
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
        repoPath = os.path.join(config.PATH_TO_DATASHELF, 'database', remoteName)
        
        self.gitManager.clone_source_from_remote(remoteName, repoPath)
        sourceMetaDict = util.csv_to_dict(os.path.join(repoPath, 'meta.csv'))
        self.sources.loc[remoteName] = pd.Series(sourceMetaDict)   
        self.gitManager.unvalidated_repos[remoteName] = git.Git(repoPath)
        self.sources.loc[remoteName,'git_commit_hash'] = self.gitManager[remoteName].execute(['git', 'rev-parse', 'HEAD'])
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitManager.gitAddFile('main', config.SOURCE_FILE) 
        sourceInventory = pd.read_csv(os.path.join(repoPath, 'source_inventory.csv'), index_col=0)
        for idx in sourceInventory.index:
            self.inventory.loc[idx,:] = sourceInventory.loc[idx,:]
        self._gitCommit('imported ' + remoteName)

    def exportSourceToRemote(self, sourceID):
#        repoPath = os.path.join(config.PATH_TO_DATASHELF, 'database', sourceID)
        
        
        self.gitManager.create_remote_repo(sourceID)
#        sourceInventory = self.inventory.loc[self.inventory.source==sourceID,:]
#        sourceInventory.to_csv(os.path.join(repoPath, 'source_inventory.csv'))
#        self.gitManager.gitAddFile(sourceID, os.path.join(repoPath, 'source_inventory.csv')) 
        
#        self.gitManager.commit('added export inventory')
        self.gitManager.push_to_remote_datashelf(sourceID)
        print('export successful: ({})'.format( config.DATASHELF_REMOTE +  sourceID))
        
#    def updateSourceFromRemote(self, sourceID):
        
#%%
class GitRepository_Manager(dict):
    """
    # Management of git repositories for fast access
    """
    def __init__(self, config):
        self.PATH_TO_DATASHELF = config.PATH_TO_DATASHELF
        self.updatedRepos      = set()
        self.sources   = pd.read_csv(config.SOURCE_FILE, index_col='SOURCE_ID')
        
        self.unvalidated_repos   = dict()
        self.filesToAdd          = dict()
        self.filesToAdd['main']  =list()
        for sourceID in self.sources.index:
            repoPath = os.path.join(self.PATH_TO_DATASHELF,  'database', sourceID)
#            self.filesToAdd[sourceID]  =list()
            self.unvalidated_repos[sourceID] = git.Git(repoPath)
            commitHash = self.unvalidated_repos[sourceID].execute(['git', 'rev-parse', 'HEAD'])
            if commitHash != self.sources.loc[sourceID, 'git_commit_hash']:
                raise(BaseException('Source {} is inconsistend with overall database'.format(sourceID)))
            
    def __getitem__(self, *args, **kwargs):
        """ 
        Overwrites __getitem__ to automatically load git class of a 
        repository and checks for uncommited changes
        """
        sourceID = args[0]
        
        
        if hasattr(self, sourceID):
            return super().__getattribute__(sourceID)
        else:
            if sourceID == 'main':
                object.__setattr__(self,sourceID, git.Git(self.PATH_TO_DATASHELF))
                if super().__getattribute__(sourceID).diff() != '':
                    raise(Exception('Main database repository is inconsistent! - please check uncommitted modifications'))
                
            else:
                object.__setattr__(self,sourceID , self.unvalidated_repos[sourceID])
                if super().__getattribute__(sourceID).diff() != '':
                    raise(Exception('Source repository: "' + sourceID + '" is inconsistent! - please check uncommitted modifications'))
            return super().__getattribute__(sourceID)
        
    def _validateRepository(self, repoName):
        if self[repoName].diff() != '':
            raise(Exception('Git repository: "{}" is inconsistent! - please check uncommitted modifications'.format(repoName)))
        else:
            config.DB_READ_ONLY = False
            if config.DEBUG:
                print('Repo {} is clean'.format(repoName))
            return True
        
    def init_new_repo(self, repoPath, repoID, sourceMetaDict):
        import pathlib as plib

        self.sources.loc[repoID] = pd.Series(sourceMetaDict)
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile('main', config.SOURCE_FILE)

#            self._gitCommit()

        print('creating folder ' + repoPath)
        os.makedirs(repoPath, exist_ok=True)
        git.Repo.init(repoPath)
        self.unvalidated_repos[repoID] = git.Git(repoPath)
        self.filesToAdd[repoID]        = list()
        self[repoID] = git.Git(repoPath)  
        for subFolder in config.SOURCE_SUB_FOLDERS:
            os.makedirs(os.path.join(repoPath, subFolder), exist_ok=True)
            filePath = os.path.join(repoPath, subFolder, '.gitkeep')
            plib.Path(filePath).touch()
            self.gitAddFile(repoID, filePath)
        metaFilePath = os.path.join(repoPath, 'meta.csv')
        util.dict_to_csv(sourceMetaDict, metaFilePath)
        self.gitAddFile(repoID, metaFilePath)
        self.commit('added source: ' + repoID)

       
#                self.gitManager.commit('added source: ' + source_ID)
    def gitAddFile(self, repoName, filePath, addToGit=True):
#        print(filePath)
        if config.DEBUG:
            print('Added file {} to repo: {}'.format(filePath,repoName))
        
        # important to initial the existing repos
        try:
            self.filesToAdd[repoName].append(filePath)
        except:
            self[repoName].refresh()
            self.filesToAdd[repoName] = [filePath]
            
#        self[repoName].execute(["git", "add", filePath])
#        self[repoName].add(filePath)    
        if repoName != 'main':
            self.updatedRepos.add(repoName)
        
    def gitRemoveFile(self, repoName, filePath):
        if config.DEBUG:
            print('Removed file {} to repo: {}'.format(filePath,repoName))
        self[repoName].execute(["git", "rm", filePath])
        if repoName != 'main':
            self.updatedRepos.add(repoName)
    
    def _gitUpdateFile(self, repoName, filePath):
        pass
        
    def commit(self, message):
        
        if 'main' in self.updatedRepos:
            self.updatedRepos.remove('main')
        for repoID in self.updatedRepos:
#            try:
            self[repoID].add(self.filesToAdd[repoID])
            self[repoID].execute(["git", "commit", '-m' "" +  message + " by " + config.CRUNCHER])
            self.sources.loc[repoID,'git_commit_hash'] = self[repoID].execute(['git', 'rev-parse', 'HEAD'])
            self.filesToAdd[repoID]  = list()
#            except:
#                print('Commit of {} repository failed'.format(repoID))  
        
        # commit main repository
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile('main', config.SOURCE_FILE)
        self['main'].add(self.filesToAdd['main'])
        self['main'].execute(["git", "commit", '-m ' " " + message + " by " + config.CRUNCHER])
        self.filesToAdd['main']  = list()
        #reset updated repos to empty
        self.updatedRepos        = set()
        
        
    def create_remote_repo(self, repoName):
        if self[repoName].execute(["git", "remote"]) == 'origin':
            print('remote origin already exists, skip')
            return 
        self[repoName].execute(["git", "remote", "add",  "origin",  config.DATASHELF_REMOTE + repoName + ".git"])
        self[repoName].execute(["git","push", "--set-upstream","origin","master"])
        #self[repoName].execute(["git","push"])
    
    def push_to_remote_datashelf(self, repoName):
        self[repoName].execute(["git","push"])
        
    def clone_source_from_remote(self, repoName, repoPath):
        url = config.DATASHELF_REMOTE +  repoName + '.git'
        git.repo.base.Repo.clone_from(url=url, to_path=repoPath)   
        
    def pull_update_from_remote(self, repoName):
        self[repoName].pull()
        
    def updateGitHash(self, repoName):
        self.sources.loc[repoName,'git_commit_hash'] = self[repoName].execute(['git', 'rev-parse', 'HEAD'])
        self.commit()
        
    def setActive(self, repoName):
        self[repoName].refresh()
        
    def isSource(self, sourceID):
        if sourceID in self.sources.index:
            self[sourceID].refresh()
            return True
        else:
            return False
        