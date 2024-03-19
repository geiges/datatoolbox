#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV and git-based database to store year-country data in a multi-user
setup
"""

import time
tt=time.time()
import os
import git
import tqdm

import copy
import types

from threading import Thread
import traceback
from collections import defaultdict
from pathlib import Path
import shutil

import pandas as pd
import numpy as np

from . import config
from .data_structures import Datatable, TableSet, read_csv
#from . import mapping as mapp
from . import io_tools as io
from . import util
from . import core

from tabulate import tabulate

if config.DEBUG:
    
    print('Database -> Import libary in {:2.4f} seconds'.format(time.time() - tt))

# helper funtions
def restore_source(repoName):
    """
    Restores the data respository based on the last working git hash in the inventory file.
    After a successfulr restorations, datatoolbox need to be imported from scratch. For notebooks
    the restart of the kernal might be required.

    Parameters
    ----------
    repoName : str
        Source ID.

    Returns
    -------
    None.

    """

    
    sources = pd.read_csv(config.SOURCE_FILE, index_col="SOURCE_ID")
    git_hash = sources.git_commit_hash.loc[repoName]
    repoPath = os.path.join(config.PATH_TO_DATASHELF, "database", repoName)
    git_repo = git.Repo(repoPath)
    git_repo.git.execute(["git", "reset", "--hard", git_hash])


def unique(table, field=None):
    #%%
    unique_dict = dict()
    max_len = 0
    if field is None:
        fields = config.ID_FIELDS
    else:
        fields = [field]

    for var in fields:
        unique_dict[var] = table.loc[:, var].unique()
        max_len = max(max_len, len(unique_dict[var]))
    unique_data = pd.DataFrame(index=range(max_len), columns=fields, data="").astype(
        str
    )

    for var in fields:
        len_data = len(unique_dict[var])
        unique_data.loc[unique_data.index[:len_data], var] = unique_dict[var]
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None
    ):  # more options can be specified also
        print(unique_data)


class DataSource:
    def __init__(self, sourceID):

        folder_path = os.path.join(config.PATH_TO_DATASHELF, "data", sourceID)


#%% Database
class Database:
    """
    CSV based database that uses git for as distributed version control system.
    Each table is saved locally as a csv file and identified by a  unique ID.
    The csv files are organized in various sources in individual folders. Each
    sources comes with its own git repository and can be shared with others.
    """

    #%% magicc functions
    def __init__(self):
        """
        Initialized the database and creates an empty one in case the directory
        in the config is empty.
        """
        tt = time.time()
        self.path = config.PATH_TO_DATASHELF

        if not os.path.exists(os.path.join(self.path, "inventory.csv")):
            self.create_empty_datashelf(config.MODULE_PATH, self.path)
            
        
        if config.TEST_ENV:
            # if no config is given, the empty sandbox is populated with some test
            
            tablesToCommit, source_meta = util._create_sandbox_tables(
                "SOURCE_A_2020", 1
            )
            self.commitTables(
                tablesToCommit,
                message="added dummy tablesof source A",
                sourceMetaDict=source_meta,
            )
            tablesToCommit, source_meta = util._create_sandbox_tables(
                "SOURCE_B_2020", 2
            )
            self.commitTables(
                tablesToCommit,
                message="added dummy tables of source B",
                sourceMetaDict=source_meta,
            )
            print("Added test tables to Sandbox datashelf")

        if config.DEBUG:
            print("Database class loaded in {:2.4f} seconds".format(time.time() - tt))

    def __getattribute__(self, name):
        """
        

        Parameters
        ----------
        name : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        
        if (
                (name == 'inventory' and 'inventory' not in self.__dict__) or 
                (name == 'INVENTORY_PATH' and 'INVENTORY_PATH' not in self.__dict__)):
            tt = time.time()
            self.INVENTORY_PATH = os.path.join(self.path, "inventory.csv")
            self.inventory = self._load_inventory(self.INVENTORY_PATH)
            if config.DEBUG:
                print("Database loaded in {:2.4f} seconds".format(time.time() - tt))
        
            
        if (
            (name == 'sources' and 'sources' not in self.__dict__) or 
            (name == 'gitManager' and 'gitManager' not in self.__dict__)
            ):
            
            tt2 = time.time()
            self.gitManager = GitRepository_Manager(config)
            self.sources = self.gitManager.sources
            
            if config.DEBUG:
                print("Git Manager loaded in {:2.4f} seconds".format(time.time() - tt2))

        return super().__getattribute__(name)
        

    #%% private functions

    def _addNewSource(self, sourceMetaDict):
        """
        Private
        Adds new source to the sources table
        """
        source_ID = sourceMetaDict["SOURCE_ID"]

                
        if not self.sourceExists(source_ID):
            
            # check if source is aready online
            if self.gitManager.check_if_online_repo_exists(source_ID):
                raise(Exception(f"Souce with name {source_ID} exists already online. Use dt.importSourceFromRemote() do create a local copy. Review the data and consider adding to the existing dataset"))
            sourcePath = os.path.join(
                config.PATH_TO_DATASHELF, "database", sourceMetaDict["SOURCE_ID"]
            )
            self.gitManager.init_new_repo(sourcePath, source_ID, sourceMetaDict)

        else:
            print("source already exists")

    def _addTable(self, datatable):
        """
        Private
        Pools functionality to add table to the database
        """

        source = datatable.source()
        datatable.meta["creator"] = config.CRUNCHER
        sourcePath = os.path.join("database", source)
        filePath = os.path.join(sourcePath, "tables", datatable.getTableFileName())
        if (config.OS == "win32") | (config.OS == "Windows"):
            filePath = filePath.replace("|", "___")

        if "standard_region" in datatable.columns or "region" in datatable.columns:
            datatable = datatable.reset_index().set_index(["region", "standard_region"])
            datatable.columns = datatable.columns.astype(int)

        datatable = util.cleanDataTable(datatable)
        datatable = datatable.sort_index(axis="index")
        datatable = datatable.sort_index(axis="columns")

        self.isConsistentTable(datatable)

        self._gitAddTable(datatable, source, filePath)

    def _checkTablesOnDisk(self, sourceID=None):
        notExistingTables = list()

        if sourceID is None:
            table_list = self.inventory.index
        else:
            table_list = self.inventory.index[self.inventory.source == sourceID]

        for tableID in self.inventory.index:
            filePath = self._getTableFilePath(tableID)
            if not os.path.exists(filePath):
                notExistingTables.append(tableID)

        return notExistingTables

    def _gitAddTable(self, datatable, source, filePath):
        """
        Private
        Added file to git system
        """
        datatable.to_csv(os.path.join(config.PATH_TO_DATASHELF, filePath))
        ID = datatable.ID
        self.gitManager.gitAddFile(
            source, os.path.join("tables", core.generate_table_file_name(ID))
        )

    def _gitCommit(self, message):
        """
        Private
        Commits all changes to git
        """
        self.inventory.to_csv(self.INVENTORY_PATH)
        self.gitManager.gitAddFile("main", self.INVENTORY_PATH)

        for sourceID in self.gitManager.updatedRepos:
            if sourceID == "main":
                continue
            repoPath = os.path.join(config.PATH_TO_DATASHELF, "database", sourceID)
            sourceInventory = self.inventory.loc[self.inventory.source == sourceID, :]
            sourceInventory.to_csv(os.path.join(repoPath, "source_inventory.csv"))
            self.gitManager.gitAddFile(
                sourceID, os.path.join(repoPath, "source_inventory.csv")
            )
        self.gitManager.commit(message)

    def _getSourceFromID(self, tableID):
        """
        Private
        Returns the source of a given table
        """
        return tableID.split(config.ID_SEPARATOR)[-1]

    def _getTableFilePath(self, ID):
        """
        Private
        Return the file path for a given tableID
        """
        source = self.inventory.loc[ID].source
        fileName = core.generate_table_file_name(ID)
        return os.path.join(
            config.PATH_TO_DATASHELF, "database", source, "tables", fileName
        )

    def _get_source_path(self, sourceID):
        return os.path.join(config.PATH_TO_DATASHELF, "database", sourceID)

    def _load_inventory(self, pathname):
        return pd.read_csv(
            pathname, index_col=0, dtype={"source_year": str},
            engine='pyarrow')

    def _reloadInventory(self):
        """
        Private
        Reloades the inventory from the csv file
        """
        self.inventory = pd.read_csv(
            self.INVENTORY_PATH, index_col=0, dtype={"source_year": str}
        )

    def _removeTable(self, ID):
        """
        Private method
        Function to pool code for removing a table from the database
        """
        source = self.inventory.loc[ID, "source"]
        tablePath = self._getTableFilePath(ID)
        self.remove_from_inventory(ID)
        self.gitManager.gitRemoveFile(source, tablePath)

    def _tableExists(self, ID):
        return ID in self.inventory.index

    def _updateTable(self, oldTableID, newDataTable):
        """
        Private

        Method as a common function form multiple functions
        """
        newDataTable = util.cleanDataTable(newDataTable)

        oldDataTable = self.getTable(oldTableID)
        oldID = oldDataTable.meta["ID"]
        newID = newDataTable.generateTableID()

        if (
            oldID == newID
        ):  # and (oldDataTable.meta['unit'] == newDataTable.meta['unit']):
            # only change data
            self._addTable(newDataTable)
        else:
            # delete old table
            self.removeTable(oldID)

            # add new table
            self._addTable(newDataTable)

            # change inventory
            self.inventory.rename(index={oldID: newID}, inplace=True)
        self.add_to_inventory(newDataTable)

    def _validateRepository(self, repoID="main"):
        """
        Private
        Checks that a sub repository is valid. Valid means that the git repository
        is clean and not outanding commits are there.
        """
        return self.gitManager._validateRepository(repoID)

    #%% public functions
    def available_remote_data_updates(self):
        return self.gitManager.available_remote_data_updates()



    
    def create_empty_datashelf(self, modulePath, pathToDataself):
        """
        Method to create the required files for an empty csv-based data base.
        (Equivalent to the fucntions in admin.py)
        """
        datashelf = Path(pathToDataself)
        datashelf.mkdir(parents=True, exist_ok=True)

        # add subfolders database
        (datashelf / "database").mkdir(exist_ok=True)
        (datashelf / "mappings").mkdir(exist_ok=True)
        (datashelf / "rawdata").mkdir(exist_ok=True)

        # create mappings
        modulePath = Path(modulePath)

        for fn in ("regions.csv", "continent.csv", "country_codes.csv"):
            shutil.copyfile(modulePath / "data" / fn, datashelf / "mappings" / fn)

        # created sources.csv that contains the indivitual information for each source
        sourcesDf = pd.DataFrame(columns=config.SOURCE_META_FIELDS)
        sourcesDf.to_csv(datashelf / "sources.csv")

        # creates inventory.csv that contains all data tables from all sources
        inventoryDf = pd.DataFrame(columns=config.INVENTORY_FIELDS)
        inventoryDf.to_csv(datashelf / "inventory.csv")
        git.Repo.init(datashelf)

    def check_for_new_remote_data (self, update=False):
        # redirecting to gitManager function
        return self.gitManager.check_for_new_remote_data(update)
    
    def info(self):
        """
        Shows the most inmportant information about the status of the database
        """

        print("######## Database informations: #############")
        print("Your database is located at: " + self.path)
        print("Number of tables: {}".format(len(self.inventory)))
        print("Number of data sources: {}".format(len(self.gitManager.sources)))
        print(
            "Number of commits: {}".format(
                self.gitManager["main"].git.rev_list("--all", "--count")
            )
        )
        print("#############################################")

    def remote_sourceInfo(self):
        """
        Returns a list of available sources and meta data
        """
        from tabulate import tabulate

        remote_repo_path = os.path.join(
            config.PATH_TO_DATASHELF, "remote_sources", "source_states.csv"
        )

        if not os.path.exists(remote_repo_path):
            print("No information about remote data available")
            print("Consider run again with update = True")

        else:
            remote_sources_df = pd.read_csv(remote_repo_path, index_col=0)

        df_print = self.sources.loc[
            :,
            [
                "tag",
            ],
        ]
        df_print["remote_tag"] = None
        for sourceID in remote_sources_df.index:

            if sourceID not in self.sources.index:
                continue
            remote_tag = remote_sources_df.loc[sourceID, "tag"]
            local_tag = self.sources.loc[sourceID, "tag"]
            df_print.loc[sourceID, "remote_tag"] = remote_tag

        print(tabulate(remote_sources_df, headers="keys", tablefmt="psql"))
        return remote_sources_df

    def sourceInfo(self, show_number_of_table=False):
        """
        Returns a list of available sources and meta data
        """
        if show_number_of_table:
            inv = self.sources.sort_index().copy()
            
            n_tables = [len(self.findp(source=x).index) for x in inv.index]
            inv.loc[:, 'size'] = n_tables
            return inv
        else:
                
            return self.sources.sort_index()

    def returnInventory(self):
        """
        Returns a copy of the data base inventory.
        """
        return copy.copy(self.inventory)

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
        self.inventory.loc[datatable.ID] = [
            datatable.meta.get(x, None) for x in config.INVENTORY_FIELDS
        ]
        
    def remove_from_inventory(self, tableID):
        """
        Method to remove a table from the global inventory
        Input: tableID
        """
        self.inventory.drop(tableID, inplace=True)

    def findc(self, **kwargs):
        """
        Method to search through the inventory. kwargs can be all inventory entires
        (see config.INVENTORY_FIELDS).
        """

        # loop over all keys of kwargs to filter based on all of them
        mask = True
        for key, val in kwargs.items():
            mask &= self.inventory[key].str.contains(val, regex=False, na=False)

        table = (self.inventory if mask is True else self.inventory.loc[mask]).copy()

        # test to add function to a instance (does not require class)
        table.graph = types.MethodType(util.plot_query_as_graph, table)
        table.short = types.MethodType(util.shorten_find_output, table)
        table.unique = types.MethodType(unique, table)
        table.to_pyam = types.MethodType(util.to_pyam, table)
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
        from .tools import xarray
        # filter by columns and list of values
        keep = True

        for field, pattern in filters.items():
            # treat `col=None` as no filter applied
            if pattern is None:
                continue

            if field not in self.inventory:
                raise ValueError(f"filter by `{field}` not supported")

            keep &= util.pattern_match(self.inventory[field], pattern, regex=regex)

        if level is not None:
            keep &= self.inventory["variable"].str.count(r"\|") == level

        table = pd.DataFrame(
            self.inventory if keep is True else self.inventory.loc[keep]
        )

        # test to add function to a instance (does not require class)
        table.graph = types.MethodType(util.plot_query_as_graph, table)
        table.short = types.MethodType(util.shorten_find_output, table)
        table.unique = types.MethodType(unique, table)

        table.filterp = types.MethodType(util.filterp, table)
        table.load = types.MethodType(self.getTables, table.index)
        table.as_pyam = types.MethodType(util.to_pyam, table)
        table.as_xarray = types.MethodType(xarray.load_as_xdataset, table)
        table.as_wide_dataframe = types.MethodType(self.query_to_long_table, table)
        return table

    def finde(self, **kwargs):
        """
        Finds an exact match for the given filter criteria.
        """
        table = self.inventory.copy()
        for key in kwargs.keys():
            mask = self.inventory[key] == kwargs[key]
            mask[pd.isna(mask)] = False
            mask = mask.astype(bool)
            table = table.loc[mask].copy()

        # test to add function to a instance (does not require class)
        table.graph = types.MethodType(util.plot_query_as_graph, table)
        table.short = types.MethodType(util.shorten_find_output, table)
        table.to_pyam = types.MethodType(util.to_pyam, table)
        return table

    def getTable(self, ID, native_regions=False):
        """
        Method to load the datatable for the given tableID.

        Input
        -----
        tableID : str
        native_regions : bool, optional
            Load native region defintions if available. The default is False.

        Returns
        table : Datatable
        """
        if config.logTables:
            core.LOG["tableIDs"].append(ID)

        if self._tableExists(ID):
            # load table from database

            filePath = self._getTableFilePath(ID)

            table = read_csv(filePath, native_regions)
            return table

        elif os.path.exists("data"):
            fileName = core.generate_table_file_name(ID)
            filePath = os.path.join("data", fileName)

            if os.path.exists(filePath):
                table = read_csv(filePath, native_regions)
               
                return table

        if config.AUTOLOAD_SOURCES:
            # trying to import sources from remote on demand
            source = self._getSourceFromID(ID)

            if source in core.DB.sources.index:

                raise (BaseException("Table {} not found".format(ID)))

            else:
                try:
                    if config.DEBUG:
                        print("Trying to import source {}".format(source))
                    self.importSourceFromRemote(source)
    
                    print(
                        """###########################################################
                             Successfully imported source: {}
                             ###########################################################""".format(
                            source
                        )
                    )
    
                    if self._tableExists(ID):
                        # load table from database
    
                        filePath = self._getTableFilePath(ID)
                        table = read_csv(filePath, native_regions).drop_duplicates()
                        table = table[table.index.notnull()]
                        return table
                except Exception:
    
                    if config.DEBUG:
                        print(traceback.format_exc())
                        print("Failed to import source {}".format(source))

        raise (BaseException("Table {} not found".format(ID)))

    def getTables(self, iterIDs, native_regions=False, disable_progress=None):
        """
        Method to return multiple datatables at once as a dictionary like
        set fo tables.

        Input
        -----
        iterIDS: list [str]
            List of IDs to load.
        native_regions : bool, optional
            Load native region defintions if available. The default is False.
        disable_progress : bool, optional
            Disable displaying of progressbar. The default None hides the
            progressbar on non-tty outputs

        Returns
        -------
        tables : TableSet
        """

        if config.logTables:
            IDs = list()
        res = TableSet()
        for ID in tqdm.tqdm(iterIDs, disable=disable_progress):
            table = self.getTable(ID, native_regions)
            if config.logTables:
                IDs.append(ID)
            res.add(table)
        if config.logTables:
            core.LOG["tableIDs"].extend(IDs)
        return res

    def getTablesAvailable(self):
        """
        Return a locally stored pandas dataframe of tables
        on datashelf
        """
        return pd.read_csv(
            os.path.join(config.MODULE_DATA_PATH, "datashelf_contents.csv")
        )

    def get_remote_summary(self):
        remote_repo_path = os.path.join(
            config.PATH_TO_DATASHELF, "remote_sources", "source_states.csv"
        )

        remote_sources_df = pd.read_csv(remote_repo_path, index_col=0)
        return remote_sources_df

    def startLogTables(self):
        """
        Starts the logging of loaded datatables. This is useful to collect all
        required tables for a given analysis to create a datapackage for off-line
        useage
        """
        config.logTables = True
        core.LOG["tableIDs"] = list()

    def stopLogTables(self):
        """
        Stops the logging process of datatables and return the list of loaded
        table IDs for more processing.
        """
        import copy

        config.logTables = False
        outList = copy.copy(core.LOG["tableIDs"])
        # core.LOG.TableList = list()
        return outList

    def clearLogTables(self):
        """
        Clears the list of logged tables. This is anyway done if the package is
        newly loaded
        """
        core.LOG["tableIDs"] = list()

    def save_logged_tables(self, folder="data"):
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
        # create folder if required
        if not os.path.exists(folder):
            os.mkdir(folder)
        # save tables to disk
        self.saveTablesToDisk(folder, core.LOG["tableIDs"])
        if config.DEBUG:
            print(
                "{} tables stored to directory {}".format(
                    len(core.LOG["tableIDs"]), folder
                )
            )

    def saveTablesToDisk(self, folder, IDList):
        """
        Function to save a list of tables to disk as csv files.
        """
        from shutil import copyfile
        import os

        for ID in IDList:
            pathToFile = self._getTableFilePath(ID)
            print()
            copyfile(pathToFile, folder + "/" + os.path.basename(pathToFile))

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
        sourceID = dataTable.meta["source"]
        if not self.isSource(sourceID):
            if sourceMetaDict is None:
                raise (
                    BaseException("Source does not exist and no sourceMeta provided")
                )
            else:
                if not (sourceMetaDict["SOURCE_ID"] in self.gitManager.sources.index):
                    self._addNewSource(sourceMetaDict)

        dataTable = util.cleanDataTable(dataTable)
        self._addTable(dataTable)
        self.add_to_inventory(dataTable)

        self._gitCommit(message)

    def commitTables(
        self,
        dataTables,
        message,
        sourceMetaDict=None,
        append_data=False,
        update=False,
        overwrite=False,
        cleanTables=True,
    ):
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
        overwrite : [optional] : bool to choose if data is overwriten (new data
                                 overwrites old data)
        cleanTables [optional] : bool (default: true) to choose if tables are
                                 cleaned before commit

        TODO: Check flags
        """
        # create a new source if not extisting
        if (sourceMetaDict is not None) and (
            not self.isSource(sourceMetaDict["SOURCE_ID"])
        ):
            self._addNewSource(sourceMetaDict)

        # only test if an table is update if the source did exist
        if update:
            oldTableIDs = [table.generateTableID() for table in dataTables]
            self.updateTables(oldTableIDs, dataTables, message)
            return

        else:
            for dataTable in tqdm.tqdm(dataTables):

                if config.DEBUG:
                    print(dataTable.ID)
                if cleanTables:
                    dataTable = util.cleanDataTable(dataTable)

                if dataTable.isnull().all().all():
                    print("ommiting empty table: " + dataTable.ID)
                    continue

                if dataTable.ID not in self.inventory.index:
                    # add entire new table

                    self._addTable(dataTable)
                    self.add_to_inventory(dataTable)
                elif overwrite:
                    self._addTable(dataTable)
                elif append_data:
                    # append data to table
                    oldTable = self.getTable(dataTable.ID, native_regions=True)
                    mergedTable = oldTable.combine_first(dataTable)
                    mergedTable = Datatable(mergedTable, meta=dataTable.meta)
                    self._addTable(mergedTable)

        self._gitCommit(message)

    def update_mapping_file(self, sourceID, mapping_file_path, sourceMetaDict=None):
        """
        adds mapping file to database

        Parameters
        ----------
        source : TYPE
            DESCRIPTION.
        ID : TYPE
            DESCRIPTION.
        mapping : TYPE
            DESCRIPTION.
        """
        if not core.DB.isSource(sourceMetaDict["SOURCE_ID"]):
            if sourceMetaDict is None:
                raise (BaseException("Source meta can not be None"))
            core.DB._addNewSource(sourceMetaDict)

        source_path = self._get_source_path(sourceID)
        # for key in mapping.keys():
        dest_file_path = os.path.join(source_path, "mapping.xlsx")
        shutil.copyfile(mapping_file_path, dest_file_path)
        # mapping[key].to_csv(file_path)

        self.gitManager.gitAddFile(sourceID, dest_file_path)

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
            raise (BaseException("source  does not exist"))

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
                raise (BaseException("source  does not exist"))

        # loop over tables
        for oldTableID, newDataTable in tqdm.tqdm(
            zip(oldTableIDs, newDataTables), desc="Updating tables"
        ):

            if oldTableID in self.inventory.index:
                self._updateTable(oldTableID, newDataTable)

            else:
                dataTable = util.cleanDataTable(newDataTable)
                self._addTable(dataTable)
                self.add_to_inventory(dataTable)
        self._gitCommit(message)

    def updateTablesAvailable(self, private_access_token):
        """
        Method to update module data folder with the latest
        datashelf contents on Gitlab

        Requirements:

            - private_access_token: generated on Gitlab

        """

        try:
            import gitlab
        except ImportError:
            print("python-gitlab not available: Install with mamba or conda")
            raise

        gl = gitlab.Gitlab(private_token=private_access_token)
        # define group
        group = gl.groups.get("climateanalytics/datashelf")
        projects = group.projects.list(include_subgroups=True, all=True)
        print("fetching datashelf contents...")
        # init list
        rows = []
        for project in projects:
            # append
            rows.append(
                {
                    "table": project.name,
                    "last_update": pd.to_datetime(project.last_activity_at),
                    "description": project.description,
                    #'fetched'        : pd.to_datetime('today')
                }
            )
        # make df
        tables = pd.DataFrame(rows)
        # change datetime
        tables["last_update"] = tables["last_update"].dt.strftime("%d-%m-%Y")
        
        tables.to_csv(
            os.path.join(config.MODULE_DATA_PATH, "datashelf_contents.csv"), index=False
        )
        print("done")

    def validate_ID(self, ID, print_statement=True):
        """
        Method to chekc the validity of a table ID and check the state of the
        data
        """
        RED = "\033[31m"
        GREEN = "\033[32m"
        BLACK = "\033[30m"
        source = ID.split(config.ID_SEPARATOR)[-1]
        print("TableID: {}".format(ID))
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

        fileName = core.generate_table_file_name(ID)
        tablePath = os.path.join(
            config.PATH_TO_DATASHELF, "database", source, "tables", fileName
        )

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

    def removeTables_of_source(self, sourceID, IDList):

        if not self.isSource(sourceID):
            raise (BaseException("source  does not exist"))

        files_on_disk = list()
        for tableID in IDList:
            filePath = core.DB._getTableFilePath(tableID)
            if os.path.exists(filePath):
                files_on_disk.append(tableID)

        files = ["tables/" + core.generate_table_file_name(x) for x in files_on_disk]

        self.gitManager.gitRemoveFiles(sourceID, files)

        self.remove_from_inventory(IDList)

        self._gitCommit(f"{len(IDList)} tables of {sourceID} removed")

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
                raise (BaseException("source  does not exist"))

        for ID in tqdm.tqdm(IDList, desc="Removing tables"):

            source = self.inventory.loc[ID, "source"]
            tablePath = self._getTableFilePath(ID)
            self.gitManager.gitRemoveFile(source, tablePath)

        self.remove_from_inventory(IDList)

        self._gitCommit("Tables removed")

    def removeTable(self, tableID):
        """
        Method to remnove tables from the database

        Input
        -----
        tableID : str
        """
        sourceID = self._getSourceFromID(tableID)
        if not self.isSource(sourceID):
            raise (BaseException("source  does not exist"))

        self._removeTable(tableID)
        self._gitCommit("Table removed")
        self._reloadInventory()

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

            raise (
                BaseException(
                    "Sorry, data of table {} is needed to be numeric".format(datatable)
                )
            )

        # check that spatial index is consistend with defined countries or regions
        # invalidSpatialIDs = datatable.index.difference(mapp.getValidSpatialIDs())
        # if len(invalidSpatialIDs) > 0:
        #     raise(BaseException('Sorry, regions in table {}: {} do not exist'.format(datatable, invalidSpatialIDs)))

        # check that the time colmns are years
        from pandas.api.types import is_integer_dtype

        if not is_integer_dtype(datatable.columns):
            raise (
                BaseException(
                    "Sorry, year index in table {} is not integer".format(datatable)
                )
            )

        if sum(datatable.index.duplicated()) > 0:
            print(datatable.meta)
            raise (
                BaseException(
                    "Sorry, region index in table {} is not  unique".format(datatable)
                )
            )
        return True

    def removeSource(self, sourceID):
        """
        Function to remove an entire source from the database.
        """

        if self.sourceExists(sourceID):
            sourcePath = os.path.join(config.PATH_TO_DATASHELF, "database", sourceID)
            shutil.rmtree(sourcePath, ignore_errors=False, onerror=None)
        self.gitManager.sources = self.gitManager.sources.drop(sourceID, axis=0)
        self.sources = self.gitManager.sources
        self.inventory = self.inventory.loc[self.inventory.source != sourceID]
        #        self.gitManager.updatedRepos.add('main')
        self._gitCommit(sourceID + " deleted")


    def test_ssh_remote_connection(self):
        return self.gitManager.test_ssh_remote_connection()
        
    
    def updateExcelInput(self, fileName):
        """
        This function updates all data values that are defined in the input sheet
        in the given excel file
        """
        if config.DB_READ_ONLY:
            assert self._validateRepository()
        ins = io.Inserter(fileName="demo.xlsx")
        for setup in ins.getSetups():
            dataTable = self.getTable(setup["dataID"])
            ins._writeData(setup, dataTable)

    def importSourceFromRemote(self, remoteName):
        """
        This functions imports (git clone) a remote dataset and creates a local
        copy of it.

        Input is an existing sourceID.
        """
        repoPath = os.path.join(config.PATH_TO_DATASHELF, "database", remoteName)

        self.gitManager.clone_source_from_remote(remoteName, repoPath)

        sourceInventory = pd.read_csv(
            os.path.join(repoPath, "source_inventory.csv"),
            index_col=0,
            dtype={"source_year": str},
        )
        self.inventory = pd.concat([self.inventory, sourceInventory], verify_integrity=True)
        self._gitCommit("imported " + remoteName)

    def exportSourceToRemote(self, sourceID):
        """
        This function exports a new local dataset to the defind remote database.

        Input is a local sourceID as a str.
        """
        self.gitManager.create_remote_repo(sourceID)
        self.gitManager.push_to_remote_datashelf(sourceID)
        print("export successful: ({})".format(config.DATASHELF_REMOTE + sourceID))

    def checkout_source_version(self, sourceID, tag="latest"):

        # set source to tag
        hash = self.gitManager.checkout_git_version(sourceID, tag)

        # update sources.csv
        self.gitManager.updateGitHash_and_Tag(sourceID)

        # update inventory
        tables_to_remove = self.inventory.index[self.inventory.source == sourceID]
        self.inventory = self.inventory.drop(tables_to_remove)

        inv_file = self.gitManager.get_inventory_file_of_source(sourceID)
        source_inventory = self._load_inventory(inv_file)
        self.inventory = pd.concat([self.inventory, source_inventory])
        # commit all
        self._gitCommit(f"Set source {sourceID} to tag {tag}")


    def push_source_to_remote(self, repoName, force=True):
        return self.gitManager.push_to_remote_datashelf(repoName, force)
   
    def pull_update_from_remote(self, repoName):
        """
        Updates the local data repository by the newest version on the remote repository


        Parameters
        ----------
        repoName : str
            Source ID string to identify which source repository should be updated.

        Returns
        -------
        None.

        """
        new_inventory = self.gitManager.pull_update_from_remote(
            repoName, self.inventory
        )
        self.inventory = new_inventory

        self._gitCommit("udpate from remote")

    def query_to_long_table(
        self,
        query,
        native_regions=False,
        meta_list=["variable", "region", "scenario", "model", "source", "unit"],
    ):

        tables = [self.getTable(x, native_regions=native_regions) for x in query.index]
        final_tables = list()
        for table in tables:
            if table.empty:
                continue

            
            inp_dict = dict()
            for metaKey in meta_list:
                if metaKey == "region":
                    inp_dict[metaKey] = table.index
                else:
                    inp_dict[metaKey] = table.meta.get(metaKey, "")

            try:
                final_tables.append(
                    pd.DataFrame(table.assign(**inp_dict)).reset_index(drop=True)
                )
            except KeyError as exc:
                raise AssertionError(
                    f"meta of {table.ID} does not contain {exc.args[0]}"
                )
        long_df = pd.concat(final_tables, ignore_index=True, sort=False)

        long_df = long_df.set_index(meta_list)
        return long_df


#%% Git Repository Manager
class GitRepository_Manager:
    """
    # Management of git repositories for fast access
    """

    #%% Magicc methods
    def __init__(self, config, debugmode=False):
        self.PATH_TO_DATASHELF = config.PATH_TO_DATASHELF
        self.sources = pd.read_csv(config.SOURCE_FILE, index_col="SOURCE_ID")

        remote_repo_path = os.path.join(
            config.PATH_TO_DATASHELF, "remote_sources", "source_states.csv"
        )
        if os.path.exists(remote_repo_path):
            self.remote_sources = pd.read_csv(remote_repo_path, index_col=0)
            
            
            new_items, updated_items = self._get_difference_to_remote()
            n_new_entries = len(new_items)
            n_updated_sources = len(updated_items)
            if n_new_entries + n_updated_sources  > 0:
                print('Remote: ',end='')
                if n_new_entries >0:
                   print(f'({n_new_entries}) new sources', end='')
                    
                if n_updated_sources > 0:
                    print(f' and ({len(updated_items)}) updated sources', end='')
                print(' are available online (see dt.available_remote_data_updates)')
                
        else:
            print('Remote: not setup')
        
        self.repositories = dict()
        self.updatedRepos = set()
        self.validatedRepos = set()
        self.filesToAdd = defaultdict(list)
        
        # remote update checks (only once per day)
        self._init_remote_repo()
        
        
        if not debugmode:
            for sourceID in self.sources.index:
                repoPath = os.path.join(self.PATH_TO_DATASHELF, "database", sourceID)
                self.repositories[sourceID] = git.Repo(repoPath)
                self.verifyGitHash(sourceID)

            self.repositories["main"] = git.Repo(self.PATH_TO_DATASHELF)
            self._validateRepository("main")
        else:
            print("Git manager initialized in debugmode")

        self.check_for_new_remote_data()
        
    def __getitem__(self, sourceID):
        """
        Retrieve `sourceID` from repositories dictionary and ensure cleanliness
        """
        repo = self.repositories[sourceID]
        if sourceID not in self.validatedRepos:
            self._validateRepository(sourceID)
        return repo

    #%% Private methods
    
    def _ssh_agent_running(self):
        import subprocess

        proc = subprocess.Popen(["ssh-add -l"], stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        return not out.startswith(b'The agent has no identities')

    def _get_difference_to_remote(self):
        
        new_items = self.remote_sources.index.difference(
            self.sources.index
        )
        compare_df = self.sources.copy()
        compare_df['remote_tag'] = self.remote_sources['tag']
        compare_df = compare_df[(~compare_df.tag.isnull()) &(~compare_df.remote_tag.isnull())]
        
        updated_items = compare_df.index[(
            compare_df.tag.apply(lambda x : float(x[1:])) < compare_df.remote_tag.apply(lambda x : float(x[1:]))
            )]
        
        
        
        return new_items, updated_items
    
    def _check_online_data(self):
        curr_date = pd.to_datetime(core.get_time_string()).date()
        last_access_date = pd.to_datetime(
            self._get_last_remote_access()
        ).date()

        return pd.isna(last_access_date) or curr_date > last_access_date
  
    def _init_remote_repo(self):   
        remote_repo_path = os.path.join(config.PATH_TO_DATASHELF, "remote_sources")
        if os.path.exists(remote_repo_path):
            self.remote_repo = self._get_remote_sources_repo()
            
            dpath = os.path.join(
            config.PATH_TO_DATASHELF,
            "remote_sources",
            "source_states.csv",
                )
            self.remote_sources = pd.read_csv(dpath, index_col=0)
        else:
            #create empty dummy
            self.remote_sources = pd.DataFrame()
            
    def _get_remote_sources_repo(self):
        remote_repo_path = os.path.join(config.PATH_TO_DATASHELF, "remote_sources")
        remote_repo = git.Repo(remote_repo_path)

        return remote_repo

    def _pull_remote_sources(self):

        remote_repo_path = os.path.join(config.PATH_TO_DATASHELF, "remote_sources")
        if os.path.exists(remote_repo_path):
            # pull
            remote_repo_path = os.path.join(config.PATH_TO_DATASHELF, "remote_sources")
            remote_repo = git.Repo(remote_repo_path)
            remote_repo.remote("origin").pull(progress=TqdmProgressPrinter())

        else:
            # clone
            remote_repo = self._clone_remote_sources()

        self._update_last_remote_access()
        self.remote_repo = remote_repo

        return remote_repo

    def _clone_remote_sources(self):

        url = config.DATASHELF_REMOTE + "remote_sources.git"
        remote_repo = git.Repo.clone_from(
            url=url,
            to_path=os.path.join(config.PATH_TO_DATASHELF, "remote_sources"),
            progress=TqdmProgressPrinter(),
        )
        self._update_last_remote_access()

        return remote_repo

    def _get_last_remote_access(self):
        filepath = os.path.join(
            config.PATH_TO_DATASHELF, "remote_sources", "last_accessed_remote"
        )
        if not os.path.exists(filepath):
            return np.nan
        with open(filepath, "r") as f:
            last_accessed = f.read()
        return last_accessed

    def _update_last_remote_access(self):

        filepath = os.path.join(
            config.PATH_TO_DATASHELF, "remote_sources", "last_accessed_remote"
        )
        with open(filepath, "w") as f:
            f.write(core.get_time_string())

    def _update_local_sources_tag(self, repoName):
        
        repo = self.repositories[repoName]
        tag = self.get_tag_of_source(repoName)
        self.sources.loc[repoName, "tag"] = tag
        self.commit("Update tags of sources")

    def _update_remote_sources(self, repoName):

        dpath = os.path.join(
            config.PATH_TO_DATASHELF,
            "remote_sources",
            "source_states.csv",
        )
        remote_repo = git.Repo(os.path.join(config.PATH_TO_DATASHELF, "remote_sources"))
        rem_sources_df = pd.read_csv(dpath, index_col=0)

        repo = self[repoName]
        hash = repo.commit().hexsha
        user = config.CRUNCHER

        if len(repo.tags) == 0:
            # start new tag with version 1.0
            tag = "v1.0"

        else:
            tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
            last_tag = tags[-1]
            tag_hash = last_tag.commit.hexsha

            if tag_hash == hash:
                # no new commits -> keep tag
                tag = last_tag.name

                if rem_sources_df.loc[repoName, "tag"] == tag:
                    # nothing needs to be done
                    return repo
            else:
                # there are new commits -> increase version by 1.0

                latest_tag = tags[-1]
                tag = f'v{float(latest_tag.name.replace("v",""))+1:1.1f}'

        # update remote sources csv
        repo.create_tag(tag)
        
        rem_sources_df.loc[repoName, :] = (hash, tag, user)
        rem_sources_df.to_csv(dpath)
        
        remote_repo.index.add("source_states.csv")
        remote_repo.index.commit("remote source update" + " by " + config.CRUNCHER)
        
        self.remote_sources = rem_sources_df
        return repo

    def _gitUpdateFile(self, repoName, filePath):
        pass

    def _validateRepository(self, sourceID):
        """
        Private
        Checks if sourceID points to a valid repository

        """
        repo = self.repositories[sourceID]

        if sourceID != "main":
            self.verifyGitHash(sourceID)

        if repo.is_dirty():
            raise RuntimeError(
                'Git repo: "{}" is inconsistent! - please check uncommitted modifications'.format(
                    sourceID
                )
            )

        config.DB_READ_ONLY = False
        if config.DEBUG:
            print("Repo {} is clean".format(sourceID))
        self.validatedRepos.add(sourceID)
        return True

    #%% Public methods
    
    
    def available_remote_data_updates(self):
        
        new_items, updated_items = self._get_difference_to_remote()
        
        print('New items:')
        print(tabulate(
            self.remote_sources.loc[new_items, ['tag', 'last_to_update']], 
            headers="keys", tablefmt="psql"))
        
        print('Sources with newer data:')
        
        df = pd.concat(
            [
                self.sources.loc[updated_items, ['tag']].rename(columns={'tag':'local_tag'}),
                self.remote_sources.loc[updated_items, ['tag']].rename(columns={'tag':'remote_tag'}),
            ], axis=1)
        print(tabulate(
            df, 
            headers="keys", tablefmt="psql"))
    
        
    def check_if_online_repo_exists(self, sourceID):
        """
        Check if source in local inventory

        Parameters
        ----------
        sourceID : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        return sourceID in self.remote_sources.index
    
    def check_for_new_remote_data(self, force_check=False, foreground=False):
        """
        Checks if source is in online repositorty

        Parameters
        ----------
        force_check : TYPE, optional
            DESCRIPTION. The default is False.
        foreground : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        None.

        """
        if (force_check or self._check_online_data()) and not ("PYTEST_CURRENT_TEST" in os.environ):
                
            # check for remote data
            try:
                if foreground:
                    print("Looking for new online sources...", end='')
                    self._pull_remote_sources()
                    print("Done!")    
                else:
                    if self._ssh_agent_running():
                        print("Looking for new online sources in the backgound")
                        thread = Thread(target=self._pull_remote_sources)
                        thread.start()
                    else:
                        print('SSH agent not running, not checking for remote data.')
                
            except:
                print("Could not check online data repository")
                import traceback

                traceback.print_exc()
         
    def clone_source_from_remote(self, repoName, repoPath):
        """
        Function to clone a remote git repository as a local copy.

        Input
        -----
        repoName : str - valid repository in the remove database
        repoPath : str - path of the repository
        """

        self._pull_remote_sources()
        try:
            print("Try cloning source via ssh...", end='')
            url = config.DATASHELF_REMOTE + repoName + ".git"
            repo = git.Repo.clone_from(
                url=url, to_path=repoPath, progress=TqdmProgressPrinter()
            )
        except:
            print('failed.')
            try:
                
                print("Try Cloning source via https...", end='')
                url = config.DATASHELF_REMOTE_HTTPS + repoName + ".git"
                repo = git.Repo.clone_from(
                    url=url, to_path=repoPath, progress=TqdmProgressPrinter()
                )
            except Exception:
                print('failed.')
                if config.DEBUG:
                    print(traceback.format_exc())
                    print("Failed to import source {}".format(repoName))
                raise(Exception(f"""Both SSH and HTTPs import failed. Check your connection, password or if requrested data exists on remote.
                Consider the following options:                
                    1) Does "{repoName}" exists in {config.DATASHELF_REMOTE_HTTPS}
                    2) Check your ssh connection with: dt.test_ssh_remote_connection())
                    """))
        self.repositories[repoName] = repo

        # Update source file
        sourceMetaDict = util.csv_to_dict(os.path.join(repoPath, "meta.csv"))
        sourceMetaDict["git_commit_hash"] = repo.commit().hexsha
        tag = self.get_tag_of_source(repoName)
        sourceMetaDict["tag"] = tag
        self.sources.loc[repoName] = pd.Series(sourceMetaDict)
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile("main", config.SOURCE_FILE)

        return repo
    

    def get_source_repo_failsave(self, sourceID):
        """
        Retrieve `sourceID` from repositories dictionary without checks
        """
        repoPath = os.path.join(self.PATH_TO_DATASHELF, "database", sourceID)
        repo = git.Repo(repoPath)
        return repo

    def get_inventory_file_of_source(self, repoName):
        repo = self[repoName]
        return os.path.join(repo.working_dir, "source_inventory.csv")

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
        self.gitAddFile("main", config.SOURCE_FILE)

        repoPath = Path(repoPath)
        print(f"creating folder {repoPath}")
        repoPath.mkdir(parents=True, exist_ok=True)
        self.repositories[repoID] = git.Repo.init(repoPath)

        for subFolder in config.SOURCE_SUB_FOLDERS:
            subPath = repoPath / subFolder
            subPath.mkdir(exist_ok=True)
            filePath = subPath / ".gitkeep"
            filePath.touch()
            self.gitAddFile(repoID, filePath)

        metaFilePath = repoPath / "meta.csv"
        util.dict_to_csv(sourceMetaDict, metaFilePath)
        self.gitAddFile(repoID, metaFilePath)

        self.commit("added source: " + repoID)

    def gitAddFile(self, repoName, filePath):
        """
        Addes a new file to a repository

        Input
        -----
        repoName : str
        filePath : str of the relative file path
        """
        if config.DEBUG:
            print("Added file {} to repo: {}".format(filePath, repoName))

        self.filesToAdd[repoName].append(str(filePath))
        self.updatedRepos.add(repoName)

    def gitRemoveFiles(self, repoName, filePaths):
        """
        Removes mutiple file from the git repository

        Input
        -----
        repoName : str
        filePath : str of the relative file path
        """
        self[repoName].index.remove(filePaths, working_tree=True)
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
            print("Removed file {} to repo: {}".format(filePath, repoName))
        self[repoName].git.execute(
            ["git", "rm", "-f", filePath]
        )  # TODO Only works with -f forced, but why?
        self.updatedRepos.add(repoName)

    def commit(self, message):
        """
        Function to commit all oustanding changes to git. All repos including
        'main' are commited if there is any change

        Input
        ----
        message : str - commit message
        """
        if "main" in self.updatedRepos:
            self.updatedRepos.remove("main")

        for repoID in self.updatedRepos:
            repo = self.repositories[repoID]
            repo.index.add(self.filesToAdd[repoID])
            commit = repo.index.commit(message + " by " + config.CRUNCHER)
            self.sources.loc[repoID, "git_commit_hash"] = commit.hexsha
            tag = self.get_tag_of_source(repoID)
            self.sources.loc[repoID, "tag"] = tag
            del self.filesToAdd[repoID]

        # commit main repository
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile("main", config.SOURCE_FILE)

        main_repo = self["main"]
        main_repo.index.add(self.filesToAdd["main"])
        main_repo.index.commit(message + " by " + config.CRUNCHER)
        del self.filesToAdd["main"]

        # reset updated repos to empty
        self.updatedRepos = set()

    def create_remote_repo(self, repoName):
        """
        Function to create a remote git repository from an existing local repo
        """
        repo = self[repoName]

        if "origin" in repo.remotes:
            # remote origin has been configured already, but re-push anyway, since this
            # could have been a connectivity issue
            origin = repo.remotes.origin
        else:
            origin = repo.create_remote(
                "origin", config.DATASHELF_REMOTE + repoName + ".git"
            )

        branch = repo.active_branch
        origin.push(branch, progress=TqdmProgressPrinter())

        # Update references on remote
        origin.fetch()
        branch.set_tracking_branch(origin.refs[0])

    def get_hash_of_source(self, repoName):
        repo = self[repoName]
        return repo.head.commit.hexsha

    def get_tag_of_source(self, repoName):
        repo = self.get_source_repo_failsave(repoName)
        if len(repo.tags) == 0:
            return None

        # tag of head
        return next(
            (tag.name for tag in repo.tags if tag.commit == repo.head.commit), None
        )
        # latest tag
        tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
        return tags[-1].name

    def checkout_git_version(self, repoName, tag):

        repo = self[repoName]

        if tag == "latest":
            # hash = repo.commit().hexsha
            tag = "master"
        elif tag in repo.tags:
            hash = repo.tags["v3.0"].commit.hexsha
        else:
            raise (Exception(f"Tag {tag} does not exist"))

        repo.git.checkout(tag)
        return repo.commit().hexsha

    def push_to_remote_datashelf(self, repoName, force=True):
        """
        This function used git push to update the remote database with an updated
        source dataset.

        Input is the source ID as a str.

        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO

        """
        remote_repo = self._pull_remote_sources()
        repo = self[repoName]
        if (not force) and (
            "Your branch is up to date with 'origin/master'"
            in repo.git.execute(["git", "status"])
        ):
            print("Nothing to push")
            print(repo.git.execute(["git", "status"]))
            return

        self._update_remote_sources(repoName)
        self._update_local_sources_tag(repoName)

        remote_repo.remotes.origin.push(progress=TqdmProgressPrinter())

        self[repoName].remotes.origin.push(progress=TqdmProgressPrinter())

        self[repoName].remotes.origin.push(progress=TqdmProgressPrinter(), tags=True)

    def test_ssh_remote_connection(self):
        """
        Function to test the ssh connection to the remote data repository using
        'ssh -T host'
        Returns
        -------
        None.

        """
        host = config.DATASHELF_REMOTE.split(':')[0]
        print(f'Testing connection to host {host}')
        cmd = f"ssh -T {host}"
        import subprocess
        retcode = subprocess.call(cmd,shell=True)
        if retcode==0:
            print('Successfully connected')
        else:
            print(f'Connection failed with exit code {retcode}')
    

    def pull_update_from_remote(self, repoName, old_inventory):
        """
        This function used git pull an updated remote source dataset to the local
        database.

        Input is the source ID as a str.

        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO

        """
        remote_repo = self._pull_remote_sources()

        self[repoName].remote("origin").pull(progress=TqdmProgressPrinter())
        self.updateGitHash_and_Tag(repoName)
        repoPath = os.path.join(self.PATH_TO_DATASHELF, "database", repoName)
        sourceInventory = pd.read_csv(
            os.path.join(repoPath, "source_inventory.csv"),
            index_col=0,
            dtype={"source_year": str},
        )
        new_inventory = pd.concat(
            [old_inventory[old_inventory["source"] != repoName], sourceInventory]
        )

        return new_inventory

    def verifyGitHash(self, repoName):
        """
        Function to verify the git hash code of an existing git repository
        """
        repo = self.repositories[repoName]
        if repo.commit().hexsha != self.sources.loc[repoName, "git_commit_hash"]:
            raise RuntimeError(
                "Source {} is inconsistent with overall database".format(repoName)
            )

    def updateGitHash_and_Tag(self, repoName):
        """
        Function to update the git hash code in the sources.csv by the repo hash code
        """
        self.sources.loc[repoName, "git_commit_hash"] = self[repoName].commit().hexsha
        tag = self.get_tag_of_source(repoName)
        self.sources.loc[repoName, "tag"] = tag

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


#%% TqdmProgressPrinter
class TqdmProgressPrinter(git.RemoteProgress):
    known_ops = {
        git.RemoteProgress.COUNTING: "counting objects",
        git.RemoteProgress.COMPRESSING: "compressing objects",
        git.RemoteProgress.WRITING: "writing objects",
        git.RemoteProgress.RECEIVING: "receiving objects",
        git.RemoteProgress.RESOLVING: "resolving stuff",
        git.RemoteProgress.FINDING_SOURCES: "finding sources",
        git.RemoteProgress.CHECKING_OUT: "checking things out",
    }

    def __init__(self):
        super().__init__()
        self.progressbar = None

    def update(self, op_code, cur_count, max_count=None, message=""):
        if op_code & self.BEGIN:
            desc = self.known_ops.get(op_code & self.OP_MASK)
            self.progressbar = tqdm.tqdm(desc=desc, total=max_count)

        self.progressbar.set_postfix_str(message, refresh=False)
        self.progressbar.update(cur_count)

        if op_code & self.END:
            self.progressbar.close()
