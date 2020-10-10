import os
import pandas as pd
import pyam
import xarray as xr
import tqdm
import git
import shutil
from pathlib import Path
from dataclasses import dataclass, field
from textwrap import dedent

from typing import Iterable, Union, Optional, Mapping

from .data_structures import Datatable, TableSet, read_csv
from . import config, util


@dataclass
class Source:
    inventory: pd.DataFrame = field(repr=False)

    def to_pyam(self, keys: Iterable[str]) -> pyam.IamDataFrame:
        """Generate a pyam dataframe from keys

        By retrieving datatables into a tableset and converting jointly.

        Arguments
        ---------
        keys : Iterable[str]
            Identifiers to get from source

        Returns
        -------
        pyam.IamDataFrame
        """
        return self.to_tableset(keys).to_pyam()

    def to_xarray(self, keys: Iterable[str]) -> xr.Dataset:
        return self.to_tableset(keys).to_xarray()

    def to_tableset(self, keys: Iterable[str]) -> TableSet:
        tableset = TableSet()
        for key in keys:
            tableset.add(self[key])
        return tableset


@dataclass
class DirectorySource(Source):
    path: Path

    @property
    def name(self):
        return self.path.name

    @staticmethod
    def read_inventory(path: os.PathLike) -> pd.DataFrame:
        return pd.read_csv(
            path / "source_inventory.csv", index_col=0, dtype={"source_year": str}
        )

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> "DirectorySource":
        path = Path(path)
        inventory = cls.read_inventory(path)
        return cls(inventory, path)

    def reload_inventory(self):
        self.inventory = inventory = self.read_inventory(self.path)
        return inventory

    def file_path(self, key: str) -> os.PathLike:
        return self.path / "tables" / (key.replace("|", "-").replace("/", "-") + ".csv")

    def __getitem__(self, key) -> Datatable:
        """
        Retrieve `key` as datatable
        """
        self.check_clean(refresh=False)
        return read_csv(self.file_path(key))

    def remove(self):
        """Removes the source entirely from disk"""
        shutil.rmtree(self.path)
        self.inventory = self.inventory.reindex(index=[])
        self.is_clean = False


#     def commitTable(self, dataTable, message, sourceMetaDict=None):
        
#         sourceID = dataTable.meta['source']
#         if not self.isSource(sourceID):
#             if sourceMetaDict is None:
#                 raise(BaseException('Source does not extist and now sourceMeta provided'))
#             else:
#                 if not( sourceMetaDict['SOURCE_ID'] in self.gitManager.sources.index):
#                     self._addNewSource(sourceMetaDict)
        
#         dataTable = util.cleanDataTable(dataTable)
#         self._addTable(dataTable)
#         self.add_to_inventory(dataTable)
           
#         self._gitCommit(message)

#     def commitTables(self, 
#                      dataTables, 
#                      message, 
#                      sourceMetaDict, 
#                      append_data=False, 
#                      update=False, 
#                      overwrite=False , 
#                      cleanTables=True):
#         # create a new source if not extisting
#         if not self.isSource(sourceMetaDict['SOURCE_ID']):
#             self._addNewSource(sourceMetaDict)

#         # only test if an table is update if the source did exist
#         if update:
#             oldTableIDs = [table.generateTableID() for table in dataTables]
#             self.updateTables(oldTableIDs, dataTables, message)
#             return            
        
#         else:
#             for dataTable in tqdm.tqdm(dataTables):
#                 if cleanTables:
#                     dataTable = util.cleanDataTable(dataTable)
                
#                 if dataTable.isnull().all().all():
#                     print('ommiting empty table: ' + dataTable.ID)
#                     continue
                
#                 if dataTable.ID not in self.inventory.index:
#                     # add entire new table
                    
#                     self._addTable(dataTable)
#                     self.add_to_inventory(dataTable)
#                 elif overwrite:
#                     oldTable = self.getTable(dataTable.ID)
#                     mergedTable = dataTable.combine_first(oldTable)
#                     mergedTable = Datatable(mergedTable, meta = dataTable.meta)
#                     self._addTable(mergedTable)
#                 elif append_data:
#                     # append data to table
#                     oldTable = self.getTable(dataTable.ID)
#                     mergedTable = oldTable.combine_first(dataTable)
#                     mergedTable = Datatable(mergedTable, meta = dataTable.meta)
#                     self._addTable(mergedTable)
                
#         self._gitCommit(message)
        

#     def updateTable(self, oldTableID, newDataTable, message):
        
#         sourceID = self._getSourceFromID(newDataTable.ID)
#         if not self.isSource(sourceID):
#             raise(BaseException('source  does not exist'))
            
#         self._updateTable(oldTableID, newDataTable)
#         self._gitCommit(message)
    
#     def updateTables(self, oldTableIDs, newDataTables, message):

#         """
#         same as updateTable, but for list of tables
#         """
#         sourcesToUpdate = list()
#         for tableID in oldTableIDs:
#             sourceID = self._getSourceFromID(tableID)
#             if sourceID not in sourcesToUpdate:
#                 sourcesToUpdate.append(sourceID)
        
#         # check that all sources do exist
#         for sourceID in sourcesToUpdate:
#             if not self.isSource(sourceID):
#                 raise(BaseException('source  does not exist'))
        
        
#         for oldTableID, newDataTable in tqdm.tqdm(zip(oldTableIDs, newDataTables)):
            
#             if oldTableID in self.inventory.index:
#                 self._updateTable(oldTableID, newDataTable)
#             else:
#                 dataTable = util.cleanDataTable(newDataTable)
#                 self._addTable(dataTable)
#                 self.add_to_inventory(dataTable)
#         self._gitCommit(message)

#     def _updateTable(self, oldTableID, newDataTable):
        
#         newDataTable = util.cleanDataTable(newDataTable)
        
#         oldDataTable = self.getTable(oldTableID)
#         oldID = oldDataTable.meta['ID']
#         #print(oldDataTable.meta)
#         newID = newDataTable.generateTableID()
        
#         if oldID == newID and (oldDataTable.meta['unit'] == newDataTable.meta['unit']):
#             #only change data
#             self._addTable( newDataTable)
#         else:
#             # delete old table
# #            tablePath = self._getPathOfTable(oldID)
#             self.removeTable(oldID)
            
#             # add new table
#             self._addTable( newDataTable)

#             #change inventory
#             self.inventory.rename(index = {oldID: newID}, inplace = True)
#             self.add_to_inventory(newDataTable)


#     def validate_ID(self, ID, print_statement=True):
#         RED = '\033[31m'
#         GREEN = '\033[32m'
#         BLACK = '\033[30m'
#         source = ID.split(config.ID_SEPARATOR)[-1]
#         print('TableID: {}'.format(ID))
#         valid = list()
#         if self.sourceExists(source):
#             if print_statement:
#                 print(GREEN + "Source {} does exists".format(source))
#             valid.append(True)
#         else:
#             if print_statement:
#                 print(RED + "Source {} does not exists".format(source))
#             valid.append(False)
#         if ID in self.inventory.index:
#             if print_statement:
#                 print(GREEN + "ID is in the inventory")
#             valid.append(True)
#         else:
#             if print_statement:
#                 print(RED + "ID is missing in the inventory")
#             valid.append(False)
            
#         fileName = self._getTableFileName(ID)
#         tablePath = os.path.join(config.PATH_TO_DATASHELF, 'database', source, 'tables', fileName)

#         if os.path.isfile(tablePath):
#             if print_statement:
#                 print(GREEN + "csv file exists")
#             valid.append(True)
#         else:
#             if print_statement:
#                 print(RED + "csv file does not exists")
            
#             valid.append(False)

#         print(BLACK)

#         return all(valid)

#     def removeTables(self, IDList):
        
#         sourcesToUpdate = list()
#         for tableID in IDList:
#             sourceID = self._getSourceFromID(tableID)
#             if sourceID not in sourcesToUpdate:
#                 sourcesToUpdate.append(sourceID)
        
#         # check that all sources do exist
#         for source in sourcesToUpdate:
#             if not self.isSource(sourceID):
#                 raise(BaseException('source  does not exist'))
        
#         for ID in IDList:
#             source = self.inventory.loc[ID, 'source']
#             tablePath = self._getTableFilePath(ID)

#             self.remove_from_inventory(ID)
#             self.gitManager.gitRemoveFile(source, tablePath)

#         self._gitCommit('Tables removed')
        
#     def removeTable(self, tableID):

#         sourceID = self._getSourceFromID(tableID)
#         if not self.isSource(sourceID):
#             raise(BaseException('source  does not exist'))
        
#         self._removeTable( tableID)
#         self._gitCommit('Table removed')
#         self._reloadInventory()

#     def _removeTable(self, ID):

#         source = self.inventory.loc[ID, 'source']
#         tablePath = self._getTableFilePath(ID)
#         self.remove_from_inventory(ID)
        
# #        self.gitManager[source].execute(["git", "rm", tablePath])
#         self.gitManager.gitRemoveFile(source, tablePath)
# #        self._gitCommit('Table removed')
# #        self._reloadInventory()
        
        
#     def _tableExists(self, ID):
#         return ID in self.inventory.index
    


#     def tableExist(self, tableID):
#         return self._tableExists(tableID)

#     def isConsistentTable(self, datatable):
        
#         if not pd.np.issubdtype(datatable.values.dtype, pd.np.number):
            
#             raise(BaseException('Sorry, data of table {} is needed to be numeric'.format(datatable)))            
            
#         # check that spatial index is consistend with defined countries or regions
#         for regionIdx in datatable.index:
#             #print(regionIdx + '-')
#             if not mapp.regions.exists(regionIdx) and not mapp.countries.exists(regionIdx):
#                 raise(BaseException('Sorry, region in table {}: {} does not exist'.format(datatable, regionIdx)))
        
#         # check that the time colmns are years
#         from pandas.api.types import is_integer_dtype
#         if not is_integer_dtype(datatable.columns):
#             raise(BaseException('Sorry, year index in table {} is not integer'.format(datatable)))

#         if sum(datatable.index.duplicated()) > 0:
#             print(datatable.meta)
#             raise(BaseException('Sorry, region index in table {} is not  unique'.format(datatable)))
#         return True
    

#     def _addTable(self, datatable):
    
        
#         ID = datatable.generateTableID()
#         source = datatable.source()
#         datatable.meta['creator'] = config.CRUNCHER
#         sourcePath = os.path.join('database', source)
#         filePath = os.path.join(sourcePath, 'tables',  self._getTableFileName(ID))
#         if (config.OS == 'win32') | (config.OS == "Windows"):
#             filePath = filePath.replace('|','___')
        
#         datatable = datatable.sort_index(axis='index')
#         datatable = datatable.sort_index(axis='columns')
        
        
        
#         self.isConsistentTable(datatable)
        
# #        if not self.sourceExists(source):
            
        
# #        tt = time.time()
#         self._gitAddTable(datatable, source, filePath)
# #        print('time: {:2.6f}'.format(time.time()-tt))
#         #self.add_to_inventory(datatable)
        
#     def _gitAddTable(self, datatable, source, filePath):
        
#         datatable.to_csv(os.path.join(config.PATH_TO_DATASHELF, filePath))
        
#         self.gitManager.gitAddFile(source, os.path.join('tables', self._getTableFileName(datatable.ID)))
    



@dataclass
class GitRepositorySource(DirectorySource):
    """
    # Management of git repositories for fast access
    """

    repo: git.Repo
    is_clean: Optional[bool] = None

    @classmethod
    def from_repo(cls, repo: git.Repo) -> "GitRepositorySource":
        path = Path(repo.working_dir)
        inventory = cls.read_inventory(path)
        return cls(inventory, path, repo)

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> "GitRepositorySource":
        return cls.from_repo(git.Repo(path))

    @classmethod
    def clone_from_remote(
        cls, name: str, path: Union[str, os.PathLike, None] = None
    ) -> "GitRepositorySource":
        if path is None:
            path = Path(config.PATH_TO_DATASHELF) / "database" / name

        url = config.DATASHELF_REMOTE + name + ".git"
        repo = git.Repo.clone_from(
            url=url, to_path=path, progress=TqdmProgressPrinter()
        )

        return cls.from_repo(repo)

    @classmethod
    def init(
        cls,
        name: str,
        meta: Mapping[str, str],
        path: Union[str, os.PathLike, None] = None,
    ) -> "GitRepositorySource":
        if path is None:
            path = Path(config.PATH_TO_DATASHELF) / "database" / name
        else:
            path = Path(path)

        print(f"creating folder {path}")
        path.mkdir(parents=True, exist_ok=True)
        repo = git.Repo.init(path)

        for folder in config.SOURCE_SUB_FOLDERS:
            sub_path = path / folder
            sub_path.mkdir(exist_ok=True)
            gitkeep = sub_path / ".gitkeep"
            gitkeep.touch()
            repo.index.add(gitkeep)

        meta_path = path / "meta.csv"
        util.dict_to_csv(meta, meta_path)
        repo.index.add(meta_path)

        inventory_path = path / "source_inventory.csv"
        inventory = pd.DataFrame(columns=config.INVENTORY_FIELDS)
        inventory.to_csv(inventory_path)
        repo.index.add(inventory_path)

        repo.index.commit("added source: " + name)

        return cls(inventory, path, repo)

    def reload_inventory(self) -> pd.DataFrame:
        # TODO Maybe refreshing is not a good idea here
        self.check_clean(refresh=True)
        return super().reload_inventory()

    def check_clean(self, refresh: bool = True) -> bool:
        """
        Check whether working dir of the repository is clean

        Returns
        ----
        True

        Raises
        ------
        DatatoolboxException if the repository is not clean
        """
        if refresh or not self.is_clean:
            self.is_clean = not self.repo.is_dirty()

        if not self.is_clean:
            diff = ""
            if (stage_diff := self.repo.git.diff(staged=True, stat=True)):
                diff += stage_diff + " (in stage)\n"
            if (working_diff := self.repo.git.diff(stat=True)):
                diff += working_diff + " (in working dir)\n"

            raise RuntimeError(
                f"Repository {self.name} at {self.path} has the following uncommitted"
                f" modifications:\n" + diff +
                dedent(f"""
                    You can either roll them back by calling rollback on this source, ie

                        dt.datashelf.sources['{self.name}'].rollback()

                    or, if you know they are not the result of a failed operation, commit them, ie

                        dt.datashelf.sources['{self.name}'].commit("<description of changes>", all=True)
                """)
            )

        return self.is_clean

    def commit(self, message, all=False):
        if all:
            self.repo.git.add(update=True)
        return self.repo.index.commit(message)

    def rollback(self):
        self.repo.head.reset(working_tree=True)
        self.check_clean(refresh=True)

    def push(self):
        """
        This function used git push to update the remote database with an updated
        source dataset.

        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO
        """
        repo = self.repo

        if "origin" not in repo.remotes:
            origin = repo.create_remote(
                "origin", config.DATASHELF_REMOTE + self.name + ".git"
            )
            origin.fetch()

            repo.heads.master.set_tracking_branch(origin.refs.master)

        repo.remote("origin").push(progress=TqdmProgressPrinter())

    def pull(self):
        """
        This function uses git to pull an updated remote source dataset to the local
        database.

        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO
        """
        self.repo.remote("origin").pull(progress=TqdmProgressPrinter())
        self.reload_inventory()

    update = pull


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
