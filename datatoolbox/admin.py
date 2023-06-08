#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 12:01:06 2020

@author: ageiges
"""
import os
import pandas as pd
import platform
import git
import os

OS = platform.system()
# from . import config
from datatoolbox import config
import git
import shutil
import datatoolbox as dt
import subprocess

def create_empty_datashelf(pathToDataself, force_new=False):
    """
    User funciton to create a empty datashelf

    Parameters
    ----------
    pathToDataself : TYPE
        DESCRIPTION.
    force_new : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """
    from . import config

    _create_empty_datashelf(
        pathToDataself,
        config.MODULE_PATH,
        config.SOURCE_META_FIELDS,
        config.INVENTORY_FIELDS,
        force_new=force_new,
    )


def compare_source_inventory_to_main(sourceID):
    git_manager = dt.database.GitRepository_Manager(dt.config, debugmode=True)
    inventory_csv_path = os.path.join(git_manager.PATH_TO_DATASHELF, 'inventory.csv')
    inventory = pd.read_csv(inventory_csv_path, index_col=0)

    source_inventory_path = os.path.join(
        git_manager.PATH_TO_DATASHELF, 'database', sourceID, 'source_inventory.csv'
    )
    source_inventory = pd.read_csv(source_inventory_path, index_col=0)

    main_tables = inventory.index[inventory.source == sourceID]
    missing_tables_in_source = main_tables.difference(source_inventory.index)
    missing_tables_in_main = source_inventory.index.difference(main_tables)
    if len(missing_tables_in_source) > 0:
        print(f'{len(missing_tables_in_source)} are missing in source inventory')

    if len(missing_tables_in_main) > 0:
        print(f'{len(missing_tables_in_main)} are missing in main inventory')

    if (len(missing_tables_in_main) == 0) and (len(missing_tables_in_source) == 0):
        print('Both inventories are equivalent')

    return missing_tables_in_source, missing_tables_in_main


def fix_uncommited_changes_in_source(sourceID):
    """
    Fix source when uncommited changes prevend the normal use of datatoolbox database.
    All uncommited local changes will be removed using "git reset --hard"

    Parameters
    ----------
    sourceID : str
        Source to be fixed.

    Returns
    -------
    None.

    """
    from . import config

    git_manager = dt.database.GitRepository_Manager(config, debugmode=True)
    repo = git_manager.get_source_repo_failsave(sourceID)
    repo.git.reset('--hard', 'origin/master')
    print('All done')


def fix_inconsistent_source(sourceID, fix_what, source_hash=None):
    """
    Fix inconsistent source if the datatoolbox sources csv does indicated a different
    git hash than the actual source after e.g. an manual commit.

    Parameters
    ----------
    sourceID : str
        Source to be fixed after a manual chagne
    fix_what : str
        What to fix - options are currently only "sources.csv".

    Returns
    -------
    None.

    """

    from . import config

    git_manager = dt.database.GitRepository_Manager(config, debugmode=True)
    if fix_what == 'sources':
        main = git_manager.repositories['main'] = git.Repo(
            git_manager.PATH_TO_DATASHELF
        )

        source = git.Repo(
            os.path.join(git_manager.PATH_TO_DATASHELF, 'database', sourceID)
        )

        source_hash = source.head.object.hexsha

    elif fix_what == 'source_revision':
        main = git_manager.repositories['main'] = git.Repo(
            git_manager.PATH_TO_DATASHELF
        )

        source_repo = git.Repo(
            os.path.join(git_manager.PATH_TO_DATASHELF, 'database', sourceID)
        )
        source_repo.git.execute(["git", "reset", "--hard", source_hash])
        source_hash = source_repo.head.object.hexsha

        inventory_csv_path = os.path.join(
            git_manager.PATH_TO_DATASHELF, 'inventory.csv'
        )
        inventory = pd.read_csv(inventory_csv_path, index_col=0)

        ix_to_remove = inventory.index[inventory.source == sourceID]
        inventory = inventory.drop(ix_to_remove)

        source_inventory_path = os.path.join(
            git_manager.PATH_TO_DATASHELF, 'database', sourceID, 'source_inventory.csv'
        )
        source_inventory = pd.read_csv(source_inventory_path, index_col=0)

        inventory = pd.concat([inventory, source_inventory])
        inventory.to_csv(inventory_csv_path)
        main.index.add(inventory_csv_path)

    sources_csv_path = os.path.join(git_manager.PATH_TO_DATASHELF, 'sources.csv')

    sources_df = pd.read_csv(sources_csv_path, index_col=0)

    print(
        f'Overwriting has {sources_df.loc[sourceID, "git_commit_hash"]} -> {source_hash}'
    )
    sources_df.loc[sourceID, 'git_commit_hash'] = source_hash
    sources_df.to_csv(sources_csv_path)

    main.index.add(sources_csv_path)
    main.index.commit(f'Fixed source {sourceID}' + " by " + config.CRUNCHER)


def get_source_log(sourceID='main', n=5):
    """
    Returns last log history of a source

    Parameters
    ----------
    sourceID : TYPE, optional
        DESCRIPTION. The default is 'main'.
    n : TYPE, optional
        DESCRIPTION. The default is 5.

    Returns
    -------
    None.

    """
    git_manager = dt.database.GitRepository_Manager(dt.config, debugmode=True)

    if sourceID == 'main':
        repo = git.Repo(git_manager.PATH_TO_DATASHELF)
    else:
        repo = git.Repo(
            os.path.join(git_manager.PATH_TO_DATASHELF, 'database', sourceID)
        )
    # repo = git.Repo("/home/user/.emacs.d")
    commits = list(repo.iter_commits("master", max_count=n))
    print(f'============= Log of {sourceID} =============')
    for i in range(n):
        comm = commits[i]
        print(
            comm.committed_datetime.strftime('"%d-%m-%Y %H:%M')
            + ' - '
            + comm.message
            + ' - \t Hash: '
            + comm.hexsha
        )
    print(f'============= End log =============')


# def


def _create_empty_datashelf(
    pathToDataself, MODULE_PATH, SOURCE_META_FIELDS, INVENTORY_FIELDS, force_new=False
):
    """
    Private function to create empty datashelf without propper
    init of config
    """
    from pathlib import Path
    import os

    path = Path(pathToDataself)

    if force_new:
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)

    # add subfolders database
    Path(os.path.join(pathToDataself, 'database')).mkdir()
    Path(os.path.join(pathToDataself, 'mappings')).mkdir()
    Path(os.path.join(pathToDataself, 'rawdata')).mkdir()

    # create mappings
    os.makedirs(os.path.join(pathToDataself, 'mappings'), exist_ok=True)
    shutil.copyfile(
        os.path.join(MODULE_PATH, 'data/regions.csv'),
        os.path.join(pathToDataself, 'mappings/regions.csv'),
    )
    shutil.copyfile(
        os.path.join(MODULE_PATH, 'data/continent.csv'),
        os.path.join(pathToDataself, 'mappings/continent.csv'),
    )
    shutil.copyfile(
        os.path.join(MODULE_PATH, 'data/country_codes.csv'),
        os.path.join(pathToDataself, 'mappings/country_codes.csv'),
    )

    sourcesDf = pd.DataFrame(columns=SOURCE_META_FIELDS)
    filePath = os.path.join(pathToDataself, 'sources.csv')
    sourcesDf.to_csv(filePath,  index=False)

    inventoryDf = pd.DataFrame(columns=INVENTORY_FIELDS)
    filePath = os.path.join(pathToDataself, 'inventory.csv')
    inventoryDf.to_csv(filePath)
    git.Repo.init(pathToDataself)

def _open_dialog_for_user():
    # Linux
    if OS == 'Linux':
        import tkinter as tk
        from tkinter import simpledialog, filedialog

        ROOT = tk.Tk()
        ROOT.withdraw()
        userName = simpledialog.askstring(
            title="Initials", prompt="Enter your Initials or Name: "
        )
        print("Welcome", userName)

        root = tk.Tk()
        root.withdraw()  # use to hide tkinter window

        def search_for_file_path():
            currdir = os.getcwd()
            tempdir = filedialog.askdirectory(
                parent=root, initialdir=currdir, title='Please select a directory: '
            )
            if len(tempdir) > 0:
                print("You chose: %s" % tempdir)
            return tempdir

        file_path_variable = search_for_file_path()

    else:
        userName = input("Please enter your initials: ")
        file_path_variable = input("Please enter path to datashelf: ")

    return userName, file_path_variable

def create_personal_setting(
        modulePath, 
        OS,
        userName = None,
        file_path_variable = None):

    from . import config

    os.makedirs(config.CONFIG_DIR, exist_ok=True)
    
    fin = open(os.path.join(modulePath, 'data', 'personal_template.py'), 'r')

    fout = open(os.path.join(config.CONFIG_DIR, 'personal.py'), 'w')
    
    if (userName is None) or (file_path_variable is None):
        userName, file_path_variable = _open_dialog_for_user()
    
    if file_path_variable.startswith('..'):
        raise(Exception('Please use an absolute path to the datashelf directory.'))
    for line in fin.readlines():
        outLine = line.replace('XX', userName).replace('/PPP/PPP', file_path_variable)
        fout.write(outLine)
    fin.close()
    fout.close()


def create_initial_config(module_path, config_path, write_config=True):
    import git

    fin = open(os.path.join(module_path, 'data', 'personal_template.py'), 'r')

    DEBUG = False
    READ_ONLY = True
    sandboxPath = os.path.join(module_path, 'data', 'SANDBOX_datashelf')

    git.Repo.init(sandboxPath)

    if write_config:
        # create directory
        os.makedirs(config_path, exist_ok=True)
        fout = open(os.path.join(config_path, 'personal.py'), 'w')
        for line in fin.readlines():
            outLine = line.replace('/PPP/PPP', sandboxPath)
            fout.write(outLine)
        fout.close()
    fin.close()

    return 'XXX', sandboxPath, READ_ONLY, DEBUG


def change_personal_config(userName = None, file_path_variable = None):
    #    from .tools.install_support import create_personal_setting
    modulePath = os.path.dirname(__file__) + '/'
    create_personal_setting(modulePath, OS, userName, file_path_variable)


def _create_test_tables():
    """
    Creates tables in the database for testing
    """
    import numpy as np

    data = np.ones([4, 5])

    ones = dt.Datatable(
        data,
        columns=[2010, 2012, 2013, 2015, 2016],
        index=['ARG', 'DEU', 'FRA', 'GBR'],
        meta={
            'entity': 'Numbers',
            'category': 'Ones',
            'scenario': 'Historic',
            'source': 'Numbers_2020',
            'unit': 'm',
        },
    )

    fives = dt.Datatable(
        data * 5,
        columns=[2010, 2012, 2013, 2015, 2016],
        index=['ARG', 'DEU', 'FRA', 'GBR'],
        meta={
            'entity': 'Numbers',
            'category': 'Fives',
            'scenario': 'Historic',
            'source': 'Numbers_2020',
            'unit': 'm',
        },
    )

    sourceMeta = {
        'SOURCE_ID': 'Numbers_2020',
        'collected_by': dt.config.CRUNCHER,
        'date': '31.08.2020',
        'source_url': 'datatoolbox',
        'licence': 'free for all',
    }

    dt.commitTable(ones, 'add first table', sourceMeta)
    dt.commitTable(fives, 'add second table', sourceMeta)


def _re_link_functions(dt):

    dt.commitTable = dt.core.DB.commitTable
    dt.commitTables = dt.core.DB.commitTables

    dt.updateTable = dt.core.DB.updateTable
    dt.updateTables = dt.core.DB.updateTables
    dt.updateTablesAvailable = dt.core.DB.updateTablesAvailable

    dt.removeTable = dt.core.DB.removeTable
    dt.removeTables = dt.core.DB.removeTables

    dt.findc = dt.core.DB.findc
    dt.findp = dt.core.DB.findp
    dt.find = dt.core.DB.findc
    dt.finde = dt.core.DB.finde
    dt.getTable = dt.core.DB.getTable
    dt.getTables = dt.core.DB.getTables
    dt.getTablesAvailable = dt.core.DB.getTablesAvailable

    dt.isAvailable = dt.core.DB._tableExists
    dt.validate_ID = dt.core.DB.validate_ID
    dt.sourceInfo = dt.core.DB.sourceInfo

    dt.updateExcelInput = dt.core.DB.updateExcelInput

def open_config_file():
    filepath = os.path.join(config.CONFIG_DIR,'personal.py')#
   
    if platform.system() == 'Darwin':       # macOS
        subprocess.run(['open', filepath], check=True)
    elif platform.system() == 'Windows':    # Windows
        os.startfile(filepath)
    else:                                   # linux variants
        subprocess.call(('xdg-open', filepath))

def set_autoload_source(boolean):
    """
    Admin funtion to change permanently the personal configuration "AUTOLOAD_SOURCES".
    If set to True and if the database is connecte to a remote git repository,
    datatoolbox is try to import missing sources if a table is loaded that is locally
    not available.

    Parameters
    ----------
    boolean : bool
        New config value.

    Returns
    -------
    None.

    """
    from . import config

    config.AUTOLOAD_SOURCES = boolean

    fin = open(os.path.join(config.CONFIG_DIR, 'personal.py'), 'r')
    lines = fin.readlines()
    fin.close()
    fout = open(os.path.join(config.CONFIG_DIR, 'personal.py'), 'w')

    line_found = False
    for line in lines:
        if line.startswith('AUTOLOAD'):
            outLine = 'AUTOLOAD_SOURCES = {}'.format(boolean)
            line_found = True
        else:
            outLine = line

        fout.write(outLine)

    if not line_found:
        # add it to old personal config
        outLine = 'AUTOLOAD_SOURCES = {}'.format(boolean)
        fout.write(outLine)
    fout.close()


def switch_database_to_testing():
    from . import config

    #    from datatoolbox.tools.install_support import create_initial_config
    #
    _, sandboxPath, READ_ONLY, DEBUG = create_initial_config(
        config.MODULE_PATH, config_path=config.CONFIG_DIR, write_config=False
    )
    #
    if os.path.exists(os.path.join(sandboxPath, 'sources.csv')):
        shutil.rmtree(sandboxPath)

    create_empty_datashelf(sandboxPath)
    config.PATH_TO_DATASHELF = sandboxPath
    config.SOURCE_FILE = os.path.join(sandboxPath, 'sources.csv')
    dt.core.DB = dt.database.Database()

    _re_link_functions(dt)
    _create_test_tables()


def naming_convention_test():
    #%%
    from naming_convention import entities

    variable_list = dt.findp().loc[:, ['variable', 'entity']]
    entities_to_test = tuple(entities)

    inconsistent_ids = list()
    nan_ids = list()
    for idx, (variable, entity) in variable_list.iterrows():
        if pd.isnull(entity):
            nan_ids.append(idx)
        else:
            if not idx.startswith(entities_to_test):
                inconsistent_ids.append(idx)

    #%%
    # finding entity
    tableList = list()
    for idx in nan_ids:

        variable, entity = variable_list.loc[idx, :]
        if idx.startswith(entities_to_test):
            candiates = list()

            for entity in entities_to_test:
                if idx.startswith(entity):
                    candiates.append(entity)

                sort_candiates = sorted(candiates, key=len)

            entity = sort_candiates[-1]
            category = variable.lstrip(entity).lstrip('|')
            table = dt.getTable(idx)
            table.meta['category'] = category
            table.meta['entity'] = entity
            tableList.append(table)
    dt.updateTables([x.ID for x in tableList], tableList, 'added entity')
