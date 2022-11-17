#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 11:07:40 2021

@author: ageiges
"""
import os
import shutil

# import config


#%% patch 0.4.5
def patch_045_update_personal_config(personal):

    MODULE_PATH = os.path.dirname(__file__)

    fin = open(os.path.join(MODULE_PATH, "settings", "personal.py"), "r")
    lines = fin.readlines()
    fin.close()
    # os.makedirs(os.path.join(config.MODULE_PATH, 'settings'),exist_ok=True)
    fout = open(os.path.join(MODULE_PATH, "settings", "personal.py"), "w")

    for line in lines:
        if line.endswith("\n"):
            outLine = line
        else:
            outLine = line + "\n"

        fout.write(outLine)

    # add it to old personal config
    outLine = "AUTOLOAD_SOURCES = True"
    fout.write(outLine)
    fout.close()

    personal.AUTOLOAD_SOURCES = False

    return personal


#%% patch 0.4.7
def patch_047_move_config_file():
    from appdirs import user_data_dir

    appname = "datatoolbox"
    appauthor = "ageiges"
    CONFIG_DIR = user_data_dir(appname, appauthor)

    if os.path.isfile(
        os.path.join(os.path.dirname(__file__), 'settings', "personal.py")
    ):
        print("Old configuration exists: APPLYING PATCH 47")

        if not os.path.exists(CONFIG_DIR):
            print("Creating new config folder")
            os.makedirs(CONFIG_DIR, exist_ok=True)

        print("Copying personal.py")
        shutil.copyfile(
            os.path.join(os.path.dirname(__file__), "settings", "personal.py"),
            os.path.join(CONFIG_DIR, "personal.py"),
        )
        print("removing old settings folder")
        # os.remove(os.path.join(os.path.dirname(__file__), 'settings', 'personal.py'))
        shutil.rmtree(os.path.join(os.path.dirname(__file__), "settings"))


#%% patch 0.5
# def patch_050_source_tracking(personal):
#     from appdirs import user_data_dir

#     appname = "datatoolbox"
#     appauthor = "ageiges"
#     CONFIG_DIR = user_data_dir(appname, appauthor)

#     fin = open(os.path.join(CONFIG_DIR, "personal.py"), "r")
#     lines = fin.readlines()
#     fin.close()
#     # os.makedirs(os.path.join(config.MODULE_PATH, 'settings'),exist_ok=True)
#     fout = open(os.path.join(CONFIG_DIR, "personal.py"), "w")

#     for line in lines:
#         if line.endswith("\n"):
#             outLine = line
#         else:
#             outLine = line + "\n"

#         fout.write(outLine)
#     # add it to old personal config
#     outLine = "last_checked_remote = None"
#     fout.write(outLine)
#     fout.close()

#     config.last_checked_remote = None

#     return personal


def patch_050_update_sources_csv(coreDB):
    # update columns
    print("patching sources.csv")
    coreDB.sources["tag"] = None
    for sourceID in coreDB.sources.index:
        tag = coreDB.gitManager.get_tag_of_source(sourceID)
        coreDB.sources.loc[sourceID, "tag"] = tag
    coreDB._gitCommit("update sources by patch for v0.50")

    from .database import GitRepository_Manager
    from . import config

    coreDB.gitManager = GitRepository_Manager(config)
    coreDB.INVENTORY_PATH = os.path.join(coreDB.path, "inventory.csv")
    coreDB.inventory = coreDB._load_inventory(coreDB.INVENTORY_PATH)
    coreDB.sources = coreDB.gitManager.sources
    print("All done!")
