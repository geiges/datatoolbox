#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 10:57:28 2020

@author: ageiges
"""



def __getAvailableFiles():
    import sys
    from . import config
    sys.path.append(config.PATH_TO_WORKFLOWS)
    #%%
    from os import listdir
    from os.path import isfile, join
    files = [f for f in listdir(config.PATH_TO_WORKFLOWS) if isfile(join(config.PATH_TO_WORKFLOWS, f))]

    return files


for file in __getAvailableFiles():
    
    if '__' in file:
        continue
    
    __new_moduleName = file.replace('.py','')
    try:
        globals()[__new_moduleName]= __import__(__new_moduleName)    
    except:
        print("failed to import workflow for {}".format(__new_moduleName))
del file

