#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 11:07:40 2021

@author: ageiges
"""
import os
#%% patch 0.4.5
def patch_045_update_personal_config(personal):
    
    MODULE_PATH = os.path.dirname(__file__)

    fin = open(os.path.join(MODULE_PATH, 'settings','personal.py'), 'r')
    lines = fin.readlines()
    fin.close()
    # os.makedirs(os.path.join(config.MODULE_PATH, 'settings'),exist_ok=True)
    fout = open(os.path.join(MODULE_PATH, 'settings','personal.py'), 'w')
    
    for line in lines:
        if line.endswith('\n'):
            outLine = line
        else:
            outLine = line + '\n'
            
        fout.write(outLine)
    
    # add it to old personal config
    outLine = 'AUTOLOAD_SOURCES = True'
    fout.write(outLine)
    fout.close()
    
    personal.AUTOLOAD_SOURCES = False
    
    return personal