#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 11:07:40 2021

@author: ageiges
"""
import os
import shutil

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

def patch_047_move_config_file():
    from appdirs import user_data_dir
    appname = "datatoolbox"
    appauthor = "ageiges"
    CONFIG_DIR = user_data_dir(appname, appauthor)
    
    if os.path.isfile(os.path.join(os.path.dirname(__file__),'settings', 'personal.py')):
        print('Old configuration exists: APPLYING PATCH 47')
        
        if not os.path.exists(CONFIG_DIR):
            print('Creating new config folder')
            os.makedirs(CONFIG_DIR, exist_ok = True)
        
        print('Copying personal.py')
        shutil.copyfile(os.path.join(os.path.dirname(__file__), 'settings', 'personal.py'),
                        os.path.join(CONFIG_DIR,'personal.py'))
        print('removing old settings folder')
        # os.remove(os.path.join(os.path.dirname(__file__), 'settings', 'personal.py'))
        shutil.rmtree(os.path.join(os.path.dirname(__file__),'settings'))
if __name__ == '__main__':
    patch_047_move_config_file()