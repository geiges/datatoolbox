#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 22:44:27 2020

@author: ageiges
"""

import git
import pandas as pd
import os
import tqdm 

#%%
path = '/Users/andreasgeiges/Documents/datashelf_minimal'
#path = '/media/sf_Documents/datashelf_minimal'
os.chdir(path)
repo = git.Git(path)

inventory = pd.read_csv(path + "/inventory.csv", index_col=0)

toDelete = list()
for idx,row in tqdm.tqdm(inventory.iterrows()):
#    print(row)
    if  '|' not in idx:
        continue
    
#    parts = idx.split('|')
    source = row['source']
    fileName = row.name + '.csv'
    if os.path.exists('database/' + source + '/' + fileName):
        repo.execute(['git', 'mv', 'database/' + source + '/' + fileName, 'database/' + source + '/' + fileName.replace('|','___')])
    else:
        toDelete.append('database/' + source + '/' + fileName)
        