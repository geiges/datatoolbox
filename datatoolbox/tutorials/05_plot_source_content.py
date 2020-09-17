#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 19 17:54:38 2020

@author: ageiges
"""

#%%
import datatoolbox as dt
source = 'UNFCCC_CRF_2020'
entities = dt.find(source=source).entity.unique()

for entity in entities:
    dt.find(entity=entity,source=source).graph(entity + '.png')

#%%
res = dt.find(source=source)
res.index
dt.removeTables(list(res.index))
