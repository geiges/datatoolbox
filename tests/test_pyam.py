#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 12:23:20 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy as np
import pandas as pd
import os


def test_import():
    import pyam
    
    
def test_to_pyam_interface():
    #%%
    inv = dt.findp(variable = 'Emissions|CO2|Total',
                  source='SOURCE_A_2020')
    
    tableSet = dt.getTables(inv.index)
    
    
    idf = tableSet.to_pyam()
    
    tableSet_new = dt.interfaces.pyam.from_IamDataFrame(idf)
    
    assert tableSet.keys() == tableSet_new.keys()
    
    fields_to_check = dt.config.ID_FIELDS + dt.config.OPTIONAL_META_FIELDS
    for table_in, table_out in zip(tableSet, tableSet_new):
        meta_in = {table_in.meta.get(x,'') for x in fields_to_check}
        meta_out = {table_out.meta.get(x,'') for x in fields_to_check}
        assert meta_in == meta_out
        assert (table_in.values == table_out.values).all()
        assert (table_in.index == table_out.index).all()
        assert (table_in.columns == table_out.columns).all()

    


