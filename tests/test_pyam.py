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
# import pandas.testing as npt

from util_for_testing import df

dt.admin.switch_database_to_testing()

def test_import():
    import pyam
    

    
def test_to_pyam_interface():
    #%%
    inv = dt.findp(variable = 'Numbers**')
    
    tableSet = dt.getTables(inv.index)
    
    
    idf = tableSet.to_pyam()
    
    tableSet_new = dt.interfaces.pyam.from_IamDataFrame(idf)
    
    assert tableSet.keys() == tableSet_new.keys()
    
    fields_to_check = dt.config.ID_FIELDS + dt.config.OPTIONAL_META_FIELDS
    
    keys = list(tableSet.keys())
    keys.sort()
    
    for key  in keys:
        table_in  = tableSet[key]
        table_out = tableSet_new[key]
        meta_in = [table_in.meta.get(x,'') for x in fields_to_check]
        meta_out = [table_out.meta.get(x,'') for x in fields_to_check]
        assert meta_in == meta_out
        assert (table_in.values == table_out.values).all()
        assert (table_in.index == table_out.index).all()
        assert (table_in.columns == table_out.columns).all()

def test_lossless_conversion():
    
    exp = df.copy().clean() # copy initial datatable
    print(exp)
    idf = exp.to_pyam() # create IamDataFrame
    obs = dt.Datatable.from_pyam(idf)
    
    assert pd.testing.assert_frame_equal(obs,exp) is None

if __name__ == '__main__':
    test_import()
    test_to_pyam_interface()
    test_lossless_conversion()
