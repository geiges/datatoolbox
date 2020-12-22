#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 09:34:50 2020

@author: ageiges
"""

def yearsColumnsOnly(index):
    """
    Extracts from any given index only the index list that can resemble 
    as year 
    
    e.g. 2001
    """
    
    import re
    REG_YEAR   = re.compile('^[0-9]{4}$')
    
    newColumns = []
    for col in index:
        if REG_YEAR.search(str(col)) is not None:
            newColumns.append(col)
        else:
            try: 
                if ~np.isnan(col) and REG_YEAR.search(str(int(col))) is not None:
                #   test float string
                    newColumns.append(col)
            except:
                pass
    return newColumns