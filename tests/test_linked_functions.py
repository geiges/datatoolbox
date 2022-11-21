#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 25 12:32:28 2020

@author: ageiges
"""

import datatoolbox as dt
dt.admin.switch_database_to_testing()

def test_init():
    dt.inventory()
    # dt.DBinfo()
    dt.sourceInfo()
    assert len(dt.sourceInfo()) == len(dt.core.DB.sources)
    
def test_DB():
    
    assert hasattr(dt.core, 'DB')
    
if __name__ == '__main__':
    test_init()
    test_DB()