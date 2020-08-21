#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:52:37 2020

@author: ageiges
"""

import datatoolbox as dt
import numpy.testing as npt
import numpy as np

from util import df1

def test_interpolation():
    from datatoolbox.tools.for_datatables import interpolate
    
    resTable = interpolate(df1)
    
    assert (resTable.loc['DEU',2012] == 3)
    assert np.isnan(resTable.loc['GBR',2010])
    assert np.isnan(resTable.loc['DEU',2015])
    assert np.isnan(resTable.loc['FRA',2013])
if __name__== '__main__':
    test_interpolation()