#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 11:05:30 2020

@author: ageiges
"""
from .. import config
from . import for_datatables

from . import pandas
from . import excel

from . import matplotlib

#%% optional
if config.AVAILABLE_XARRAY:
    from . import xarray

if config.AVAILABLE_DOCX:
    from . import word

# from . import magicc6

from . import pyam
