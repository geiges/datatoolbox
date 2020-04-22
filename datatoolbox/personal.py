    #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Personal setup
"""
from sys import platform

CRUNCHER = 'AG'

DB_READ_ONLY = False

if platform == "linux" or platform == "linux2":
    PATH_TO_DATASHELF = '/media/sf_Documents/datashelf/'
else:
    PATH_TO_DATASHELF = '/Users/andreasgeiges/Documents/datashelf/'