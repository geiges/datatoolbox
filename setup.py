#!/usr/bin/env python
from __future__ import print_function
 
import os
import sys
import subprocess

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

INFO = {
    'version': '0.1.4',
    }

def main():    
    packages = [
        'datatoolbox',
#	'tools',
#	'data',
        ]
    pack_dir = {
        'datatoolbox': 'datatoolbox',
#        'tools':'datatoolbox/tools',
#	'data': 'datatoolbox/data'
	}
    package_data = {'datatoolbox': ['data/*', 'tools/*']}
#    packages = find_packages()
    setup_kwargs = {
        "name": "datatoolbox",
        "version": INFO['version'],
        # update the following:
        "description": 'The Python Data Toolbox',
        "author": 'Andreas Geiges',
        "author_email": 'a.geiges@gmail.com',
        "url": 'https://gitlab.com/climateanalytics/datatoolbox',
        "packages": packages,
        "package_dir": pack_dir,
        "package_data" : package_data
        }
    rtn = setup(**setup_kwargs)

if __name__ == "__main__":
    main()

