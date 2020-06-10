#!/usr/bin/env python
from __future__ import print_function
 
import os
import sys
import subprocess

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

from datatoolbox import __version__


INFO = {
    'version': __version__,
    }
#find_packages(exclude=['*personal.py', ])
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
    package_data = {'datatoolbox': ['data/*',
				    'data/SANDBOX_datashelf/*',
				    'data/SANDBOX_datashelf/mappings/*',
                                    'tools/*',
				    'tutorials/*',
                                    'pint_definitions.txt']}
    with open('README.md', encoding='utf-8') as f:
        long_description = f.read()
#    packages = find_packages()
    setup_kwargs = {
        "name": "datatoolbox",
        "version": INFO['version'],
        # update the following:
        "description": 'The Python Data Toolbox',
        "long_description" : long_description,
        "long_description_content_type" :"text/markdown",
        "author": 'Andreas Geiges',
        "author_email": 'a.geiges@gmail.com',
        "url": 'https://gitlab.com/climateanalytics/datatoolbox',
        "packages": packages,
        "package_dir": pack_dir,
        "package_data" : package_data,
        
        "install_requires": [
            "pandas",
            "gitpython",
            "openscm_units",
            "pint==0.9",
            "pycountry",
            "fuzzywuzzy",
            "tqdm",
            "pyam-iamc<=0.3.0",
            "openpyxl"],
        }
    rtn = setup(**setup_kwargs)

if __name__ == "__main__":
    main()

