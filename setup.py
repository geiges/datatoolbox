#!/usr/bin/env python
from __future__ import print_function

import os
import sys
import subprocess

from setuptools import setup, find_packages

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
    package_data = {
        'datatoolbox': [
            'data/*',
            'data/SANDBOX_datashelf/*',
            'data/SANDBOX_datashelf/mappings/*',
            'tools/*',
            'tutorials/*.py',
            'pint_definitions.txt']}
    with open('README.md', encoding='utf-8') as f:
        long_description = f.read()
#    packages = find_packages()
    setup_kwargs = {
        "name": "datatoolbox",
        "description": 'The Python Data Toolbox',
        "long_description" : long_description,
        "long_description_content_type" :"text/markdown",
        "author": 'Andreas Geiges',
        "author_email": 'a.geiges@gmail.com',
        "url": 'https://gitlab.com/climateanalytics/datatoolbox',
        "packages": packages,
        "package_dir": pack_dir,
        "package_data" : package_data,
        "use_scm_version": {'write_to': 'datatoolbox/version.py'},
        "setup_requires": ['setuptools_scm'],
        "install_requires": [
            "appdirs",
            "pandas",
            "gitpython",
            "openscm_units",
            "pint",
            "pint-xarray",
            "pycountry",
            "fuzzywuzzy",
            "tqdm",
            "pyam-iamc",
            "networkx",
            "pint_xarray",
            "openpyxl",
            "xarray",
            "deprecated",],
        }
    rtn = setup(**setup_kwargs)

if __name__ == "__main__":
    main()

