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
    'version': '0.0.1',
    }

def main():    
    packages = [
        'datatoolbox', 
        ]
    pack_dir = {
        'datatoolbox': 'datatoolbox',
        }
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
        }
    rtn = setup(**setup_kwargs)

if __name__ == "__main__":
    main()

