DATATOOLBOX
=====================

This is a python package for handling global datasets. It contains the following features:

1. Augumented pandas DataFrames adding meta data
2. Automatic unit conversion and dataframe based computations
3. ID based data structure

This package is under development and serves as a collection of tools around various data analysis packages. 
The package is developed in-house and supported by Climate Analytics gGmbH and therefore mainly used in the context of climate change mitigation.

The included csv-based git database structure allows multi-user access to unified and version-controlled data sets. 
Data access is locally controlled by define data IDs and globally via dataset-IDs.

Authors:
- Andreas Geiges  
- Jonas Hörsch
- Gaurav Ganti

Dependencies
------------
- pandas
- numpy
- gitpython
- openscm-units
- pint==0.11
- pycountry
- fuzzywuzzy
- tqdm
- matplotlib
- openpyxl
- pyam-iamc<=0.8.0
- hdx-python-country
- networkx>=2.4.0
- xarray
- deprecated
- pyarrow

Installation via pip
--------------------

    Using pip:

    pip install datatoolbox

    Using conda
    
    conda install datatoolbox
    

Read the docs
-------------
https://datatoolbox.readthedocs.io/en/latest/

Testing
----------

From the root directory, run:

    pytest

