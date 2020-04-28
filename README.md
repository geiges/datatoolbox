DATA TOOL BOX
=====================

This is a python tool box project for handling global datasets. It contains the following features:

1. Augumented pandas DataFrames adding meta data, 
2. Automatic unit conversion and table based computations
3. ID based data structure

Authors:
Andreas Geiges 


Dependencies
------------

1. `nose <https://pypi.python.org/pypi/nose/1.3.7>`_
2. `Sphinx <https://pypi.python.org/pypi/Sphinx>`_

Installation via pip
--------------------


    pip install -i https://test.pypi.org/simple/ datatoolbox


Testing
----------

From the root directory, run::

    nosetests -w tests

Continuous Integration
-----------------------


This repository currently has its documentation on `ReadTheDocs
[<http://datatoolbox.readthedocs.org/en/latest/]. You can add any new project and
have it automatically hosted by doing the following

- on Github

  - Setting -> Webhooks and Services-> Add Service -> ReadTheDocs

- on ReadTheDocs

  - Add the project
  - On readthedocs.org/projects/<project name>, do Admin -> Advanced Settings ->
    Check the "Install Project" Box at the top

On *Nix Platforms
~~~~~~~~~~~~~~~~~

After you install the project locally, you can generate documentation by::

    cd docs
    make html

You can serve the documentation locally via::

    make serve
	
You can then view the docs at [http://localhost:8000/build/html/]


