DATA TOOL BOX
=====================

This is a python tool box project for handling global datasets. It contains the following features:

1.
2.

Authors:
Andreas Geiges 


Dependencies
------------

1. `nose <https://pypi.python.org/pypi/nose/1.3.7>`_
2. `Sphinx <https://pypi.python.org/pypi/Sphinx>`_

Download this project
--------------------

Choose a project name and run the following commands replacing ``<project
name>`` with your choice::

    git clone https://gitlab.com/datatoolbox.git

You should now have a fresh new repository with your project ready to go. You
can sync it with Github via::

Installing
----------

You can install the stub like any other python module::

    ./setup.py install

or for local installations::

    ./setup.py install --user

Testing
----------

From the root directory, run::

    nosetests -w tests

Continuous Integration
-----------------------


This repository currently has its documentation on `ReadTheDocs
<http://datatoolbox.readthedocs.org/en/latest/>`_. You can add any new project and
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
	
You can then view the docs at http://localhost:8000/build/html/

On Windows
~~~~~~~~~~~~~~~~~

Follow the above instructions replacing ``make`` with ``./make.bat``.
