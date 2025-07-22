Extral Documentation
====================

Welcome to Extral, a versatile ETL (**E**\ xtract, **T**\ ransform, **L**\ oad) application designed to move data from a *source* database to a *destination* database.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting-started
   configuration
   api

Features
--------

Extral supports the following connectors:

* **MySQL / MariaDB** - Both source and destination
* **PostgreSQL** - Both source and destination
* **File Sources** - CSV and JSON file support

Key capabilities:

* **Parallel Processing** - Configure multiple worker threads for simultaneous table processing
* **Incremental Loading** - Track changes using cursor fields to load only new/updated data
* **Multiple Load Strategies** - Append, replace, or merge data based on your requirements
* **Flexible Configuration** - YAML-based configuration for easy setup and maintenance

Quick Start
-----------

Install Extral:

.. code-block:: bash

   pip install extral

Create a configuration file (see :doc:`configuration` for details):

.. code-block:: yaml

   source:
     type: mysql
     host: localhost
     user: root
     password: example_password
     database: source_db

   destination:
     type: postgresql
     host: localhost
     user: loader
     password: example_password
     database: dest_db

   tables:
     - name: customers
       strategy: merge
       merge_key: id

Run the ETL process:

.. code-block:: bash

   extral --config config.yaml

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`