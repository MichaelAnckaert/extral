Extral Documentation
====================

Welcome to Extral, a versatile data migration application designed to efficiently **Extract** data from a *source*, store it with intermediate processing, and **Load** it into a *destination*. 

.. note::
   Extral follows an Extract-Store-Load (EL) pattern with intermediate storage. Data transformation capabilities are planned for future releases.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting-started
   configuration
   api

Features
--------

Extral supports the following connectors:

**Database Sources and Destinations:**
* **MySQL / MariaDB** - Full bidirectional support
* **PostgreSQL** - Full bidirectional support

**File Sources:**
* **CSV Files** - Local files and HTTP/HTTPS URLs
* **JSON Files** - Local files and HTTP/HTTPS URLs with flexible schema support

Key capabilities:

* **TUI Interface** - Interactive Text User Interface for real-time monitoring and log viewing
* **Multi-Pipeline Processing** - Define and run multiple ETL pipelines in parallel
* **Parallel Processing** - Configure multiple worker threads for simultaneous dataset processing
* **Incremental Loading** - Track changes using cursor fields to load only new/updated data  
* **Multiple Load Strategies** - Append, replace (truncate/recreate), or merge data based on your requirements
* **Intermediate Storage** - Fault-tolerant compressed storage between extract and load phases
* **Pre-flight Validation** - Connectivity testing and configuration validation before execution
* **Flexible Configuration** - YAML-based multi-pipeline configuration for easy setup and maintenance

Quick Start
-----------

Install Extral using uv (recommended) or pip:

.. code-block:: bash

   # Using uv (fast Python package manager)
   curl -LsSf https://astral.sh/uv/install.sh | sh
   uv tool install extral

   # Or using pip
   pip install extral

Create a configuration file (see :doc:`configuration` for details):

.. code-block:: yaml

   logging:
     level: info

   processing:
     workers: 4

   pipelines:
     - name: mysql_to_postgres
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

Run the data migration:

.. code-block:: bash

   # Console mode (default)
   extral --config config.yaml

   # Interactive TUI mode
   extral --config config.yaml --tui

   # Validate configuration only
   extral --config config.yaml --validate-only

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`