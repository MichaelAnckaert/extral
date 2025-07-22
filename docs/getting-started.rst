Getting Started
===============

This guide will help you get started with Extral, a Python-powered ETL application.

Installation
------------

Extral can be installed using pip:

.. code-block:: bash

   pip install extral

Requirements
~~~~~~~~~~~~

* Python 3.12 or higher
* Access to source and destination databases
* Network connectivity between Extral and your databases

Basic Usage
-----------

Extral is configured using YAML files and run from the command line.

1. **Create a configuration file** (e.g., ``config.yaml``)
2. **Run the ETL process** using the command line tool

Command Line Interface
~~~~~~~~~~~~~~~~~~~~~~

The basic command to run Extral is:

.. code-block:: bash

   extral --config <path-to-config-file>

Command line options:

* ``--config <file>`` - Path to the YAML configuration file (required)

Example Usage
-------------

Here's a simple example that copies data from a MySQL database to PostgreSQL:

**config.yaml:**

.. code-block:: yaml

   logging:
     level: info

   processing:
     workers: 4

   source:
     type: mysql
     host: localhost
     port: 3306
     user: root
     password: example_password
     database: source_db
     charset: utf8mb4

   tables:
     - name: customers
       batch_size: 100
       strategy: merge
       merge_key: id
       incremental:
         field: updated_on
         type: datetime
         initial_value: '2022-01-01T00:00:00'
     - name: orders
       strategy: append

   destination:
     type: postgresql
     host: localhost
     port: 5432
     user: loader
     password: example_password
     database: dest_db
     schema: public

**Run the ETL:**

.. code-block:: bash

   extral --config config.yaml

Testing
-------

Extral includes comprehensive testing capabilities using pytest.

Running Tests
~~~~~~~~~~~~~

.. code-block:: bash

   # Run all tests
   uv run pytest

   # Run only unit tests (fast, no external dependencies)
   uv run pytest -m unit

   # Run only integration tests (requires external services)
   uv run pytest -m integration

   # Run tests with coverage report
   uv run pytest --cov=src/extral --cov-report=html

   # Run specific test file
   uv run pytest tests/unit/test_config.py

Test Categories
~~~~~~~~~~~~~~~

Tests are organized using markers:

* ``unit`` - Fast tests that don't require external dependencies
* ``integration`` - Tests requiring external services (databases, files)
* ``database`` - Tests requiring database connections
* ``file`` - Tests requiring file system access
* ``slow`` - Tests that take longer to run
* ``network`` - Tests requiring network access

Next Steps
----------

* Read the :doc:`configuration` guide to learn about all available options
* Explore the :doc:`api` documentation for advanced usage
* Check out the examples in the project repository