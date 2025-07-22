Getting Started
===============

This guide will help you get started with Extral, a Python-powered data migration and EL (Extract-Load) application with intermediate storage capabilities.

Installation
------------

Extral can be installed using uv (recommended) or pip:

.. code-block:: bash

   # Using uv (fast Python package manager)
   curl -LsSf https://astral.sh/uv/install.sh | sh
   uv tool install extral

   # Or using pip  
   pip install extral

Requirements
~~~~~~~~~~~~

* Python 3.12 or higher
* Access to source and destination databases
* Network connectivity between Extral and your databases

Basic Usage
-----------

Extral is configured using YAML files and supports multiple execution modes:

1. **Create a configuration file** (e.g., ``config.yaml``) with multi-pipeline format
2. **Choose your execution mode**: Console (default), TUI, or validation-only
3. **Run the data migration** using the command line tool

Command Line Interface
~~~~~~~~~~~~~~~~~~~~~~

The basic command to run Extral is:

.. code-block:: bash

   extral --config <path-to-config-file>

Command line options:

* ``--config <file>`` - Path to the YAML configuration file (required)
* ``--tui`` - Enable Text User Interface mode for interactive monitoring
* ``--validate-only`` - Validate configuration and connectivity without running data migration
* ``--dry-run`` - Show execution plan without actually running the migration
* ``--continue-on-error`` - Continue processing other datasets even if some fail
* ``--skip-datasets <names>`` - Skip specific datasets (comma-separated list)

Example Usage
-------------

Here are examples showing different ways to use Extral:

Database-to-Database Migration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**config.yaml:**

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
         port: 3306
         user: root
         password: example_password
         database: source_db
         charset: utf8mb4
       destination:
         type: postgresql
         host: localhost
         port: 5432
         user: loader
         password: example_password
         database: dest_db
         schema: public
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

File-to-Database Migration
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipelines:
     - name: csv_to_postgres
       source:
         type: file
         files:
           - name: customer_data
             format: csv
             file_path: /data/customers.csv
             # Or from HTTP: http_path: https://example.com/customers.csv
             options:
               delimiter: ","
               quotechar: "\""
               encoding: utf-8
             strategy: merge
             merge_key: customer_id
       destination:
         type: postgresql
         host: localhost
         port: 5432
         user: loader
         password: example_password
         database: warehouse

Running Different Modes
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Console mode (default) - shows progress in terminal
   extral --config config.yaml

   # TUI mode - interactive interface with real-time monitoring
   extral --config config.yaml --tui

   # Validation only - check configuration and connectivity
   extral --config config.yaml --validate-only

   # Dry run - show what would be done without executing
   extral --config config.yaml --dry-run

Testing
-------

Extral includes a comprehensive test suite using pytest with both unit and integration tests.

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

* ``unit`` - Fast tests that don't require external dependencies (config validation, error handling)
* ``integration`` - Tests requiring external services (database connectors, file operations)
* ``database`` - Tests requiring database connections
* ``file`` - Tests requiring file system access  
* ``slow`` - Tests that take longer to run
* ``network`` - Tests requiring network access

TUI Interface
-------------

Extral includes a sophisticated Text User Interface (TUI) for interactive monitoring:

**Features:**
* Real-time pipeline status monitoring  
* Dataset progress tracking (Active, Finished, Waiting columns)
* Live statistics and runtime display
* Scrollable log output with fullscreen mode (press 'l')
* Interactive controls (arrow keys for scrolling, 'q' to quit)
* Graceful shutdown confirmation dialogs

**Enable TUI Mode:**

.. code-block:: bash

   # Via command line flag
   extral --config config.yaml --tui

   # Or in configuration file
   logging:
     level: info
     mode: tui

**TUI Controls:**
* ``↑/↓`` - Scroll through dataset list
* ``l`` - Toggle log view (fullscreen)  
* ``q`` - Quit (with confirmation)
* ``Esc`` - Exit fullscreen log view

Next Steps
----------

* Read the :doc:`configuration` guide to learn about all available options
* Explore the :doc:`api` documentation for advanced usage
* Check out the examples in the project repository