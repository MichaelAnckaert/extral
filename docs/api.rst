API Reference
=============

This section provides detailed documentation of Extral's Python API.

.. note::
   Extral follows an Extract-Store-Load architecture with intermediate storage. The API is organized around connectors, handlers, and core processing modules.

Configuration Module
--------------------

Configuration management and validation for multi-pipeline setups.

.. automodule:: extral.config
   :members:
   :undoc-members:
   :show-inheritance:

Main Module
-----------

Main entry point and pipeline orchestration.

.. automodule:: extral.main
   :members:
   :undoc-members:
   :show-inheritance:

Context and Events
------------------

Application Context
~~~~~~~~~~~~~~~~~~~

Central coordinator for events, state, and configuration.

.. automodule:: extral.context
   :members:
   :undoc-members:
   :show-inheritance:

Event System
~~~~~~~~~~~~

Event-driven architecture for monitoring and coordination.

.. automodule:: extral.events
   :members:
   :undoc-members:
   :show-inheritance:

Handlers
--------

Console Handler
~~~~~~~~~~~~~~~

Traditional console output handler.

.. automodule:: extral.handlers.console_handler
   :members:
   :undoc-members:
   :show-inheritance:

TUI Handler
~~~~~~~~~~~

Interactive Text User Interface handler.

.. automodule:: extral.handlers.tui_handler
   :members:
   :undoc-members:
   :show-inheritance:

Stats Handler
~~~~~~~~~~~~~

Statistics collection and reporting handler.

.. automodule:: extral.handlers.stats_handler
   :members:
   :undoc-members:
   :show-inheritance:

Connectors
----------

Connector Base
~~~~~~~~~~~~~~

.. automodule:: extral.connectors.connector
   :members:
   :undoc-members:
   :show-inheritance:

Database Connectors
~~~~~~~~~~~~~~~~~~~

Generic Database Connector
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: extral.connectors.database.generic
   :members:
   :undoc-members:
   :show-inheritance:

MySQL Connector
^^^^^^^^^^^^^^^

.. automodule:: extral.connectors.database.mysql
   :members:
   :undoc-members:
   :show-inheritance:

PostgreSQL Connector
^^^^^^^^^^^^^^^^^^^^

.. automodule:: extral.connectors.database.postgresql
   :members:
   :undoc-members:
   :show-inheritance:

File Connectors
~~~~~~~~~~~~~~~

File Base Connector
^^^^^^^^^^^^^^^^^^^

.. automodule:: extral.connectors.file.base
   :members:
   :undoc-members:
   :show-inheritance:

CSV Connector
^^^^^^^^^^^^^

.. automodule:: extral.connectors.file.csv_connector
   :members:
   :undoc-members:
   :show-inheritance:

JSON Connector
^^^^^^^^^^^^^^

.. automodule:: extral.connectors.file.json_connector
   :members:
   :undoc-members:
   :show-inheritance:

File Utilities
^^^^^^^^^^^^^^

.. automodule:: extral.connectors.file.utils
   :members:
   :undoc-members:
   :show-inheritance:

Core Modules
------------

Extract Module
~~~~~~~~~~~~~~

Data extraction from sources with batch processing and incremental loading.

.. automodule:: extral.extract
   :members:
   :undoc-members:
   :show-inheritance:

Load Module
~~~~~~~~~~~

Data loading into destinations with multiple strategies (append, replace, merge).

.. automodule:: extral.load
   :members:
   :undoc-members:
   :show-inheritance:

Schema Module
~~~~~~~~~~~~~

.. automodule:: extral.schema
   :members:
   :undoc-members:
   :show-inheritance:

State Module
~~~~~~~~~~~~

.. automodule:: extral.state
   :members:
   :undoc-members:
   :show-inheritance:

Store Module
~~~~~~~~~~~~

.. automodule:: extral.store
   :members:
   :undoc-members:
   :show-inheritance:

Validation Module
~~~~~~~~~~~~~~~~~

Pre-flight validation for configuration and connectivity testing.

.. automodule:: extral.validation
   :members:
   :undoc-members:
   :show-inheritance:

Utility Modules
---------------

Database Module
~~~~~~~~~~~~~~~

.. automodule:: extral.database
   :members:
   :undoc-members:
   :show-inheritance:

Encoder Module
~~~~~~~~~~~~~~

.. automodule:: extral.encoder
   :members:
   :undoc-members:
   :show-inheritance:

Error Tracking Module
~~~~~~~~~~~~~~~~~~~~~

.. automodule:: extral.error_tracking
   :members:
   :undoc-members:
   :show-inheritance:

Exceptions Module
~~~~~~~~~~~~~~~~~

.. automodule:: extral.exceptions
   :members:
   :undoc-members:
   :show-inheritance: