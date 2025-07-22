Configuration
=============

Extral uses YAML configuration files to define ETL pipelines. This section documents the complete configuration syntax and all available options.

Configuration File Structure
-----------------------------

The configuration file is structured in the following main sections:

.. code-block:: yaml

   logging:
     level: info

   processing:
     workers: 4

   pipelines:
     - name: pipeline1
       source:
         # source configuration
       destination:
         # destination configuration
       tables:  # or files:
         # table/file configurations

Global Configuration
--------------------

Logging Configuration
~~~~~~~~~~~~~~~~~~~~~

Controls the logging behavior of Extral.

.. code-block:: yaml

   logging:
     level: info  # debug, info, warning, error, critical

**Options:**

* ``level`` (string, default: "info") - Log level for the application

Processing Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

Controls parallel processing behavior.

.. code-block:: yaml

   processing:
     workers: 4  # Number of parallel workers

**Options:**

* ``workers`` (integer, default: 4) - Number of parallel table processing workers

Pipeline Configuration
----------------------

Extral supports multiple pipelines in a single configuration file. Each pipeline defines a complete ETL workflow.

Basic Pipeline Structure
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   pipelines:
     - name: my_pipeline
       source:
         # Source connector configuration
       destination:
         # Destination connector configuration
       workers: 2  # Optional: override global workers setting

**Pipeline Options:**

* ``name`` (string, required) - Unique name for the pipeline
* ``source`` (object, required) - Source connector configuration
* ``destination`` (object, required) - Destination connector configuration
* ``workers`` (integer, optional) - Override global worker count for this pipeline

Source and Destination Connectors
----------------------------------

Database Connectors
~~~~~~~~~~~~~~~~~~~

MySQL and PostgreSQL connectors share the same configuration structure:

.. code-block:: yaml

   source:  # or destination:
     type: mysql  # or postgresql
     host: localhost
     port: 3306   # 3306 for MySQL, 5432 for PostgreSQL
     user: username
     password: password
     database: database_name
     schema: public      # PostgreSQL only, optional
     charset: utf8mb4    # MySQL only, default: utf8mb4
     tables:
       - name: table1
         # table configuration options

**Database Connector Options:**

* ``type`` (string, required) - "mysql" or "postgresql"
* ``host`` (string, required) - Database server hostname
* ``port`` (integer, optional) - Database server port (defaults: MySQL=3306, PostgreSQL=5432)
* ``user`` (string, required) - Database username
* ``password`` (string, required) - Database password
* ``database`` (string, required) - Database name
* ``schema`` (string, optional) - Schema name (PostgreSQL only)
* ``charset`` (string, optional) - Character set (MySQL only, default: "utf8mb4")
* ``tables`` (array, required) - List of table configurations

File Connectors
~~~~~~~~~~~~~~~

File connectors support CSV and JSON files from local filesystem or HTTP URLs:

.. code-block:: yaml

   source:  # or destination:
     type: file
     files:
       - name: customers_data
         format: csv  # or json
         file_path: /path/to/customers.csv
         # OR
         http_path: https://example.com/customers.csv
         options:
           delimiter: ","
           quotechar: "\""
           encoding: utf-8
         strategy: replace
         merge_key: id

**File Connector Options:**

* ``type`` (string, required) - Must be "file"
* ``files`` (array, required) - List of file configurations

**File Item Options:**

* ``name`` (string, required) - Logical name for the file (like table name)
* ``format`` (string, required) - "csv" or "json"
* ``file_path`` (string) - Local file path (either this or http_path required)
* ``http_path`` (string) - HTTP/HTTPS URL (either this or file_path required)
* ``options`` (object, optional) - Format-specific options
* ``strategy`` (string, optional) - Load strategy: "append", "replace", "merge" (default: "replace")
* ``merge_key`` (string) - Required if strategy is "merge"
* ``batch_size`` (integer, optional) - Number of records to process per batch

Table Configuration
-------------------

Tables define how individual database tables or files are processed during the ETL operation.

Basic Table Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   tables:
     - name: customers
       strategy: merge
       merge_key: id
       batch_size: 1000

**Table Options:**

* ``name`` (string, required) - Name of the table
* ``strategy`` (string, optional) - Load strategy: "append", "replace", "merge" (default: "replace")
* ``merge_key`` (string) - Primary key field, required if strategy is "merge"
* ``batch_size`` (integer, optional) - Number of records to process per batch

Load Strategies
~~~~~~~~~~~~~~~

Append Strategy
^^^^^^^^^^^^^^^

Adds new records without modifying existing data:

.. code-block:: yaml

   tables:
     - name: logs
       strategy: append

Replace Strategy
^^^^^^^^^^^^^^^^

Replaces all data in the destination table:

.. code-block:: yaml

   tables:
     - name: reference_data
       strategy: replace
       replace:
         how: recreate  # or truncate

**Replace Options:**

* ``replace.how`` (string, optional) - "recreate" (default) drops and recreates the table, "truncate" only deletes records

Merge Strategy
^^^^^^^^^^^^^^

Updates existing records and inserts new ones based on a merge key:

.. code-block:: yaml

   tables:
     - name: customers
       strategy: merge
       merge_key: customer_id

**Merge Options:**

* ``merge_key`` (string, required) - Field used to identify existing records

Incremental Loading
~~~~~~~~~~~~~~~~~~~

Incremental loading processes only new or updated records based on a cursor field:

.. code-block:: yaml

   tables:
     - name: customers
       strategy: merge
       merge_key: id
       incremental:
         field: updated_at
         type: datetime
         initial_value: '2022-01-01T00:00:00'

**Incremental Options:**

* ``field`` (string, required) - Name of the cursor field
* ``type`` (string, required) - Data type: "datetime", "integer", "string"
* ``initial_value`` (string, optional) - Starting value for first extraction

Complete Example
----------------

Here's a complete configuration file example:

.. code-block:: yaml

   logging:
     level: info

   processing:
     workers: 4

   pipelines:
     - name: mysql_to_postgres
       source:
         type: mysql
         host: mysql.example.com
         port: 3306
         user: extractor
         password: secret123
         database: production
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
             batch_size: 500
           - name: product_categories
             strategy: replace
             replace:
               how: truncate

       destination:
         type: postgresql
         host: postgres.example.com
         port: 5432
         user: loader
         password: secret456
         database: warehouse
         schema: public

     - name: csv_to_postgres
       source:
         type: file
         files:
           - name: customer_updates
             format: csv
             file_path: /data/customer_updates.csv
             options:
               delimiter: ","
               quotechar: "\""
               encoding: utf-8
             strategy: merge
             merge_key: customer_id

       destination:
         type: postgresql
         host: postgres.example.com
         port: 5432
         user: loader
         password: secret456
         database: warehouse
         schema: staging

Legacy Configuration Format
---------------------------

Extral also supports a legacy single-pipeline configuration format for backward compatibility:

.. code-block:: yaml

   # Legacy format - automatically converted to pipeline format internally
   source:
     type: mysql
     # ... source configuration

   destination:
     type: postgresql
     # ... destination configuration

   tables:
     # ... table configurations

This format is internally converted to the new pipeline format with a default pipeline name.