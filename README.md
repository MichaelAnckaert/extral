# Extral

[![PyPI version](https://badge.fury.io/py/Extral.svg)](https://badge.fury.io/py/Extral)
[![Code Quality Checks](https://github.com/MichaelAnckaert/extral/actions/workflows/workflow.yml/badge.svg)](https://github.com/MichaelAnckaert/extral/actions/workflows/workflow.yml)

Extral is a versatile ETL (**Ex**tract, **Tra**nsform, **L**oad) application designed to move data from a *source* database to a *destination* database. 

Supported Connectors:
- **MySQL / MariaDB**\
  Both source and destination
- **PostgreSQL**\
  Both source and destination


## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Configuration
Extral uses YAML configuration files to define the ETL process. Below is a sample configuration format. 

Specify the configuration file with `--config <file.yaml>` when running the application.

### Config file format

```yaml
logging:
- level: info

source:
 - type: mysql
   host: localhost
   port: 3306
   user: root
   password: example_password
   database: example_db
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
  - name: order_types
    strategy: replace

destination:
  - type: postgresql
    host: localhost
    port: 5432
    user: loader
    password: example_password
    database: example_db
    schema: public
```

## Incremental data loading
Extral supports incremental data loading, which uses a *cursor* to track the data that has already been extracted. 

Sample configuration:
```yaml
tables:
  - name: customers
    batch_size: 100
    incremental:
      field: updated_on
      type: datetime
      initial_value: '2022-01-01T00:00:00'
```

In the example above, the table *customers* is configured to use a cursor based on the field 'updated_on'. The *type* and an *initial_value* are specified. During the first extraction, only records with an *'updated_on'* field later than January 1st, 2022 will be included. Subsequent extractions will use the last value seen in the *'updated_on'* field to extract new records.

## Data loading strategies
Extral supports three strategies for loading data into the destination database:

### Merge
The *merge* strategy updates existing records in the destination table based on a specified `merge_key` and inserts new records that do not already exist. This strategy is ideal for maintaining up-to-date data while avoiding duplication. 

Sample configuration:
```yaml
tables:
  - name: customers
    strategy: merge
    merge_key: id
```

### Replace
The *replace* strategy truncates the destination table before loading new data. This ensures that the destination table contains only the latest data extracted from the source. Use this strategy when you want to completely overwrite the existing data.

Sample configuration:
```yaml
tables:
  - name: order_types
    strategy: replace
```

### Append
The *append* strategy adds new records to the destination table without modifying or removing existing records. This is useful for scenarios where historical data needs to be preserved and new data is simply added to the table.

Sample configuration:
```yaml
tables:
  - name: orders
    strategy: append
```