# Extral

[![PyPI version](https://badge.fury.io/py/Extral.svg)](https://badge.fury.io/py/Extral)
[![Code Quality Checks](https://github.com/MichaelAnckaert/extral/actions/workflows/workflow.yml/badge.svg)](https://github.com/MichaelAnckaert/extral/actions/workflows/workflow.yml)
[![Documenation](https://app.readthedocs.org/projects/extral/badge/?version=latest)]

Extral is a versatile ETL (**Ex**tract, **Tra**nsform, **L**oad) application designed to move data from a *source* database to a *destination* database. 

Supported Connectors:
- **MySQL / MariaDB**\
  Both source and destination
- **PostgreSQL**\
  Both source and destination


## Testing

Extral uses pytest for testing with both unit and integration tests.

### Running Tests

```bash
# Run all tests
uv run pytest

# Run only unit tests (fast, no external dependencies)
uv run pytest -m unit

# Run only integration tests (requires external services)
uv run pytest -m integration

# Run tests with coverage report
uv run pytest --cov=src/extral --cov-report=html

# Run tests in verbose mode
uv run pytest -v

# Run specific test file
uv run pytest tests/unit/test_config.py
```

### Test Categories

Tests are organized using markers:
- `unit`: Fast tests that don't require external dependencies
- `integration`: Tests requiring external services (databases, files)
- `database`: Tests requiring database connections
- `file`: Tests requiring file system access
- `slow`: Tests that take longer to run
- `network`: Tests requiring network access

## Documentation

Extral includes comprehensive Sphinx documentation covering installation, configuration, and API reference.

### Building Documentation

To build the documentation locally:

```bash
# Install development dependencies (includes Sphinx)
uv sync --group dev

# Build HTML documentation
cd docs && make html

# Or use the convenience script
./docs/build_docs.sh
```

The built documentation will be available at `docs/_build/html/index.html`.

### Documentation Structure

- **Getting Started** - Installation and basic usage guide
- **Configuration** - Complete YAML configuration syntax reference  
- **API Reference** - Auto-generated API documentation for all modules

### Updating Documentation

- **Content**: Edit the `.rst` files in the `docs/` directory
- **API docs**: Automatically generated from docstrings in the source code
- **Version**: Automatically synced from `src/extral/__init__.py`

After making changes, rebuild the documentation using the commands above.

## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Configuration
Extral uses YAML configuration files to define the ETL process. Below is a sample configuration format. 

Specify the configuration file with `--config <file.yaml>` when running the application.

### Config file format

```yaml
logging:
- level: info

processing:
- workers: 4  # Number of parallel table processing workers (default: 4)

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

## Processing Configuration
Extral supports parallel processing of multiple tables to improve performance. You can configure the number of worker threads used for parallel table processing.

```yaml
processing:
  - workers: 8  # Number of parallel table processing workers (default: 4)
```

The `workers` parameter controls how many tables can be processed simultaneously. The default value is 4 if not specified. Adjust this value based on your system resources and database capacity.

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
The *replace* strategy ensures that the destination table contains only the latest data extracted from the source. Use this strategy when you want to completely overwrite the existing data.

By default, the replace strategy will drop and recreate the destination table. You can configure how the strategy behaves using the `replace` configuration:

- **recreate** (default): Drops and recreates the destination table with the latest schema from the source
- **truncate**: Deletes all records from the destination table while preserving the table structure

Sample configuration:
```yaml
tables:
  - name: order_types
    strategy: replace
    # Uses recreate by default

  - name: products
    strategy: replace
    replace:
      how: truncate  # Only delete records, keep table structure
```

### Append
The *append* strategy adds new records to the destination table without modifying or removing existing records. This is useful for scenarios where historical data needs to be preserved and new data is simply added to the table.

Sample configuration:
```yaml
tables:
  - name: orders
    strategy: append
```