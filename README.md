# Extral

[![PyPI version](https://badge.fury.io/py/Extral.svg)](https://badge.fury.io/py/Extral)
[![Code Quality Checks](https://github.com/MichaelAnckaert/extral/actions/workflows/workflow.yml/badge.svg)](https://github.com/MichaelAnckaert/extral/actions/workflows/workflow.yml)
[![Documenation](https://app.readthedocs.org/projects/extral/badge/?version=latest)](https://extral.readthedocs.io/en/latest/)

Extral is a versatile data migration application designed to efficiently **Extract** data from various sources, store it with intermediate processing, and **Load** it into destinations using an Extract-Store-Load pattern with fault tolerance and parallel processing capabilities.

> **Note**: Extral follows an Extract-Load (EL) workflow with intermediate storage. Data transformation capabilities are planned for future releases. 

All information including a Getting Started Guide can be found in the [User Documentation](https://extral.readthedocs.io/en/latest/).

## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for all information on how to contribute to Extral.


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

**User documentation can be found here: https://extral.readthedocs.io/en/latest/**. Instructions below focus on developers to add and build documentation.

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
