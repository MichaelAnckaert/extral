# Contributing to Extral

Thank you for your interest in contributing to Extral! This document outlines how to contribute to the project.

## License

By contributing to Extral, you agree that your contributions will be licensed under the Apache License 2.0.

## GitHub Workflow

### Project Repository

The main repository is hosted at: https://github.com/sinaxgcv/extral

### Forking and Pull Requests

1. **Fork the repository**: Click the "Fork" button on the GitHub repository page to create your own copy.

2. **Clone your fork**:
   ```bash
   git clone git@github.com:YOUR_USERNAME/extral.git
   cd extral
   ```

3. **Set up the upstream remote**:
   ```bash
   git remote add upstream git@github.com:sinaxgcv/extral.git
   ```

4. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

5. **Make your changes** following the guidelines in this document.

6. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

7. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Create a Pull Request**: Go to the GitHub repository and click "New Pull Request".

### Pull Request Guidelines

- **Title**: Use a descriptive title that clearly explains what your PR does
- **Description**: Include a detailed description of your changes
- **Testing**: Ensure all tests pass and add new tests for new functionality
- **Documentation**: Update documentation if your changes affect user-facing features
- **Code Quality**: Follow the project's coding standards and ensure linting passes

## Test Conventions

Extral uses pytest for testing with a comprehensive test suite structure.

### Test Organization

- **Unit Tests** (`tests/unit/`): Test individual components in isolation
- **Integration Tests** (`tests/integration/`): Test component interactions and external dependencies
- **Fixtures** (`tests/conftest.py`): Shared test fixtures and configuration

### Test Markers

Use pytest markers to categorize your tests:

- `@pytest.mark.unit`: Unit tests that don't require external dependencies
- `@pytest.mark.integration`: Integration tests requiring external services
- `@pytest.mark.database`: Tests requiring database connections
- `@pytest.mark.file`: Tests requiring file system access
- `@pytest.mark.slow`: Tests that are slow to run
- `@pytest.mark.network`: Tests requiring network access

### Running Tests

```bash
# Install development dependencies
uv sync --locked --all-extras --dev

# Run all tests
uv run pytest

# Run only unit tests
uv run pytest -m unit

# Run with coverage
uv run pytest --cov=src/extral --cov-report=html

# Run integration tests (requires database setup)
uv run pytest --run-integration
```

### Writing Tests

- **Test Naming**: Use descriptive test names: `test_should_extract_data_when_valid_config_provided`
- **Fixtures**: Use existing fixtures from `conftest.py` or create new ones for common test data
- **Mocking**: Use `pytest-mock` for mocking external dependencies
- **Assertions**: Use descriptive assertion messages
- **Test Data**: Use the provided fixtures for consistent test data

Example test structure:
```python
@pytest.mark.unit
def test_should_validate_config_when_all_required_fields_present(sample_database_config):
    """Test that configuration validation passes for valid config."""
    # Arrange
    config = sample_database_config
    
    # Act
    result = validate_config(config)
    
    # Assert
    assert result.is_valid
    assert result.errors == []
```

## Documentation

Extral uses Sphinx for documentation generation.

### Documentation Structure

- **Source**: Documentation files are in the `docs/` directory
- **API Reference**: Auto-generated from docstrings in the code
- **User Guides**: Written in reStructuredText (`.rst`) format

### Building Documentation

```bash
# Build documentation
cd docs
uv run sphinx-build -b html . _build/html

# Or use the provided script
./build_docs.sh
```

### Documentation Guidelines

- **Docstrings**: Use Google-style docstrings for all public functions and classes
- **Type Hints**: Include comprehensive type hints for all function parameters and return values
- **Examples**: Include usage examples in docstrings where appropriate
- **API Changes**: Update API documentation when making changes to public interfaces

Example docstring format:
```python
def extract_data(self, dataset_name: str, extract_config: ExtractConfig) -> Generator[list[DatabaseRecord], None, None]:
    """
    Extract data from the source dataset.

    Args:
        dataset_name: Name/identifier of the dataset to extract from
                     (could be table name, file path, API endpoint, etc.)
        extract_config: Configuration for extraction (incremental, batch size, etc.)

    Yields:
        Batches of database records as dictionaries

    Raises:
        ConnectionException: If unable to connect to the data source
        ValidationException: If the dataset_name or config is invalid

    Example:
        ```python
        config = ExtractConfig(batch_size=1000, incremental=True)
        for batch in connector.extract_data("users", config):
            process_batch(batch)
        ```
    """
```

## Code Quality

### Linting and Formatting

```bash
# Run Ruff linter
uv run ruff check ./src

# Auto-fix issues
uv run ruff check --fix ./src

# Format code
uv run ruff format ./src

# Type checking with MyPy
uv run mypy --config-file pyproject.toml src/extral
```

### Code Style Guidelines

- Follow PEP 8 conventions
- Use type hints for all function parameters and return values
- Write descriptive variable and function names
- Keep functions focused and small
- Add docstrings to all public interfaces

## Connector Framework Development

⚠️ **Important Note**: The connector framework is currently under active development and is not yet complete. The current implementation includes:

- **Abstract Base Class**: `Connector` interface defining core ETL operations
- **Database Connectors**: MySQL and PostgreSQL implementations
- **File Connectors**: Basic CSV and JSON file support

### Current Limitations

- **Plugin System**: No standardized plugin loading mechanism
- **Configuration Schema**: Connector-specific configuration validation is incomplete
- **Error Handling**: Connector-specific error handling patterns not fully established
- **Testing Framework**: Limited testing utilities for custom connectors

### Adding New Connectors

If you want to contribute a new connector, please:

1. **Discuss First**: Open an issue to discuss the connector design before implementing
2. **Follow Interface**: Implement the `Connector` abstract base class
3. **Configuration**: Define configuration schema using TypedDict patterns from existing connectors
4. **Testing**: Add comprehensive tests using the existing test framework
5. **Documentation**: Include usage examples and configuration documentation

Example connector structure:
```python
from extral.connectors.connector import Connector
from extral.config import ExtractConfig, LoadConfig

class YourConnector(Connector):
    def extract_data(self, dataset_name: str, extract_config: ExtractConfig):
        # Implementation
        pass
        
    def load_data(self, dataset_name: str, data: list, load_config: LoadConfig):
        # Implementation  
        pass
        
    def test_connection(self) -> bool:
        # Implementation
        pass
```

### Future Framework Plans

- Standardized connector registration and discovery
- Configuration validation framework
- Common testing utilities for connector developers
- Plugin packaging and distribution guidelines

## Getting Help

- **Issues**: Report bugs or request features via GitHub Issues
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Documentation**: Check the project documentation at `docs/`