# Copyright 2025 Michael Anckaert
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Pytest configuration and fixtures for Extral tests.
"""

import tempfile
import json
import os
from pathlib import Path
from typing import Dict, Any, Generator
import pytest

from extral.config import (
    Config, 
    PipelineConfig, 
    DatabaseConfig, 
    FileConfig, 
    TableConfig, 
    FileItemConfig,
    LoadStrategy
)


# Test configuration fixtures
@pytest.fixture
def sample_database_config() -> DatabaseConfig:
    """Sample database configuration for testing."""
    return DatabaseConfig(
        type="mysql",
        host="localhost",
        port=3306,
        user="test_user",
        password="test_password",
        database="test_db",
        charset="utf8mb4",
        tables=[
            TableConfig(
                name="users",
                strategy=LoadStrategy.REPLACE,
                merge_key=None,
                batch_size=1000
            ),
            TableConfig(
                name="orders",
                strategy=LoadStrategy.MERGE,
                merge_key="id",
                batch_size=500
            )
        ]
    )


@pytest.fixture  
def sample_file_config() -> FileConfig:
    """Sample file configuration for testing."""
    return FileConfig(
        type="csv",
        files=[
            FileItemConfig(
                name="users_file",
                format="csv",
                file_path="/tmp/users.csv",
                strategy=LoadStrategy.APPEND,
                batch_size=100
            ),
            FileItemConfig(
                name="orders_file",
                format="csv",
                http_path="https://example.com/orders.csv",
                strategy=LoadStrategy.REPLACE,
                batch_size=200
            )
        ]
    )


@pytest.fixture
def sample_pipeline_config(sample_database_config: DatabaseConfig) -> PipelineConfig:
    """Sample pipeline configuration for testing."""
    return PipelineConfig(
        name="test_pipeline",
        source=sample_database_config,
        destination=DatabaseConfig(
            type="postgresql",
            host="localhost", 
            port=5432,
            user="postgres_user",
            password="postgres_password",
            database="target_db",
            schema="public"
        ),
        workers=2
    )


@pytest.fixture
def sample_multi_pipeline_config(
    sample_database_config: DatabaseConfig,
    sample_file_config: FileConfig
) -> Config:
    """Sample multi-pipeline configuration for testing."""
    pipeline1 = PipelineConfig(
        name="db_to_db_pipeline",
        source=sample_database_config,
        destination=DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres_user", 
            password="postgres_password",
            database="target_db",
            schema="public"
        ),
        workers=4
    )
    
    pipeline2 = PipelineConfig(
        name="file_to_db_pipeline",
        source=sample_file_config,
        destination=DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="mysql_user",
            password="mysql_password", 
            database="file_target_db",
            charset="utf8mb4"
        ),
        workers=2
    )
    
    return Config(pipelines=[pipeline1, pipeline2])


@pytest.fixture
def temp_config_file() -> Generator[str, None, None]:
    """Create a temporary configuration file for testing."""
    config_data = {
        "pipelines": [
            {
                "name": "test_pipeline",
                "source": {
                    "type": "mysql",
                    "host": "localhost",
                    "port": 3306,
                    "user": "test_user",
                    "password": "test_password", 
                    "database": "test_db",
                    "charset": "utf8mb4",
                    "tables": [
                        {
                            "name": "test_table",
                            "strategy": "replace",
                            "batch_size": 1000
                        }
                    ]
                },
                "destination": {
                    "type": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "user": "postgres_user",
                    "password": "postgres_password",
                    "database": "target_db",
                    "schema": "public"
                },
                "workers": 2
            }
        ]
    }
    
    with tempfile.NamedTemporaryFile(
        mode='w', 
        suffix='.yaml',
        delete=False
    ) as f:
        import yaml
        yaml.dump(config_data, f)
        temp_file_path = f.name
    
    yield temp_file_path
    
    # Cleanup
    os.unlink(temp_file_path)


@pytest.fixture
def temp_csv_file() -> Generator[str, None, None]:
    """Create a temporary CSV file for testing."""
    csv_content = """id,name,email,age
1,John Doe,john@example.com,30
2,Jane Smith,jane@example.com,25
3,Bob Johnson,bob@example.com,35
"""
    
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.csv',
        delete=False
    ) as f:
        f.write(csv_content)
        temp_file_path = f.name
    
    yield temp_file_path
    
    # Cleanup
    os.unlink(temp_file_path)


@pytest.fixture  
def temp_json_file() -> Generator[str, None, None]:
    """Create a temporary JSON file for testing."""
    json_data = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
        {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35}
    ]
    
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.json', 
        delete=False
    ) as f:
        json.dump(json_data, f)
        temp_file_path = f.name
    
    yield temp_file_path
    
    # Cleanup
    os.unlink(temp_file_path)


@pytest.fixture
def temp_directory() -> Generator[str, None, None]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_state_file() -> Generator[str, None, None]:
    """Create a temporary state file for testing."""
    state_data = {
        "pipelines": {
            "test_pipeline": {
                "datasets": {
                    "test_table": {
                        "incremental": {
                            "last_value": "2023-01-01 00:00:00"
                        }
                    }
                }
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.json',
        delete=False
    ) as f:
        json.dump(state_data, f)
        temp_file_path = f.name
    
    yield temp_file_path
    
    # Cleanup
    os.unlink(temp_file_path)


# Database mock fixtures
@pytest.fixture
def mock_mysql_connection():
    """Mock MySQL connection for testing."""
    class MockMySQLConnection:
        def __init__(self):
            self.closed = False
            
        def cursor(self):
            return MockMySQLCursor()
            
        def commit(self):
            pass
            
        def close(self):
            self.closed = True
    
    class MockMySQLCursor:
        def __init__(self):
            self.closed = False
            
        def execute(self, query, params=None):
            pass
            
        def fetchone(self):
            return {"COUNT(*)": 1, "result": "success"}
            
        def fetchall(self):
            return [{"id": 1, "name": "test"}]
            
        def close(self):
            self.closed = True
            
        def __enter__(self):
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
    
    return MockMySQLConnection()


@pytest.fixture
def mock_postgresql_connection():
    """Mock PostgreSQL connection for testing."""
    class MockPostgreSQLConnection:
        def __init__(self):
            self.closed = False
            
        def cursor(self):
            return MockPostgreSQLCursor()
            
        def commit(self):
            pass
            
        def close(self):
            self.closed = True
    
    class MockPostgreSQLCursor:
        def __init__(self):
            self.closed = False
            
        def execute(self, query, params=None):
            pass
            
        def fetchone(self):
            return (True,)
            
        def fetchall(self):
            return [("id", "name"), (1, "test")]
            
        def close(self):
            self.closed = True
            
        def __enter__(self):
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
    
    return MockPostgreSQLConnection()


# Utility fixtures
@pytest.fixture(autouse=True)
def reset_state():
    """Reset global state before each test."""
    from extral.state import state
    # Reset the state singleton
    state._state_data = {}
    state._state_file_path = None
    yield
    # Cleanup after test
    state._state_data = {}
    state._state_file_path = None


@pytest.fixture
def sample_database_records():
    """Sample database records for testing."""
    return [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25}, 
        {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35}
    ]