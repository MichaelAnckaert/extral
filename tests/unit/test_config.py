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
Unit tests for configuration module.
"""

import pytest

from extral.config import (
    Config, 
    PipelineConfig, 
    DatabaseConfig, 
    FileConfig,
    TableConfig,
    FileItemConfig,
    LoadStrategy,
    ReplaceMethod
)


@pytest.mark.unit
class TestLoadStrategy:
    """Test LoadStrategy enum."""
    
    def test_load_strategy_values(self):
        """Test that LoadStrategy has the expected values."""
        assert LoadStrategy.APPEND.value == "append"
        assert LoadStrategy.REPLACE.value == "replace"  
        assert LoadStrategy.MERGE.value == "merge"


@pytest.mark.unit
class TestReplaceMethod:
    """Test ReplaceMethod enum."""
    
    def test_replace_method_values(self):
        """Test that ReplaceMethod has the expected values."""
        assert ReplaceMethod.TRUNCATE.value == "truncate"
        assert ReplaceMethod.RECREATE.value == "recreate"


@pytest.mark.unit
class TestTableConfig:
    """Test TableConfig dataclass."""
    
    def test_table_config_creation(self):
        """Test creating a TableConfig instance."""
        table_config = TableConfig(
            name="test_table",
            strategy=LoadStrategy.MERGE,
            merge_key="id", 
            batch_size=1000
        )
        
        assert table_config.name == "test_table"
        assert table_config.strategy == LoadStrategy.MERGE
        assert table_config.merge_key == "id"
        assert table_config.batch_size == 1000
    
    def test_table_config_defaults(self):
        """Test TableConfig with default values."""
        table_config = TableConfig(name="test_table")
        
        assert table_config.name == "test_table"
        assert table_config.strategy == LoadStrategy.REPLACE
        assert table_config.merge_key is None
        assert table_config.batch_size is None
    
    def test_table_config_from_dict(self):
        """Test creating TableConfig from dictionary."""
        data = {
            "name": "users",
            "strategy": "merge",
            "merge_key": "id",
            "batch_size": 500
        }
        
        table_config = TableConfig.from_dict(data)
        
        assert table_config.name == "users"
        assert table_config.strategy == LoadStrategy.MERGE
        assert table_config.merge_key == "id"
        assert table_config.batch_size == 500


@pytest.mark.unit
class TestFileItemConfig:
    """Test FileItemConfig dataclass."""
    
    def test_file_item_config_creation(self):
        """Test creating a FileItemConfig instance."""
        file_config = FileItemConfig(
            name="test_file",
            format="csv",
            file_path="/path/to/file.csv",
            strategy=LoadStrategy.APPEND,
            batch_size=100
        )
        
        assert file_config.name == "test_file"
        assert file_config.file_path == "/path/to/file.csv"
        assert file_config.strategy == LoadStrategy.APPEND
        assert file_config.batch_size == 100
        assert file_config.http_path is None
    
    def test_file_item_config_with_http_path(self):
        """Test FileItemConfig with HTTP path."""
        file_config = FileItemConfig(
            name="remote_file",
            format="csv",
            http_path="https://example.com/data.csv"
        )
        
        assert file_config.name == "remote_file"
        assert file_config.http_path == "https://example.com/data.csv"
        assert file_config.file_path is None
        assert file_config.strategy == LoadStrategy.REPLACE


@pytest.mark.unit
class TestDatabaseConfig:
    """Test DatabaseConfig dataclass."""
    
    def test_database_config_creation(self):
        """Test creating a DatabaseConfig instance."""
        db_config = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            charset="utf8mb4"
        )
        
        assert db_config.type == "mysql"
        assert db_config.host == "localhost"
        assert db_config.port == 3306
        assert db_config.user == "test_user"
        assert db_config.password == "test_password"
        assert db_config.database == "test_db"
        assert db_config.charset == "utf8mb4"
    
    def test_database_config_with_tables(self):
        """Test DatabaseConfig with tables."""
        tables = [
            TableConfig(name="users", strategy=LoadStrategy.REPLACE),
            TableConfig(name="orders", strategy=LoadStrategy.MERGE, merge_key="id")
        ]
        
        db_config = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="testdb",
            tables=tables
        )
        
        assert len(db_config.tables) == 2
        assert db_config.tables[0].name == "users"
        assert db_config.tables[1].name == "orders"


@pytest.mark.unit
class TestFileConfig:
    """Test FileConfig dataclass."""
    
    def test_file_config_creation(self):
        """Test creating a FileConfig instance."""
        files = [
            FileItemConfig(name="file1", format="csv", file_path="/path/to/file1.csv"),
            FileItemConfig(name="file2", format="json", http_path="https://example.com/file2.json")
        ]
        
        file_config = FileConfig(type="csv", files=files)
        
        assert file_config.type == "csv"
        assert len(file_config.files) == 2
        assert file_config.files[0].name == "file1"
        assert file_config.files[1].name == "file2"


@pytest.mark.unit
class TestPipelineConfig:
    """Test PipelineConfig dataclass."""
    
    def test_pipeline_config_creation(self, sample_database_config):
        """Test creating a PipelineConfig instance."""
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline_config = PipelineConfig(
            name="test_pipeline",
            source=sample_database_config,
            destination=destination,
            workers=4
        )
        
        assert pipeline_config.name == "test_pipeline"
        assert pipeline_config.source == sample_database_config
        assert pipeline_config.destination == destination
        assert pipeline_config.workers == 4
    
    def test_pipeline_config_defaults(self, sample_database_config):
        """Test PipelineConfig with default values."""
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost", 
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline_config = PipelineConfig(
            name="test_pipeline",
            source=sample_database_config,
            destination=destination
        )
        
        assert pipeline_config.workers is None


@pytest.mark.unit
class TestConfig:
    """Test Config dataclass."""
    
    def test_config_creation(self, sample_multi_pipeline_config):
        """Test creating a Config instance."""
        config = sample_multi_pipeline_config
        
        assert len(config.pipelines) == 2
        assert config.pipelines[0].name == "db_to_db_pipeline" 
        assert config.pipelines[1].name == "file_to_db_pipeline"
    
    def test_config_read_from_file(self, temp_config_file):
        """Test reading configuration from YAML file."""
        config = Config.read_config(temp_config_file)
        
        assert len(config.pipelines) == 1
        assert config.pipelines[0].name == "test_pipeline"
        assert config.pipelines[0].source.type == "mysql"
        assert config.pipelines[0].destination.type == "postgresql"
    
    def test_config_validation_no_pipelines(self):
        """Test that configuration validation fails with no pipelines."""
        with pytest.raises(ValueError, match="At least one pipeline must be configured"):
            Config.from_dict({"pipelines": []})
    
    def test_config_validation_duplicate_pipeline_names(self, sample_database_config):
        """Test that configuration validation fails with duplicate pipeline names."""
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432, 
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline1 = PipelineConfig(
            name="duplicate_name",
            source=sample_database_config,
            destination=destination
        )
        
        pipeline2 = PipelineConfig(
            name="duplicate_name", 
            source=sample_database_config,
            destination=destination
        )
        
        with pytest.raises(ValueError, match="Pipeline names must be unique"):
            Config(pipelines=[pipeline1, pipeline2])
    
    def test_config_validation_missing_source(self):
        """Test that configuration validation fails with missing source."""
        config_data = {
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "destination": {
                        "type": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "user": "postgres", 
                        "password": "password",
                        "database": "target_db"
                    }
                    # Missing source
                }
            ]
        }
        
        with pytest.raises((ValueError, KeyError)):
            Config.from_dict(config_data)
    
    def test_config_validation_missing_destination(self, sample_database_config):
        """Test that configuration validation fails with missing destination."""
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
                        "tables": [{"name": "test_table"}]
                    }
                    # Missing destination
                }
            ]
        }
        
        with pytest.raises((ValueError, KeyError)):
            Config.from_dict(config_data)
    
    def test_config_validation_database_no_tables(self):
        """Test that configuration validation fails for database with no tables."""
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
                        "tables": []  # Empty tables
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "user": "postgres",
                        "password": "password", 
                        "database": "target_db"
                    }
                }
            ]
        }
        
        with pytest.raises(ValueError, match="must have at least one table"):
            Config.from_dict(config_data)
    
    def test_config_validation_file_no_files(self):
        """Test that configuration validation fails for file config with no files."""
        config_data = {
            "pipelines": [
                {
                    "name": "test_pipeline",
                    "source": {
                        "type": "csv",
                        "files": []  # Empty files
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "user": "postgres",
                        "password": "password",
                        "database": "target_db"
                    }
                }
            ]
        }
        
        with pytest.raises(ValueError, match="must have at least one file"):
            Config.from_dict(config_data)