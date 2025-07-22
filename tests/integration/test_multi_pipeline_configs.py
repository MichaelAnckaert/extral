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
Integration tests for multi-pipeline configurations.

These tests verify that multi-pipeline configurations are properly parsed,
validated, and can be executed without conflicts.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, Mock

from extral.config import Config
from extral.main import run
from extral.validation import PipelineValidator


@pytest.mark.integration
class TestMultiPipelineConfigurations:
    """Test various multi-pipeline configuration scenarios."""

    def test_database_to_database_multi_pipeline_config(self):
        """Test configuration with multiple database-to-database pipelines."""
        config_data = {
            "pipelines": [
                {
                    "name": "mysql_to_postgres_users",
                    "source": {
                        "type": "mysql",
                        "host": "mysql-host",
                        "port": 3306,
                        "user": "mysql_user",
                        "password": "mysql_password",
                        "database": "source_db",
                        "charset": "utf8mb4",
                        "tables": [
                            {
                                "name": "users",
                                "strategy": "replace",
                                "batch_size": 1000
                            },
                            {
                                "name": "user_profiles", 
                                "strategy": "merge",
                                "merge_key": "user_id",
                                "batch_size": 500
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "postgres-host",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "target_db",
                        "schema": "public"
                    },
                    "workers": 2
                },
                {
                    "name": "mysql_to_postgres_orders",
                    "source": {
                        "type": "mysql",
                        "host": "mysql-host",
                        "port": 3306,
                        "user": "mysql_user",
                        "password": "mysql_password",
                        "database": "source_db",
                        "charset": "utf8mb4",
                        "tables": [
                            {
                                "name": "orders",
                                "strategy": "append",
                                "batch_size": 2000
                            },
                            {
                                "name": "order_items",
                                "strategy": "merge", 
                                "merge_key": "order_item_id",
                                "batch_size": 1500
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "postgres-host",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password", 
                        "database": "target_db",
                        "schema": "orders"
                    },
                    "workers": 3
                }
            ]
        }

        config = Config.from_dict(config_data)
        
        assert len(config.pipelines) == 2
        
        # Verify first pipeline
        pipeline1 = config.pipelines[0]
        assert pipeline1.name == "mysql_to_postgres_users"
        assert pipeline1.source.type == "mysql"
        assert pipeline1.destination.type == "postgresql"
        assert len(pipeline1.source.tables) == 2
        assert pipeline1.workers == 2
        
        # Verify second pipeline
        pipeline2 = config.pipelines[1]
        assert pipeline2.name == "mysql_to_postgres_orders"
        assert pipeline2.source.type == "mysql"
        assert pipeline2.destination.type == "postgresql"
        assert len(pipeline2.source.tables) == 2
        assert pipeline2.workers == 3

    def test_file_to_database_multi_pipeline_config(self):
        """Test configuration with file-to-database pipelines."""
        config_data = {
            "pipelines": [
                {
                    "name": "csv_to_mysql_pipeline",
                    "source": {
                        "type": "csv",
                        "files": [
                            {
                                "name": "customer_data",
                                "file_path": "/data/customers.csv",
                                "strategy": "replace",
                                "batch_size": 500
                            },
                            {
                                "name": "product_data",
                                "file_path": "/data/products.csv",
                                "strategy": "merge",
                                "merge_key": "product_id", 
                                "batch_size": 200
                            }
                        ]
                    },
                    "destination": {
                        "type": "mysql",
                        "host": "mysql-host",
                        "port": 3306,
                        "user": "mysql_user",
                        "password": "mysql_password",
                        "database": "analytics_db",
                        "charset": "utf8mb4"
                    },
                    "workers": 2
                },
                {
                    "name": "json_to_postgresql_pipeline",
                    "source": {
                        "type": "json",
                        "files": [
                            {
                                "name": "sales_data",
                                "http_path": "https://api.example.com/sales.json",
                                "strategy": "append",
                                "batch_size": 1000
                            },
                            {
                                "name": "inventory_data",
                                "file_path": "/data/inventory.json",
                                "strategy": "replace",
                                "batch_size": 300
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "postgres-host",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "warehouse_db",
                        "schema": "raw_data"
                    },
                    "workers": 1
                }
            ]
        }

        config = Config.from_dict(config_data)
        
        assert len(config.pipelines) == 2
        
        # Verify CSV pipeline
        csv_pipeline = config.pipelines[0]
        assert csv_pipeline.name == "csv_to_mysql_pipeline"
        assert csv_pipeline.source.type == "csv"
        assert csv_pipeline.destination.type == "mysql"
        assert len(csv_pipeline.source.files) == 2
        assert csv_pipeline.workers == 2
        
        # Verify JSON pipeline  
        json_pipeline = config.pipelines[1]
        assert json_pipeline.name == "json_to_postgresql_pipeline"
        assert json_pipeline.source.type == "json"
        assert json_pipeline.destination.type == "postgresql"
        assert len(json_pipeline.source.files) == 2
        assert json_pipeline.workers == 1

    def test_mixed_connector_types_config(self):
        """Test configuration with mixed connector types across pipelines."""
        config_data = {
            "pipelines": [
                {
                    "name": "database_extraction",
                    "source": {
                        "type": "postgresql",
                        "host": "postgres-source",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "source_db",
                        "schema": "public",
                        "tables": [
                            {
                                "name": "analytics_summary",
                                "strategy": "replace",
                                "batch_size": 100
                            }
                        ]
                    },
                    "destination": {
                        "type": "csv",
                        "files": [
                            {
                                "name": "analytics_export",
                                "file_path": "/exports/analytics.csv",
                                "strategy": "replace"
                            }
                        ]
                    }
                },
                {
                    "name": "file_ingestion", 
                    "source": {
                        "type": "csv",
                        "files": [
                            {
                                "name": "external_data",
                                "file_path": "/imports/external.csv",
                                "strategy": "append",
                                "batch_size": 1000
                            }
                        ]
                    },
                    "destination": {
                        "type": "mysql",
                        "host": "mysql-dest",
                        "port": 3306,
                        "user": "mysql_user",
                        "password": "mysql_password",
                        "database": "staging_db",
                        "charset": "utf8mb4"
                    }
                }
            ]
        }

        config = Config.from_dict(config_data)
        
        assert len(config.pipelines) == 2
        
        # Verify database-to-file pipeline
        db_to_file = config.pipelines[0]
        assert db_to_file.name == "database_extraction"
        assert db_to_file.source.type == "postgresql"
        assert db_to_file.destination.type == "csv"
        
        # Verify file-to-database pipeline
        file_to_db = config.pipelines[1]
        assert file_to_db.name == "file_ingestion"
        assert file_to_db.source.type == "csv"
        assert file_to_db.destination.type == "mysql"

    def test_configuration_from_yaml_file(self):
        """Test loading multi-pipeline configuration from YAML file."""
        config_data = {
            "pipelines": [
                {
                    "name": "test_pipeline_1",
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
                        "user": "postgres",
                        "password": "postgres",
                        "database": "target_db",
                        "schema": "public"
                    }
                },
                {
                    "name": "test_pipeline_2",
                    "source": {
                        "type": "csv",
                        "files": [
                            {
                                "name": "test_file",
                                "file_path": "/tmp/test.csv",
                                "strategy": "append"
                            }
                        ]
                    },
                    "destination": {
                        "type": "mysql",
                        "host": "localhost",
                        "port": 3306,
                        "user": "mysql_user",
                        "password": "mysql_password", 
                        "database": "file_db",
                        "charset": "utf8mb4"
                    }
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file_path = f.name

        try:
            config = Config.read_config(temp_file_path)
            
            assert len(config.pipelines) == 2
            assert config.pipelines[0].name == "test_pipeline_1"
            assert config.pipelines[1].name == "test_pipeline_2"
            
        finally:
            Path(temp_file_path).unlink()

    @patch('extral.validation.MySQLConnector')
    @patch('extral.validation.PostgreSQLConnector')
    def test_multi_pipeline_validation(self, mock_pg_connector, mock_mysql_connector):
        """Test validation of multi-pipeline configurations."""
        # Setup mocks for connectivity tests
        mock_mysql_instance = Mock()
        mock_mysql_instance.test_connection.return_value = True
        mock_mysql_connector.return_value = mock_mysql_instance

        mock_pg_instance = Mock()
        mock_pg_instance.test_connection.return_value = True  
        mock_pg_connector.return_value = mock_pg_instance

        config_data = {
            "pipelines": [
                {
                    "name": "pipeline_1",
                    "source": {
                        "type": "mysql",
                        "host": "mysql-host",
                        "port": 3306,
                        "user": "mysql_user",
                        "password": "mysql_password",
                        "database": "source_db",
                        "charset": "utf8mb4",
                        "tables": [
                            {
                                "name": "users",
                                "strategy": "replace",
                                "batch_size": 1000
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "postgres-host",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "target_db", 
                        "schema": "public"
                    }
                },
                {
                    "name": "pipeline_2",
                    "source": {
                        "type": "mysql",
                        "host": "mysql-host-2",
                        "port": 3306,
                        "user": "mysql_user_2",
                        "password": "mysql_password_2",
                        "database": "source_db_2",
                        "charset": "utf8mb4",
                        "tables": [
                            {
                                "name": "orders",
                                "strategy": "merge",
                                "merge_key": "id",
                                "batch_size": 500
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "postgres-host-2",
                        "port": 5432,
                        "user": "postgres_user_2",
                        "password": "postgres_password_2",
                        "database": "target_db_2",
                        "schema": "public"
                    }
                }
            ]
        }

        config = Config.from_dict(config_data)
        validator = PipelineValidator()
        report = validator.validate_configuration(config)

        assert report.overall_valid is True
        assert len(report.pipeline_results) == 2
        assert "pipeline_1" in report.pipeline_results
        assert "pipeline_2" in report.pipeline_results

    def test_pipeline_resource_conflict_detection(self):
        """Test detection of resource conflicts between pipelines."""
        config_data = {
            "pipelines": [
                {
                    "name": "conflicting_pipeline_1",
                    "source": {
                        "type": "mysql",
                        "host": "source-host",
                        "port": 3306,
                        "user": "user1",
                        "password": "password1",
                        "database": "source_db",
                        "tables": [
                            {
                                "name": "shared_table",
                                "strategy": "replace"
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "target-host",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "target_db",
                        "schema": "public"
                    }
                },
                {
                    "name": "conflicting_pipeline_2", 
                    "source": {
                        "type": "mysql",
                        "host": "source-host",
                        "port": 3306,
                        "user": "user2",
                        "password": "password2",
                        "database": "source_db",
                        "tables": [
                            {
                                "name": "shared_table",
                                "strategy": "merge",
                                "merge_key": "id"
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql", 
                        "host": "target-host",
                        "port": 5432,
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "target_db",
                        "schema": "public"
                    }
                }
            ]
        }

        config = Config.from_dict(config_data)
        validator = PipelineValidator()
        report = validator.validate_configuration(config)

        # Should detect resource conflict
        assert report.overall_valid is False
        assert not report.global_conflicts.is_valid
        assert "Resource conflicts detected" in str(report.global_conflicts.error_message)

    @patch('extral.validation.MySQLConnector')
    @patch('extral.validation.PostgreSQLConnector') 
    @patch('extral.main.extract_table')
    @patch('extral.main.load_data')
    def test_multi_pipeline_dry_run(
        self, mock_load_data, mock_extract_table, mock_pg_connector, mock_mysql_connector
    ):
        """Test dry run mode with multi-pipeline configuration."""
        # Setup connector instance mocks
        mock_mysql_instance = Mock()
        mock_mysql_instance.test_connection.return_value = True
        mock_mysql_connector.return_value = mock_mysql_instance
        
        mock_pg_instance = Mock()
        mock_pg_instance.test_connection.return_value = True
        mock_pg_connector.return_value = mock_pg_instance
        
        config_data = {
            "pipelines": [
                {
                    "name": "test_pipeline_1",
                    "source": {
                        "type": "mysql",
                        "host": "localhost",
                        "port": 3306,
                        "user": "test_user",
                        "password": "test_password",
                        "database": "test_db",
                        "tables": [
                            {
                                "name": "test_table",
                                "strategy": "replace"
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "localhost",
                        "port": 5432,
                        "user": "postgres",
                        "password": "postgres",
                        "database": "target_db"
                    }
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_file_path = f.name

        try:
            # Should exit with code 0 for successful dry run
            with pytest.raises(SystemExit) as exc_info:
                run(temp_file_path, dry_run=True)
            
            assert exc_info.value.code == 0
            
            # Verify that extract and load were not called in dry run
            mock_extract_table.assert_not_called()
            mock_load_data.assert_not_called()
            
        finally:
            Path(temp_file_path).unlink()

    def test_complex_multi_pipeline_configuration(self):
        """Test a complex configuration with multiple pipeline types and settings."""
        config_data = {
            "logging": {
                "level": "DEBUG"
            },
            "processing": {
                "workers": 8
            },
            "pipelines": [
                {
                    "name": "high_volume_replication",
                    "source": {
                        "type": "mysql",
                        "host": "prod-mysql-01",
                        "port": 3306,
                        "user": "replication_user",
                        "password": "replication_password",
                        "database": "production_db",
                        "charset": "utf8mb4",
                        "tables": [
                            {
                                "name": "users",
                                "strategy": "merge",
                                "merge_key": "id",
                                "batch_size": 5000,
                                "incremental": {
                                    "field": "updated_at",
                                    "type": "datetime",
                                    "initial_value": "2024-01-01 00:00:00"
                                }
                            },
                            {
                                "name": "transactions",
                                "strategy": "append",
                                "batch_size": 10000,
                                "incremental": {
                                    "field": "created_at",
                                    "type": "datetime",
                                    "initial_value": "2024-01-01 00:00:00"
                                }
                            },
                            {
                                "name": "products",
                                "strategy": "replace", 
                                "batch_size": 1000
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "analytics-postgres-01",
                        "port": 5432,
                        "user": "analytics_user",
                        "password": "analytics_password",
                        "database": "analytics_db",
                        "schema": "replicated_data"
                    },
                    "workers": 6
                },
                {
                    "name": "external_data_ingestion",
                    "source": {
                        "type": "csv",
                        "files": [
                            {
                                "name": "customer_demographics",
                                "format": "csv",
                                "file_path": "/data/daily/demographics.csv",
                                "strategy": "replace",
                                "batch_size": 2000
                            },
                            {
                                "name": "market_data",
                                "format": "csv",
                                "http_path": "https://api.market.com/daily/data.csv",
                                "strategy": "append",
                                "batch_size": 500
                            }
                        ]
                    },
                    "destination": {
                        "type": "postgresql",
                        "host": "analytics-postgres-01",
                        "port": 5432,
                        "user": "analytics_user", 
                        "password": "analytics_password",
                        "database": "analytics_db",
                        "schema": "external_data"
                    },
                    "workers": 2
                },
                {
                    "name": "report_generation",
                    "source": {
                        "type": "postgresql",
                        "host": "analytics-postgres-01",
                        "port": 5432,
                        "user": "reporting_user",
                        "password": "reporting_password",
                        "database": "analytics_db",
                        "schema": "public",
                        "tables": [
                            {
                                "name": "daily_summary",
                                "strategy": "replace",
                                "batch_size": 100
                            }
                        ]
                    },
                    "destination": {
                        "type": "json",
                        "files": [
                            {
                                "name": "daily_report",
                                "format": "json",
                                "file_path": "/reports/daily_summary.json",
                                "strategy": "replace"
                            }
                        ]
                    },
                    "workers": 1
                }
            ]
        }

        config = Config.from_dict(config_data)
        
        assert len(config.pipelines) == 3
        
        # Verify high volume replication pipeline
        hvr_pipeline = config.pipelines[0]
        assert hvr_pipeline.name == "high_volume_replication"
        assert len(hvr_pipeline.source.tables) == 3
        assert hvr_pipeline.workers == 6
        
        # Verify external data ingestion pipeline  
        edi_pipeline = config.pipelines[1]
        assert edi_pipeline.name == "external_data_ingestion"
        assert len(edi_pipeline.source.files) == 2
        assert edi_pipeline.workers == 2
        
        # Verify report generation pipeline
        rg_pipeline = config.pipelines[2]
        assert rg_pipeline.name == "report_generation"
        assert len(rg_pipeline.source.tables) == 1
        assert len(rg_pipeline.destination.files) == 1
        assert rg_pipeline.workers == 1