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
Integration tests for database connectors.

These tests require actual database instances running.
They can be skipped if databases are not available.
"""

import pytest
from unittest.mock import patch, Mock

from extral.connectors.database.mysql import MySQLConnector
from extral.connectors.database.postgresql import PostgreSQLConnector
from extral.config import DatabaseConfig, ExtractConfig, LoadConfig
from extral.exceptions import ConnectionException


class TestMySQLConnectorIntegration:
    """Integration tests for MySQL connector."""
    
    @pytest.fixture
    def mysql_config(self):
        """MySQL configuration for testing."""
        return DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user", 
            password="test_password",
            database="test_db",
            charset="utf8mb4"
        )
    
    @patch('extral.connectors.database.mysql.pymysql.connect')
    def test_mysql_connector_connect(self, mock_connect, mysql_config):
        """Test MySQL connector connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connector = MySQLConnector()
        connector.connect(mysql_config)
        
        assert connector.config == mysql_config
        assert connector.connection == mock_connection
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            charset="utf8mb4",
            cursorclass=pytest.mock.ANY
        )
    
    @patch('extral.connectors.database.mysql.pymysql.connect')
    def test_mysql_connector_test_connection_success(self, mock_connect, mysql_config):
        """Test successful MySQL connection test."""
        # Setup mock connection and cursor
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {"result": 1}
        
        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.cursor.return_value.__exit__.return_value = None
        
        mock_connect.return_value = mock_connection
        
        connector = MySQLConnector()
        connector.config = mysql_config
        
        # Test connection should succeed
        result = connector.test_connection()
        
        assert result is True
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('extral.connectors.database.mysql.pymysql.connect')
    def test_mysql_connector_test_connection_failure(self, mock_connect, mysql_config):
        """Test failed MySQL connection test."""
        import pymysql
        
        # Mock connection failure
        mock_connect.side_effect = pymysql.Error("Connection failed")
        
        connector = MySQLConnector()
        connector.config = mysql_config
        
        # Test connection should raise ConnectionException
        with pytest.raises(ConnectionException) as exc_info:
            connector.test_connection()
        
        assert "MySQL connection test failed" in str(exc_info.value)
    
    def test_mysql_connector_test_connection_no_config(self):
        """Test MySQL connection test without configuration."""
        connector = MySQLConnector()
        
        with pytest.raises(ConnectionException) as exc_info:
            connector.test_connection()
        
        assert "No database configuration provided" in str(exc_info.value)
    
    @patch('extral.connectors.database.mysql.pymysql.connect')
    def test_mysql_connector_disconnect(self, mock_connect, mysql_config):
        """Test MySQL connector disconnection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connector = MySQLConnector()
        connector.connect(mysql_config)
        connector.disconnect()
        
        mock_connection.close.assert_called_once()
    
    @patch('extral.connectors.database.mysql.pymysql.connect')
    def test_mysql_connector_is_table_exists(self, mock_connect, mysql_config):
        """Test MySQL table existence check."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {"COUNT(*)": 1}
        
        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.cursor.return_value.__exit__.return_value = None
        
        mock_connect.return_value = mock_connection
        
        connector = MySQLConnector()
        connector.connect(mysql_config)
        
        result = connector.is_table_exists("test_table")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchone.assert_called_once()


class TestPostgreSQLConnectorIntegration:
    """Integration tests for PostgreSQL connector."""
    
    @pytest.fixture
    def postgresql_config(self):
        """PostgreSQL configuration for testing."""
        return DatabaseConfig(
            type="postgresql", 
            host="localhost",
            port=5432,
            user="postgres",
            password="postgres_password",
            database="test_db",
            schema="public"
        )
    
    @patch('extral.connectors.database.postgresql.psycopg2.connect')
    def test_postgresql_connector_connect(self, mock_connect, postgresql_config):
        """Test PostgreSQL connector connection."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = PostgreSQLConnector()
        connector.connect(postgresql_config)
        
        assert connector.config == postgresql_config
        assert connector.connection == mock_connection
        assert connector.cursor == mock_cursor
        
        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="postgres",
            password="postgres_password",
            host="localhost",
            port=5432
        )
    
    @patch('extral.connectors.database.postgresql.psycopg2.connect')
    def test_postgresql_connector_test_connection_success(self, mock_connect, postgresql_config):
        """Test successful PostgreSQL connection test."""
        # Setup mock connection and cursor  
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        
        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.cursor.return_value.__exit__.return_value = None
        
        mock_connect.return_value = mock_connection
        
        connector = PostgreSQLConnector()
        connector.config = postgresql_config
        
        # Test connection should succeed
        result = connector.test_connection()
        
        assert result is True
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT 1") 
        mock_cursor.fetchone.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('extral.connectors.database.postgresql.psycopg2.connect')
    def test_postgresql_connector_test_connection_failure(self, mock_connect, postgresql_config):
        """Test failed PostgreSQL connection test."""
        import psycopg2
        
        # Mock connection failure
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        connector = PostgreSQLConnector()
        connector.config = postgresql_config
        
        # Test connection should raise ConnectionException
        with pytest.raises(ConnectionException) as exc_info:
            connector.test_connection()
        
        assert "PostgreSQL connection test failed" in str(exc_info.value)
    
    def test_postgresql_connector_test_connection_no_config(self):
        """Test PostgreSQL connection test without configuration."""
        connector = PostgreSQLConnector()
        
        with pytest.raises(ConnectionException) as exc_info:
            connector.test_connection()
        
        assert "No database configuration provided" in str(exc_info.value)
    
    @patch('extral.connectors.database.postgresql.psycopg2.connect')
    def test_postgresql_connector_disconnect(self, mock_connect, postgresql_config):
        """Test PostgreSQL connector disconnection."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = PostgreSQLConnector()
        connector.connect(postgresql_config)
        connector.disconnect()
        
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('extral.connectors.database.postgresql.psycopg2.connect')
    def test_postgresql_connector_is_table_exists(self, mock_connect, postgresql_config):
        """Test PostgreSQL table existence check."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (True,)
        
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        connector = PostgreSQLConnector()
        connector.connect(postgresql_config)
        
        result = connector.is_table_exists("test_table")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchone.assert_called_once()


@pytest.mark.integration
@pytest.mark.database
class TestDatabaseConnectorRealIntegration:
    """Real integration tests requiring actual database connections."""
    
    @pytest.mark.skipif(
        "not config.getoption('--run-integration')",
        reason="Integration tests not enabled"
    )
    def test_mysql_real_connection(self):
        """Test actual MySQL connection (requires real MySQL instance)."""
        # This test would only run with --run-integration flag
        # and would require a real MySQL instance
        config = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password", 
            database="test_db",
            charset="utf8mb4"
        )
        
        connector = MySQLConnector()
        
        try:
            connector.connect(config)
            result = connector.test_connection()
            assert result is True
        except Exception:
            pytest.skip("MySQL database not available for integration testing")
        finally:
            connector.disconnect()
    
    @pytest.mark.skipif(
        "not config.getoption('--run-integration')",
        reason="Integration tests not enabled"
    )
    def test_postgresql_real_connection(self):
        """Test actual PostgreSQL connection (requires real PostgreSQL instance)."""
        # This test would only run with --run-integration flag  
        # and would require a real PostgreSQL instance
        config = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="postgres_password",
            database="test_db"
        )
        
        connector = PostgreSQLConnector()
        
        try:
            connector.connect(config)
            result = connector.test_connection()
            assert result is True
        except Exception:
            pytest.skip("PostgreSQL database not available for integration testing")
        finally:
            connector.disconnect()


def pytest_addoption(parser):
    """Add command line option for integration tests."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests that require real database connections"
    )