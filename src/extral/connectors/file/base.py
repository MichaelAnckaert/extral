"""Base file connector implementation."""

import logging
from abc import abstractmethod
from typing import Any, Dict, Generator, Optional

from extral.config import ExtractConfig, FileItemConfig, LoadConfig
from extral.connectors.connector import Connector
from extral.database import DatabaseRecord
from extral.exceptions import ConnectionException

logger = logging.getLogger(__name__)


class FileConnector(Connector):
    """
    Abstract base class for file-based connectors.

    This class extends the generic Connector interface to provide
    file-specific functionality for ETL operations.
    """

    def __init__(self, config: FileItemConfig):
        """
        Initialize the file connector with configuration.

        Args:
            config: FileItemConfig instance with file-specific settings
        """
        self.config = config

    @abstractmethod
    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """
        Extract data from the file source.

        Args:
            dataset_name: Name of the dataset (file path or identifier)
            extract_config: Configuration for extraction

        Yields:
            Batches of database records
        """
        pass

    @abstractmethod
    def load_data(
        self,
        dataset_name: str,
        data: list[DatabaseRecord],
        load_config: LoadConfig,
    ) -> None:
        """
        Load data to the file destination.

        Args:
            dataset_name: Name of the target dataset (file path)
            data: List of records to load
            load_config: Configuration specifying the load strategy
        """
        pass

    def get_effective_path(self, dataset_name: Optional[str] = None) -> str:
        """
        Get the effective file path, handling both local and HTTP paths.

        Args:
            dataset_name: Optional dataset name to use as file path

        Returns:
            The resolved file path or URL
        """
        if self.config.file_path:
            return self.config.file_path
        elif self.config.http_path:
            return self.config.http_path
        else:
            raise ValueError("No path specified")

    def is_http_path(self, path: str) -> bool:
        """Check if the given path is an HTTP/HTTPS URL."""
        return path.startswith("http://") or path.startswith("https://")

    def test_connection(self) -> bool:
        """
        Test accessibility to the file source/destination.
        
        This method verifies that the file path or HTTP URL is accessible
        for reading (for extraction) or writing (for loading).
        
        Returns:
            bool: True if file/path is accessible, False otherwise
            
        Raises:
            ConnectionException: If file access fails with details
        """
        import os
        import urllib.request
        from pathlib import Path
        
        try:
            # Test file path accessibility
            if self.config.file_path:
                path = Path(self.config.file_path)
                
                if path.exists():
                    # File exists - test readability
                    if not path.is_file():
                        raise ConnectionException(
                            f"Path exists but is not a file: {self.config.file_path}",
                            operation="test_connection",
                            details={"file_path": str(path)}
                        )
                    
                    # Test read access
                    if not os.access(path, os.R_OK):
                        raise ConnectionException(
                            f"File exists but is not readable: {self.config.file_path}",
                            operation="test_connection",
                            details={"file_path": str(path)}
                        )
                else:
                    # File doesn't exist - test if parent directory is writable (for output)
                    parent_dir = path.parent
                    if not parent_dir.exists():
                        try:
                            parent_dir.mkdir(parents=True, exist_ok=True)
                        except Exception as e:
                            raise ConnectionException(
                                f"Cannot create parent directory: {parent_dir}",
                                operation="test_connection",
                                details={"parent_dir": str(parent_dir), "error": str(e)}
                            ) from e
                    
                    # Test write access to parent directory
                    if not os.access(parent_dir, os.W_OK):
                        raise ConnectionException(
                            f"Parent directory is not writable: {parent_dir}",
                            operation="test_connection",
                            details={"parent_dir": str(parent_dir)}
                        )
                
                logger.info(f"File access test successful: {self.config.file_path}")
            
            # Test HTTP path accessibility
            elif self.config.http_path:
                try:
                    # Try to open the URL with a timeout
                    with urllib.request.urlopen(self.config.http_path, timeout=10) as response:
                        # Check if we can read at least the first byte
                        response.read(1)
                except Exception as e:
                    raise ConnectionException(
                        f"HTTP path is not accessible: {self.config.http_path}",
                        operation="test_connection",
                        details={"http_path": self.config.http_path, "error": str(e)}
                    ) from e
                
                logger.info(f"HTTP access test successful: {self.config.http_path}")
            
            else:
                raise ConnectionException(
                    "No file path or HTTP path configured for testing",
                    operation="test_connection",
                    details={"config": str(self.config)}
                )
            
            return True
            
        except ConnectionException:
            # Re-raise connection exceptions as-is
            raise
        except Exception as e:
            error_msg = f"Unexpected error during file access test: {str(e)}"
            logger.error(error_msg)
            raise ConnectionException(
                error_msg,
                operation="test_connection",
                details={"config": str(self.config)}
            ) from e

    @abstractmethod
    def infer_schema(self, dataset_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Infer schema from the file.

        Args:
            dataset_name: Name of the dataset (file path)

        Returns:
            Schema dictionary with column names and types
        """
        pass
