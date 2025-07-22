"""Base file connector implementation."""

import logging
from abc import abstractmethod
from typing import Any, Dict, Generator, Optional

from extral.config import ExtractConfig, FileItemConfig, LoadConfig
from extral.connectors.connector import Connector
from extral.database import DatabaseRecord

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
        if dataset_name:
            return dataset_name
        elif self.config.file_path:
            return self.config.file_path
        elif self.config.http_path:
            return self.config.http_path
        else:
            raise ValueError("No path specified")

    def is_http_path(self, path: str) -> bool:
        """Check if the given path is an HTTP/HTTPS URL."""
        return path.startswith("http://") or path.startswith("https://")

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
