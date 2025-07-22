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
Generic Connector Interface Module
Defines the abstract interface for all data connectors in the ETL system.
"""

from abc import ABC, abstractmethod
from typing import Generator

from extral.config import ExtractConfig, LoadConfig
from extral.database import DatabaseRecord


class Connector(ABC):
    """
    Abstract base class for all data connectors.

    This interface defines the core ETL operations that all connectors must implement:
    - extract_data: Extract data from any source (database, files, APIs, etc.)
    - load_data: Load data into any destination with configurable strategies

    The interface is designed to be generic and not assume any specific storage type,
    allowing for databases, files, APIs, or any other data sources/destinations.
    """

    @abstractmethod
    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """
        Extract data from the source dataset.

        Args:
            dataset_name: Name/identifier of the dataset to extract from
                         (could be table name, file path, API endpoint, etc.)
            extract_config: Configuration for extraction (incremental, batch size, etc.)

        Yields:
            Batches of database records as dictionaries
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
        Load data into the destination dataset using the specified strategy.

        Args:
            dataset_name: Name/identifier of the target dataset
                         (could be table name, file path, API endpoint, etc.)
            data: List of records to load
            load_config: Configuration specifying the load strategy

        Strategies:
            - APPEND: Add new records without modifying existing data
            - REPLACE: Replace all data (with truncate or recreate method)
            - MERGE: Update existing records and insert new ones based on merge key
        """
        pass
