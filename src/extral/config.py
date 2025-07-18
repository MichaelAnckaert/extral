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
import logging
from enum import Enum
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field

import yaml

logger = logging.getLogger(__name__)


class LoadStrategy(Enum):
    """Enumeration of supported load strategies."""
    APPEND = "append"
    REPLACE = "replace"
    MERGE = "merge"


class ReplaceMethod(Enum):
    """Enumeration of supported replace methods."""
    TRUNCATE = "truncate"
    RECREATE = "recreate"


@dataclass
class ExtractConfig:
    """Configuration for data extraction operations."""
    extract_type: Optional[str] = None
    incremental_field: Optional[str] = None
    last_value: Optional[Union[str, int]] = None
    batch_size: Optional[int] = None
    
    def to_dict(self) -> dict[str, Optional[Union[str, int]]]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "extract_type": self.extract_type,
            "incremental_field": self.incremental_field,
            "last_value": self.last_value,
            "batch_size": self.batch_size,
        }


@dataclass
class LoadConfig:
    """Configuration for data loading operations."""
    strategy: LoadStrategy = LoadStrategy.REPLACE
    replace_method: ReplaceMethod = ReplaceMethod.RECREATE
    merge_key: Optional[str] = None
    batch_size: Optional[int] = None


@dataclass
class LoggingConfig:
    """Configuration for logging."""
    level: str = "INFO"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        """Create LoggingConfig from dictionary."""
        return cls(level=data.get("level", "INFO"))


@dataclass
class ProcessingConfig:
    """Configuration for processing."""
    workers: int = 4
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProcessingConfig":
        """Create ProcessingConfig from dictionary."""
        return cls(workers=data.get("workers", 4))


@dataclass
class DatabaseConfig:
    """Configuration for database connections."""
    type: str
    host: str
    port: int
    user: str
    password: str
    database: str
    schema: Optional[str] = None
    charset: str = "utf8mb4"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConfig":
        """Create DatabaseConfig from dictionary."""
        return cls(
            type=data["type"],
            host=data["host"],
            port=data.get("port", 3306 if data["type"] == "mysql" else 5432),
            user=data["user"],
            password=data["password"],
            database=data["database"],
            schema=data.get("schema"),
            charset=data.get("charset", "utf8mb4")
        )


@dataclass
class IncrementalConfig:
    """Configuration for incremental extraction."""
    field: str
    type: str
    initial_value: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IncrementalConfig":
        """Create IncrementalConfig from dictionary."""
        return cls(
            field=data["field"],
            type=data["type"],
            initial_value=data.get("initial_value")
        )


@dataclass
class ReplaceConfig:
    """Configuration for replace strategy."""
    how: ReplaceMethod = ReplaceMethod.RECREATE
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReplaceConfig":
        """Create ReplaceConfig from dictionary."""
        how_str = data.get("how", "recreate")
        return cls(how=ReplaceMethod(how_str))


@dataclass
class TableConfig:
    """Configuration for table processing."""
    name: str
    strategy: LoadStrategy = LoadStrategy.REPLACE
    merge_key: Optional[str] = None
    batch_size: Optional[int] = None
    incremental: Optional[IncrementalConfig] = None
    replace: Optional[ReplaceConfig] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableConfig":
        """Create TableConfig from dictionary."""
        strategy_str = data.get("strategy", "replace")
        strategy = LoadStrategy(strategy_str)
        
        incremental = None
        if "incremental" in data and data["incremental"]:
            incremental = IncrementalConfig.from_dict(data["incremental"])
        
        replace = None
        if "replace" in data and data["replace"]:
            replace = ReplaceConfig.from_dict(data["replace"])
        
        return cls(
            name=data["name"],
            strategy=strategy,
            merge_key=data.get("merge_key"),
            batch_size=data.get("batch_size"),
            incremental=incremental,
            replace=replace
        )


@dataclass
class Config:
    """Main configuration object."""
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    source: DatabaseConfig = field(default_factory=lambda: DatabaseConfig("", "", 0, "", "", ""))
    destination: DatabaseConfig = field(default_factory=lambda: DatabaseConfig("", "", 0, "", "", ""))
    tables: list[TableConfig] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        """Create Config from dictionary."""
        # Get single configs (take first if multiple provided)
        logging_data = data.get("logging", [{}])
        if isinstance(logging_data, list):
            logging_data = logging_data[0] if logging_data else {}
        logging_config = LoggingConfig.from_dict(logging_data)
        
        processing_data = data.get("processing", [{}])
        if isinstance(processing_data, list):
            processing_data = processing_data[0] if processing_data else {}
        processing_config = ProcessingConfig.from_dict(processing_data)
        
        source_data = data.get("source", [])
        if isinstance(source_data, list):
            if not source_data:
                raise ValueError("Source configuration is required")
            source_data = source_data[0]
        source_config = DatabaseConfig.from_dict(source_data)
        
        destination_data = data.get("destination", [])
        if isinstance(destination_data, list):
            if not destination_data:
                raise ValueError("Destination configuration is required")
            destination_data = destination_data[0]
        destination_config = DatabaseConfig.from_dict(destination_data)
        
        # Tables remain as a list
        table_configs = [TableConfig.from_dict(cfg) for cfg in data.get("tables", [])]
        
        return cls(
            logging=logging_config,
            processing=processing_config,
            source=source_config,
            destination=destination_config,
            tables=table_configs
        )
    
    @classmethod
    def read_config(cls, path: str) -> "Config":
        """Read configuration from YAML file."""
        with open(path, "r") as file:
            data = yaml.safe_load(file)
            return cls.from_dict(data)


