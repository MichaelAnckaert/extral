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
    mode: str = "standard"  # "standard" or "tui"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        """Create LoggingConfig from dictionary."""
        return cls(
            level=data.get("level", "INFO"),
            mode=data.get("mode", "standard")
        )


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
    tables: list["TableConfig"] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConfig":
        """Create DatabaseConfig from dictionary."""
        # Parse tables if present
        tables = []
        if "tables" in data:
            tables = [
                TableConfig.from_dict(table_data) for table_data in data["tables"]
            ]

        return cls(
            type=data["type"],
            host=data["host"],
            port=data.get("port", 3306 if data["type"] == "mysql" else 5432),
            user=data["user"],
            password=data["password"],
            database=data["database"],
            schema=data.get("schema"),
            charset=data.get("charset", "utf8mb4"),
            tables=tables,
        )


@dataclass
class FileItemConfig:
    """Configuration for a single file item."""

    name: str  # Logical name for the file (like table name)
    file_path: Optional[str] = None
    http_path: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    strategy: LoadStrategy = LoadStrategy.REPLACE
    merge_key: Optional[str] = None
    batch_size: Optional[int] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.file_path and not self.http_path:
            raise ValueError("Either file_path or http_path must be provided")
        if self.file_path and self.http_path:
            raise ValueError("Cannot specify both file_path and http_path")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileItemConfig":
        """Create FileItemConfig from dictionary."""
        strategy_str = data.get("strategy", "replace")
        strategy = LoadStrategy(strategy_str)

        return cls(
            name=data["name"],
            file_path=data.get("file_path"),
            http_path=data.get("http_path"),
            options=data.get("options", {}),
            strategy=strategy,
            merge_key=data.get("merge_key"),
            batch_size=data.get("batch_size"),
        )


@dataclass
class FileConfig:
    """Configuration for file connections."""

    type: str  # "file"
    files: list[FileItemConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileConfig":
        """Create FileConfig from dictionary."""
        if "files" not in data:
            raise ValueError("'files' configuration is required for file sources")

        files = [FileItemConfig.from_dict(file_data) for file_data in data["files"]]

        return cls(type=data["type"], files=files)


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
            initial_value=data.get("initial_value"),
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
            replace=replace,
        )


ConnectorConfig = Union[DatabaseConfig, FileConfig]


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline."""

    name: str
    source: ConnectorConfig
    destination: ConnectorConfig
    workers: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        """Create PipelineConfig from dictionary."""
        name = data.get("name", "default")

        # Parse source config
        source_data = data.get("source", {})
        source_config: ConnectorConfig
        source_type = source_data.get("type", "")
        if source_type in ["file", "csv", "json"]:
            source_config = FileConfig.from_dict(source_data)
        else:
            source_config = DatabaseConfig.from_dict(source_data)

        # Parse destination config
        destination_data = data.get("destination", {})
        destination_config: ConnectorConfig
        destination_type = destination_data.get("type", "")
        if destination_type in ["file", "csv", "json"]:
            destination_config = FileConfig.from_dict(destination_data)
        else:
            destination_config = DatabaseConfig.from_dict(destination_data)

        return cls(
            name=name,
            source=source_config,
            destination=destination_config,
            workers=data.get("workers"),
        )


@dataclass
class Config:
    """Main configuration object."""

    logging: LoggingConfig = field(default_factory=LoggingConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    pipelines: list[PipelineConfig] = field(default_factory=list)

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_pipelines(self.pipelines)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        """Create Config from dictionary."""
        # Parse logging config
        logging_data = data.get("logging", {})
        logging_config = LoggingConfig.from_dict(logging_data)

        # Parse processing config
        processing_data = data.get("processing", {})
        processing_config = ProcessingConfig.from_dict(processing_data)

        # Parse pipelines
        if "pipelines" not in data:
            raise ValueError("'pipelines' configuration is required")

        pipelines = [
            PipelineConfig.from_dict(pipeline_data)
            for pipeline_data in data["pipelines"]
        ]

        # Validate pipelines
        cls._validate_pipelines(pipelines)

        return cls(
            logging=logging_config, processing=processing_config, pipelines=pipelines
        )

    @classmethod
    def _validate_pipelines(cls, pipelines: list[PipelineConfig]) -> None:
        """Validate pipeline configuration."""
        if not pipelines:
            raise ValueError("At least one pipeline must be configured")

        # Check for duplicate pipeline names
        names = [pipeline.name for pipeline in pipelines]
        if len(names) != len(set(names)):
            raise ValueError("Pipeline names must be unique")

        # Validate each pipeline has valid source and destination
        for pipeline in pipelines:
            if not pipeline.source:
                raise ValueError(
                    f"Pipeline '{pipeline.name}' must have a source configuration"
                )
            if not pipeline.destination:
                raise ValueError(
                    f"Pipeline '{pipeline.name}' must have a destination configuration"
                )

            # Validate source has tables/files
            if isinstance(pipeline.source, DatabaseConfig):
                if not pipeline.source.tables:
                    raise ValueError(
                        f"Database source in pipeline '{pipeline.name}' must have at least one table"
                    )
            elif isinstance(pipeline.source, FileConfig):
                if not pipeline.source.files:
                    raise ValueError(
                        f"File source in pipeline '{pipeline.name}' must have at least one file"
                    )

    @classmethod
    def read_config(cls, path: str) -> "Config":
        """Read configuration from YAML file."""
        with open(path, "r") as file:
            data = yaml.safe_load(file)
            return cls.from_dict(data)
