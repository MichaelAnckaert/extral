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
from datetime import datetime
import json
import logging
from typing import TypedDict, Optional

from extral.encoder import CustomEncoder

logger = logging.getLogger(__name__)

DatasetState = TypedDict(
    "DatasetState",
    {
        "incremental": dict[str, str | int | None],
    },
)

PipelineState = TypedDict(
    "PipelineState",
    {
        "datasets": dict[str, DatasetState],
    },
)


# Singleton state class to manage the state of the application
class State:
    def __init__(self) -> None:
        self.pipelines: dict[str, PipelineState] = dict()

    def get_dataset_state(self, pipeline_name: str, dataset_id: str) -> Optional[DatasetState]:
        """Get the state for a specific dataset in a specific pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            dataset_id: Identifier for the dataset (table name or file name/logical name)
        """
        if pipeline_name not in self.pipelines:
            return None
        
        pipeline_state = self.pipelines[pipeline_name]
        return pipeline_state.get("datasets", {}).get(dataset_id)

    def set_dataset_state(self, pipeline_name: str, dataset_id: str, dataset_state: DatasetState) -> None:
        """Set the state for a specific dataset in a specific pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            dataset_id: Identifier for the dataset (table name or file name/logical name)
            dataset_state: State data for the dataset
        """
        if pipeline_name not in self.pipelines:
            self.pipelines[pipeline_name] = {"datasets": {}}
        
        if "datasets" not in self.pipelines[pipeline_name]:
            self.pipelines[pipeline_name]["datasets"] = {}
        
        self.pipelines[pipeline_name]["datasets"][dataset_id] = dataset_state

    def get_pipeline_state(self, pipeline_name: str) -> Optional[PipelineState]:
        """Get the complete state for a specific pipeline."""
        return self.pipelines.get(pipeline_name)

    def list_pipelines(self) -> list[str]:
        """List all pipeline names that have state."""
        return list(self.pipelines.keys())

    def list_datasets(self, pipeline_name: str) -> list[str]:
        """List all dataset IDs for a specific pipeline."""
        if pipeline_name not in self.pipelines:
            return []
        
        pipeline_state = self.pipelines[pipeline_name]
        return list(pipeline_state.get("datasets", {}).keys())

    def store_state(self):
        """Store the current state to a JSON file."""

        # Create backup of the current state
        try:
            with open(
                f".state-backup-{datetime.now().isoformat()}.json", "w"
            ) as backup_file:
                json.dump(
                    {"pipelines": self.pipelines}, backup_file, indent=4, cls=CustomEncoder
                )
            logger.debug("State backup created successfully.")
        except Exception as e:
            logger.error(f"Failed to create state backup: {e}")

        # Store the current state
        with open("state.json", "w") as state_file:
            json.dump({"pipelines": self.pipelines}, state_file, indent=4, cls=CustomEncoder)
            logger.debug("State stored successfully.")

    def load_state(self):
        try:
            with open("state.json", "r") as state_file:
                data = json.load(state_file)
                self.pipelines = data.get("pipelines", {})
                logger.debug("State loaded successfully.")
        except FileNotFoundError:
            logger.debug("State file not found. Starting with an empty state.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding state file: {e}")
            self.pipelines = {}


state = State()
