# Copyright 2025 Sinax GCV
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
from typing import TypedDict

from encoder import CustomEncoder

logger = logging.getLogger(__name__)

TableState = TypedDict(
    "TableState",
    {
        "incremental": dict[str, str | int | None],
    },
)


# Singleton state class to manage the state of the application
class State:
    def __init__(self) -> None:
        self.tables: dict[str, TableState] = dict()

    def store_state(self):
        """Store the current state to a JSON file."""

        # Create backup of the current state
        try:
            with open(
                f".state-backup-{datetime.now().isoformat()}.json", "w"
            ) as backup_file:
                json.dump(
                    {"tables": self.tables}, backup_file, indent=4, cls=CustomEncoder
                )
            logger.debug("State backup created successfully.")
        except Exception as e:
            logger.error(f"Failed to create state backup: {e}")

        # Store the current state
        with open("state.json", "w") as state_file:
            json.dump({"tables": self.tables}, state_file, indent=4, cls=CustomEncoder)
            logger.debug("State stored successfully.")

    def load_state(self):
        try:
            with open("state.json", "r") as state_file:
                self.tables = json.load(state_file)["tables"]
                logger.debug("State loaded successfully.")
        except FileNotFoundError:
            logger.debug("State file not found. Starting with an empty state.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding state file: {e}")
            self.state = {}


state = State()
