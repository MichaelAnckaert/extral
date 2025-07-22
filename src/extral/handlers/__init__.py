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
"""Event handlers for the unified logging/statistics system."""

from extral.handlers.console_handler import ConsoleHandler
from extral.handlers.stats_handler import StatsHandler
from extral.handlers.tui_handler import TUIHandler

__all__ = ["ConsoleHandler", "StatsHandler", "TUIHandler"]