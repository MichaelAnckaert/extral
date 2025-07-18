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
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from extral.database import DatabaseRecord


class CustomEncoder(json.JSONEncoder):
    def default(self, o: Any):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


class EncodingException(Exception):
    """Custom exception for encoding errors."""

    pass


def encode_data(data: list[DatabaseRecord]) -> bytes:
    """
    Encode data to JSON bytes using a custom encoder for datetime and Decimal types.
    """
    try:
        return json.dumps(data, cls=CustomEncoder).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise EncodingException(f"Failed to encode data: {e}") from e
